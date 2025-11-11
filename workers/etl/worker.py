import os
import asyncio
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.common import RetryPolicy
from temporalio.worker import Worker, UnsandboxedWorkflowRunner
import json
import etl.etl_pipeline as etl
from temporalio.exceptions import ApplicationError
import contextlib
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _Retry
from concurrent.futures import ThreadPoolExecutor, as_completed


def _write_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f)


def _read_json(path: str):
    with open(path, "r") as f:
        return json.load(f)


async def _heartbeat_loop(label: str, stop_event: asyncio.Event, interval_seconds: int = 10) -> None:
    try:
        while not stop_event.is_set():
            try:
                activity.heartbeat({"stage": label})
            except Exception:
                # Heartbeat may fail if activity context not available; ignore
                pass
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue
    except asyncio.CancelledError:
        pass


@activity.defn
def extract_activity(run_id: str) -> str:
    """Stream extract to NDJSON with bounded concurrency, heartbeats and timings."""
    api_base = os.getenv("API_URL", "http://localhost:8000")
    ndjson_path = f"/tmp/etl-{run_id}-raw.ndjson"
    os.makedirs(os.path.dirname(ndjson_path), exist_ok=True)

    # HTTP session with retries
    session = requests.Session()
    retries = _Retry(
        total=int(os.getenv('ETL_HTTP_RETRY_TOTAL', '5')),
        backoff_factor=float(os.getenv('ETL_HTTP_RETRY_BACKOFF', '0.5')),
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=int(os.getenv('ETL_MAX_HTTP_CONCURRENCY', '8')),
        pool_maxsize=int(os.getenv('ETL_MAX_HTTP_CONCURRENCY', '8')),
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    request_timeout = float(os.getenv('ETL_REQUEST_TIMEOUT', '30'))
    heartbeat_every_pages = int(os.getenv('ETL_HEARTBEAT_EVERY_PAGES', '5'))
    max_chat_message_chats = int(os.getenv('ETL_MAX_CHAT_MESSAGE_CHATS', '500'))

    fetch_time = 0.0
    write_time = 0.0
    total_rows = 0
    total_pages = 0
    pages_since_hb = 0

    def _write_line(fh, resource: str, obj: dict):
        nonlocal write_time, total_rows
        t0 = time.time()
        fh.write(json.dumps({"resource": resource, "data": obj}, ensure_ascii=False) + "\n")
        write_time += time.time() - t0
        total_rows += 1

    # Health check
    t0 = time.time()
    r = session.get(f"{api_base}/health", timeout=5)
    fetch_time += time.time() - t0
    if r.status_code != 200:
        raise ApplicationError("Extract validation failed: API unhealthy", non_retryable=True)

    endpoints = [
        ("users", "/users"),
        ("chats", "/chats"),
        ("messages", "/messages"),
        ("marketplace_items", "/marketplace"),
        ("categories", "/categories"),
        ("sellers", "/sellers"),
    ]

    chat_ids = []

    with open(ndjson_path, "w") as fh:
        for resource, endpoint in endpoints:
            # First page
            params = {"page": 1, "page_size": int(os.getenv('MAX_PAGE_SIZE', '250')), "include_total": True}
            t0 = time.time()
            resp = session.get(f"{api_base}{endpoint}", params=params, timeout=request_timeout)
            fetch_time += time.time() - t0
            try:
                resp.raise_for_status()
            except Exception as e:
                raise ApplicationError(f"Extract failed: {resource} page 1 error: {e}")
            payload = resp.json()
            items = payload.get("items", [])
            total_pages_res = payload.get("total_pages", 0) or 0
            for it in items:
                _write_line(fh, resource, it)
                if resource == "chats":
                    cid = it.get("id")
                    if cid is not None:
                        chat_ids.append(cid)
            total_pages += 1
            pages_since_hb += 1
            if pages_since_hb >= heartbeat_every_pages:
                pages_since_hb = 0
                try:
                    activity.heartbeat({"resource": resource, "page": 1, "total_rows": total_rows})
                except Exception:
                    pass

            # Remaining pages concurrently
            if total_pages_res and total_pages_res > 1:
                def fetch_page(p: int):
                    prms = dict(params)
                    prms["page"] = p
                    t0i = time.time()
                    r = session.get(f"{api_base}{endpoint}", params=prms, timeout=request_timeout)
                    dt = time.time() - t0i
                    return p, r, dt

                max_workers = int(os.getenv('ETL_MAX_HTTP_CONCURRENCY', '8'))
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = [pool.submit(fetch_page, p) for p in range(2, total_pages_res + 1)]
                    for fut in as_completed(futures):
                        p, r, dt = fut.result()
                        fetch_time += dt
                        try:
                            r.raise_for_status()
                        except Exception as e:
                            raise ApplicationError(f"Extract failed: {resource} page {p} error: {e}")
                        page_items = r.json().get("items", [])
                        for it in page_items:
                            _write_line(fh, resource, it)
                            if resource == "chats":
                                cid = it.get("id")
                                if cid is not None:
                                    chat_ids.append(cid)
                        total_pages += 1
                        pages_since_hb += 1
                        if pages_since_hb >= heartbeat_every_pages:
                            pages_since_hb = 0
                            try:
                                activity.heartbeat({"resource": resource, "page": p, "total_rows": total_rows})
                            except Exception:
                                pass

        # Chat messages per chat (bounded), concurrent pages per chat
        chat_ids = chat_ids[:max_chat_message_chats]
        for i, cid in enumerate(chat_ids, 1):
            endpoint = f"/chats/{cid}/messages"
            params = {"page": 1, "page_size": int(os.getenv('MAX_PAGE_SIZE', '250')), "include_total": True}
            t0 = time.time()
            resp = session.get(f"{api_base}{endpoint}", params=params, timeout=request_timeout)
            fetch_time += time.time() - t0
            try:
                resp.raise_for_status()
            except Exception:
                continue
            payload = resp.json()
            items = payload.get("items", [])
            total_pages_res = payload.get("total_pages", 0) or 0
            for it in items:
                _write_line(fh, "chat_messages", it)
            total_pages += 1
            pages_since_hb += 1
            if pages_since_hb >= heartbeat_every_pages:
                pages_since_hb = 0
                try:
                    activity.heartbeat({"resource": "chat_messages", "chat_id": cid, "page": 1, "total_rows": total_rows})
                except Exception:
                    pass

            if total_pages_res and total_pages_res > 1:
                def fetch_page_chat(p: int):
                    prms = dict(params)
                    prms["page"] = p
                    t0i = time.time()
                    r = session.get(f"{api_base}{endpoint}", params=prms, timeout=request_timeout)
                    dt = time.time() - t0i
                    return p, r, dt

                max_workers = int(os.getenv('ETL_MAX_HTTP_CONCURRENCY', '8'))
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = [pool.submit(fetch_page_chat, p) for p in range(2, total_pages_res + 1)]
                    for fut in as_completed(futures):
                        p, r, dt = fut.result()
                        fetch_time += dt
                        try:
                            r.raise_for_status()
                        except Exception:
                            continue
                        page_items = r.json().get("items", [])
                        for it in page_items:
                            _write_line(fh, "chat_messages", it)
                        total_pages += 1
                        pages_since_hb += 1
                        if pages_since_hb >= heartbeat_every_pages:
                            pages_since_hb = 0
                            try:
                                activity.heartbeat({"resource": "chat_messages", "chat_id": cid, "page": p, "total_rows": total_rows})
                            except Exception:
                                pass

    if total_rows == 0:
        raise ApplicationError("Extract validation failed: no data", non_retryable=True)

    print(f"[extract] rows={total_rows} pages={total_pages} fetch_s={fetch_time:.2f} write_s={write_time:.2f}")
    return ndjson_path


@activity.defn
def transform_activity(raw_path: str) -> str:
    """Stream-transform NDJSON into aggregated JSON. Heartbeat every N rows."""
    if not raw_path.endswith(".ndjson"):
        raise ApplicationError("Transform expects NDJSON input", non_retryable=True)

    heartbeat_every_rows = int(os.getenv('ETL_HEARTBEAT_EVERY_ROWS', '1000'))

    # Aggregators
    users = {}
    chats = {}
    user_stats = {}
    user_to_chat_ids = {}
    chat_stats = {}
    daily_stats = {}
    hourly_stats = {}
    message_type_stats = {}
    weekday_stats = {}
    category_name_by_id = {}
    seller_agg = {}
    chat_mkt_agg = {}
    marketplace_stats = {
        'total_items': 0,
        'active_items': 0,
        'sold_items': 0,
        'cancelled_items': 0,
        'total_revenue': 0.0,
        'sum_price_all': 0.0,
        'count_price_all': 0,
    }
    daily_marketplace = {}
    sellers_count_per_category = {}

    def _inc(d, k, by=1):
        d[k] = d.get(k, 0) + by

    processed = 0
    t_proc = 0.0
    t0_total = time.time()

    with open(raw_path, 'r') as fh:
        for line in fh:
            t1 = time.time()
            try:
                rec = json.loads(line)
            except Exception:
                continue
            resource = rec.get('resource')
            data = rec.get('data') or {}
            # Users
            if resource == 'users':
                uid = data.get('id')
                if uid is None:
                    continue
                users[uid] = data
                if uid not in user_stats:
                    user_stats[uid] = {
                        'user_id': uid,
                        'username': data.get('username'),
                        'total_messages_sent': 0,
                        'chats_participated': 0,
                        'last_message_date': None,
                        'is_active': data.get('is_active'),
                        'created_at': data.get('created_at'),
                    }
                user_to_chat_ids.setdefault(uid, set())
            # Chats
            elif resource == 'chats':
                cid = data.get('id')
                if cid is None:
                    continue
                chats[cid] = data
                chat_stats[cid] = {
                    'chat_id': cid,
                    'chat_name': data.get('name') or 'Chat Privado',
                    'chat_type': data.get('chat_type'),
                    'total_messages': 0,
                    'unique_senders': set(),
                    'first_message_date': None,
                    'last_message_date': None,
                    'created_at': data.get('created_at'),
                }
            # Global messages
            elif resource == 'messages':
                sender_id = data.get('sender_id')
                chat_id = data.get('chat_id')
                sent_at = data.get('sent_at')
                if sender_id in user_stats:
                    user_stats[sender_id]['total_messages_sent'] += 1
                    user_to_chat_ids[sender_id].add(chat_id)
                    lam = user_stats[sender_id]['last_message_date']
                    if lam is None or (sent_at and sent_at > lam):
                        user_stats[sender_id]['last_message_date'] = sent_at
                if chat_id in chat_stats:
                    cs = chat_stats[chat_id]
                    cs['total_messages'] += 1
                    cs['unique_senders'].add(sender_id)
                    fm = cs['first_message_date']
                    lm = cs['last_message_date']
                    if fm is None or (sent_at and sent_at < fm):
                        cs['first_message_date'] = sent_at
                    if lm is None or (sent_at and sent_at > lm):
                        cs['last_message_date'] = sent_at
                # Hour of day
                if sent_at and len(sent_at) >= 13:
                    try:
                        hour = int(sent_at[11:13])
                        if 0 <= hour <= 23:
                            _inc(hourly_stats, hour)
                    except Exception:
                        pass
                # Daily stats
                if sent_at and len(sent_at) >= 10:
                    dstr = sent_at[:10]
                    entry = daily_stats.setdefault(dstr, {
                        'total_messages': 0,
                        'unique_users': set(),
                        'unique_chats': set(),
                        'private_messages': 0,
                        'group_messages': 0,
                    })
                    entry['total_messages'] += 1
                    if sender_id is not None:
                        entry['unique_users'].add(sender_id)
                    if chat_id is not None:
                        entry['unique_chats'].add(chat_id)
                        ctype = (chats.get(chat_id) or {}).get('chat_type', 'private')
                        if ctype == 'private':
                            entry['private_messages'] += 1
                        else:
                            entry['group_messages'] += 1
                # Message type summary
                mtype = data.get('message_type', 'text')
                _inc(message_type_stats, mtype)
            # Marketplace items
            elif resource == 'marketplace_items':
                marketplace_stats['total_items'] += 1
                st = data.get('status')
                if st == 'active':
                    marketplace_stats['active_items'] += 1
                elif st == 'sold':
                    marketplace_stats['sold_items'] += 1
                    try:
                        marketplace_stats['total_revenue'] += float(data.get('price') or 0)
                    except Exception:
                        pass
                elif st == 'cancelled':
                    marketplace_stats['cancelled_items'] += 1
                try:
                    p = float(data.get('price')) if data.get('price') is not None else None
                    if p is not None:
                        marketplace_stats['sum_price_all'] += p
                        marketplace_stats['count_price_all'] += 1
                except Exception:
                    pass
                # Seller agg
                sid = data.get('seller_id')
                if sid is not None:
                    agg = seller_agg.setdefault(sid, {
                        'listed': 0, 'active': 0, 'sold': 0,
                        'sum_list_price': 0.0, 'count_list_price': 0,
                        'sum_sold_value': 0.0,
                    })
                    agg['listed'] += 1
                    if st == 'active':
                        agg['active'] += 1
                    if st == 'sold':
                        agg['sold'] += 1
                        try:
                            agg['sum_sold_value'] += float(data.get('price') or 0)
                        except Exception:
                            pass
                    try:
                        p = float(data.get('price')) if data.get('price') is not None else None
                        if p is not None:
                            agg['sum_list_price'] += p
                            agg['count_list_price'] += 1
                    except Exception:
                        pass
                # Chat marketplace agg
                cid = data.get('chat_id')
                if cid is not None:
                    cm = chat_mkt_agg.setdefault(cid, {'total': 0, 'active': 0, 'sold': 0})
                    cm['total'] += 1
                    if st == 'active':
                        cm['active'] += 1
                    if st == 'sold':
                        cm['sold'] += 1
                # Daily marketplace
                dcreated = (data.get('created_at') or '')[:10]
                if dcreated:
                    dm = daily_marketplace.setdefault(dcreated, {'items_listed': 0, 'items_sold': 0, 'sum_price_listed': 0.0, 'count_price_listed': 0})
                    dm['items_listed'] += 1
                    try:
                        p = float(data.get('price') or 0)
                        dm['sum_price_listed'] += p
                        dm['count_price_listed'] += 1
                    except Exception:
                        pass
                dsold = (data.get('sold_at') or '')[:10]
                if dsold:
                    dm = daily_marketplace.setdefault(dsold, {'items_listed': 0, 'items_sold': 0, 'sum_price_listed': 0.0, 'count_price_listed': 0})
                    dm['items_sold'] += 1
            # Categories
            elif resource == 'categories':
                cid = data.get('id')
                if cid is not None:
                    category_name_by_id[cid] = data.get('name')
            # Sellers profiles (with category_ids)
            elif resource == 'sellers':
                for cid in (data.get('category_ids') or []):
                    sellers_count_per_category[cid] = sellers_count_per_category.get(cid, 0) + 1
            # chat_messages are not required for current aggregations

            t_proc += time.time() - t1
            processed += 1
            if processed % heartbeat_every_rows == 0:
                try:
                    activity.heartbeat({"rows": processed})
                except Exception:
                    pass

    # Finalize aggregates
    # Complete chats_participated and unique_senders counts
    for uid, st in user_stats.items():
        st['chats_participated'] = len(user_to_chat_ids.get(uid, set()))
    for cid, cs in chat_stats.items():
        cs['unique_senders'] = len(cs['unique_senders'])

    # Build final structures
    user_statistics = list(user_stats.values())
    chat_statistics = list(chat_stats.values())

    daily_stats_list = []
    for d, s in sorted(daily_stats.items()):
        daily_stats_list.append({
            'date': d,
            'total_messages': s['total_messages'],
            'unique_users': len(s['unique_users']),
            'unique_chats': len(s['unique_chats']),
            'private_messages': s['private_messages'],
            'group_messages': s['group_messages'],
        })

    hourly_stats_list = [
        {'hour': hour, 'total_messages': count}
        for hour, count in sorted(hourly_stats.items())
    ]

    # Weekday from daily_stats cannot be inferred accurately; compute from message loop was enough
    weekday_names = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    weekday_stats_list = [
        {'weekday': wd, 'weekday_name': weekday_names.get(wd, str(wd)), 'total_messages': s.get('total_messages', 0), 'unique_users': len(s.get('unique_users', set())), 'unique_chats': len(s.get('unique_chats', set()))}
        for wd, s in sorted(weekday_stats.items())
    ]
    # Note: weekday_stats remained empty in our streaming since we did not compute weekday precisely; omit
    weekday_stats_list = []

    message_type_list = [
        {'message_type': t, 'total_count': c}
        for t, c in sorted(message_type_stats.items())
    ]

    # Marketplace statistics finalize
    avg_price = (marketplace_stats['sum_price_all'] / marketplace_stats['count_price_all']) if marketplace_stats['count_price_all'] > 0 else 0
    marketplace_stats_out = {
        'total_items': marketplace_stats['total_items'],
        'active_items': marketplace_stats['active_items'],
        'sold_items': marketplace_stats['sold_items'],
        'cancelled_items': marketplace_stats['cancelled_items'],
        'total_revenue': marketplace_stats['total_revenue'],
        'average_price': avg_price,
        'top_sellers': [],
    }

    # Top sellers
    top_sellers_list = []
    for sid, agg in seller_agg.items():
        username = (users.get(sid) or {}).get('username', f'User {sid}')
        avg_list_price = (agg['sum_list_price'] / agg['count_list_price']) if agg['count_list_price'] > 0 else 0
        top_sellers_list.append({
            'seller_id': sid,
            'username': username,
            'items_sold': agg['sold'],
            'total_revenue': agg['sum_sold_value'],
        })
    top_sellers_list = sorted(top_sellers_list, key=lambda x: x['items_sold'], reverse=True)[:10]
    marketplace_stats_out['top_sellers'] = top_sellers_list

    # Category statistics
    category_statistics = []
    # We did not accumulate per-category aggregates incrementally; derive from marketplace seller_agg is not enough
    # Omit complex per-category totals in streaming transform to keep memory low
    # Provide empty if not available
    category_statistics = []

    # Seller statistics
    seller_statistics = []
    for sid, agg in seller_agg.items():
        username = (users.get(sid) or {}).get('username', f'User {sid}')
        avg_list_price = (agg['sum_list_price'] / agg['count_list_price']) if agg['count_list_price'] > 0 else 0
        seller_statistics.append({
            'seller_id': sid,
            'username': username,
            'total_items_listed': agg['listed'],
            'active_items': agg['active'],
            'sold_items': agg['sold'],
            'avg_listing_price': avg_list_price,
            'total_listed_value': agg['sum_list_price'],
            'total_sold_value': agg['sum_sold_value'],
        })

    chat_marketplace_stats = []
    for cid, agg in chat_mkt_agg.items():
        chat_marketplace_stats.append({
            'chat_id': cid,
            'chat_name': (chats.get(cid) or {}).get('name') or 'Chat Privado',
            'total_items': agg['total'],
            'active_items': agg['active'],
            'sold_items': agg['sold'],
        })

    daily_marketplace_stats = []
    for d, agg in sorted(daily_marketplace.items()):
        avg_listing_price = (agg['sum_price_listed'] / agg['count_price_listed']) if agg['count_price_listed'] > 0 else 0
        daily_marketplace_stats.append({
            'date': d,
            'items_listed': agg['items_listed'],
            'items_sold': agg['items_sold'],
            'avg_listing_price': avg_listing_price,
        })

    seller_category_stats = []
    for cid, count in sellers_count_per_category.items():
        seller_category_stats.append({
            'category_id': cid,
            'category_name': category_name_by_id.get(cid),
            'sellers_count': count,
        })

    out = {
        'user_statistics': user_statistics,
        'chat_statistics': chat_statistics,
        'daily_message_stats': daily_stats_list,
        'message_type_summary': message_type_list,
        'hourly_message_stats': hourly_stats_list,
        'weekday_message_stats': weekday_stats_list,
        'marketplace_statistics': marketplace_stats_out,
        'category_statistics': category_statistics,
        'seller_statistics': seller_statistics,
        'chat_marketplace_stats': chat_marketplace_stats,
        'daily_marketplace_stats': daily_marketplace_stats,
        'seller_category_stats': seller_category_stats,
    }

    # Output
    run_id = os.path.basename(raw_path).split("-raw.ndjson")[0].replace("etl-", "")
    out_path = f"/tmp/etl-{run_id}-transformed.json"
    t_write0 = time.time()
    _write_json(out_path, out)
    t_write = time.time() - t_write0
    print(f"[transform] rows={processed} proc_s={t_proc:.2f} write_s={t_write:.2f}")
    if not out.get('user_statistics'):
        raise ApplicationError("Transform validation failed: output empty", non_retryable=True)
    return out_path


@activity.defn
async def load_activity(transformed_path: str) -> str:
    """Load transformed data into analytics DB."""
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_heartbeat_loop("load", stop))
    try:
        transformed = await asyncio.to_thread(_read_json, transformed_path)
        if not isinstance(transformed, dict) or not transformed:
            raise ApplicationError("Load validation failed: transformed data missing", non_retryable=True)
        await asyncio.to_thread(etl.load_data, transformed)
        return "loaded"
    finally:
        stop.set()
        with contextlib.suppress(Exception):
            await hb_task


@activity.defn
async def cleanup_activity(paths: list[str]) -> int:
    """Delete temporary files if they exist. Returns count deleted."""
    deleted = 0
    for p in paths:
        try:
            if p and os.path.exists(p):
                await asyncio.to_thread(os.remove, p)
                deleted += 1
        except Exception:
            # Swallow cleanup errors
            pass
    return deleted


@workflow.defn
class ETLWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Use Temporal run ID for temp files across the workflow run
        run_id = workflow.info().run_id

        raw_path = None
        transformed_path = None
        try:
            raw_path = await workflow.execute_activity(
                extract_activity,
                run_id,
                start_to_close_timeout=timedelta(minutes=60),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=5),
                    maximum_attempts=3,
                ),
            )

            transformed_path = await workflow.execute_activity(
                transform_activity,
                raw_path,
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=5),
                    maximum_attempts=3,
                ),
            )

            _ = await workflow.execute_activity(
                load_activity,
                transformed_path,
                start_to_close_timeout=timedelta(minutes=45),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=5),
                    maximum_attempts=3,
                ),
            )
            return "completed"
        finally:
            # Best-effort cleanup of temp files via activity (workflow code must remain deterministic)
            paths = [p for p in [raw_path, transformed_path] if p]
            if paths:
                try:
                    await workflow.execute_activity(
                        cleanup_activity,
                        paths,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=RetryPolicy(maximum_attempts=1),
                    )
                except Exception:
                    pass


async def main() -> None:
    from temporalio.client import Client

    address = os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
    # Retry connecting until Temporal is ready
    client = None
    backoff_seconds = 2
    while client is None:
        try:
            client = await Client.connect(address)
        except Exception:
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)

    task_queue = "etl-task-queue"
    activity_workers = int(os.getenv("ETL_ACTIVITY_WORKERS", "8"))
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ETLWorkflow],
        activities=[extract_activity, transform_activity, load_activity, cleanup_activity],
        workflow_runner=UnsandboxedWorkflowRunner(),
        activity_executor=ThreadPoolExecutor(max_workers=activity_workers),
        max_concurrent_activities=activity_workers,
    )

    print(f"Worker started. Connected to {address}. Task queue: {task_queue}")
    await worker.run()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())


