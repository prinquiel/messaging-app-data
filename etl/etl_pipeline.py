import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import execute_batch
import os
from datetime import datetime
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


API_BASE_URL = os.getenv('API_URL', 'http://localhost:8000')
MAX_PAGE_SIZE = 250  # Máximo permitido por el API
MAX_HTTP_CONCURRENCY = int(os.getenv('ETL_MAX_HTTP_CONCURRENCY', '8'))
MAX_CHAT_MESSAGE_CHATS = int(os.getenv('ETL_MAX_CHAT_MESSAGE_CHATS', '500'))  # cuántos chats para extraer mensajes
REQUEST_TIMEOUT_SECONDS = float(os.getenv('ETL_REQUEST_TIMEOUT', '30'))
RETRY_TOTAL = int(os.getenv('ETL_HTTP_RETRY_TOTAL', '5'))
RETRY_BACKOFF = float(os.getenv('ETL_HTTP_RETRY_BACKOFF', '0.5'))

ANALYTICS_DB_CONFIG = {
    'host': os.getenv('ANALYTICS_DB_HOST', 'localhost'),
    'port': os.getenv('ANALYTICS_DB_PORT', '5434'),
    'database': os.getenv('ANALYTICS_DB_NAME', 'analyticsdb'),
    'user': os.getenv('ANALYTICS_DB_USER', 'analyticsuser'),
    'password': os.getenv('ANALYTICS_DB_PASSWORD', 'analyticspassword')
}


# ==================== EXTRACT ====================

def _build_session():
    session = requests.Session()
    retries = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=MAX_HTTP_CONCURRENCY, pool_maxsize=MAX_HTTP_CONCURRENCY)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def extract_all_data(endpoint, resource_name):

    # Extrae TODOS los datos de un endpoint con paginación automática.
    
    print(f"\n📥 EXTRAYENDO {resource_name.upper()}...")
    
    all_items = []
    session = _build_session()
    
    # First page to learn total_pages
    url = f"{API_BASE_URL}{endpoint}"
    params = {
        'page': 1,
        'page_size': MAX_PAGE_SIZE,
        'include_total': True
    }
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()
    first_items = data.get('items', [])
    total = data.get('total', 0)
    total_pages = data.get('total_pages', 0)
    all_items.extend(first_items)
    print(f"   Página 1/{total_pages}: {len(first_items)} {resource_name} (Total acumulado: {len(all_items):,}/{total:,})")

    # Fetch remaining pages concurrently with bounded pool
    def fetch_page(p):
        try:
            r = session.get(url, params={**params, 'page': p}, timeout=REQUEST_TIMEOUT_SECONDS)
            r.raise_for_status()
            return p, r.json().get('items', [])
        except Exception as e:
            print(f"   ❌ Error extrayendo {resource_name} en página {p}: {e}")
            return p, []

    if total_pages and total_pages > 1:
        with ThreadPoolExecutor(max_workers=MAX_HTTP_CONCURRENCY) as pool:
            futures = [pool.submit(fetch_page, p) for p in range(2, total_pages + 1)]
            for fut in as_completed(futures):
                p, items = fut.result()
                all_items.extend(items)
                print(f"   Página {p}/{total_pages}: {len(items)} {resource_name} (Total acumulado: {len(all_items):,}/{total:,})")
    
    print(f"✅ Extracción completada: {len(all_items):,} {resource_name} obtenidos")
    return all_items


def extract_all_resources():
    """
    Extrae todos los recursos del API.
    
    Returns:
        Diccionario con todos los datos extraídos
    """
    print("\n" + "="*70)
    print("🔄 FASE 1: EXTRACT (Extracción de datos del API)")
    print("="*70)
    
    session = _build_session()
    try:
        response = session.get(f"{API_BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            raise Exception("API no está saludable")
        print("✅ API disponible y funcionando")
    except Exception as e:
        print(f"❌ No se puede conectar al API: {e}")
        print(f"   Asegúrate que el API esté corriendo en {API_BASE_URL}")
        raise
    
    # Extraer todos los recursos
    data = {
        'users': extract_all_data('/users', 'usuarios'),
        'chats': extract_all_data('/chats', 'chats'),
        'messages': extract_all_data('/messages', 'mensajes'),
        'marketplace_items': extract_all_data('/marketplace', 'items de marketplace'),
        'categories': extract_all_data('/categories', 'categorías de marketplace'),
        'sellers': extract_all_data('/sellers', 'perfiles de vendedores'),
    }
    
    print("\n📥 EXTRAYENDO MENSAJES POR CHAT...")
    chat_messages = {}
    # Concurrent extraction of messages per chat with bounded concurrency
    target_chats = data['chats'][:MAX_CHAT_MESSAGE_CHATS]
    print(f"📥 Extrayendo mensajes por chat de {len(target_chats)} chats con concurrencia {MAX_HTTP_CONCURRENCY}...")
    session = _build_session()
    def fetch_chat_messages(chat):
        chat_id = chat['id']
        # Reuse generic extractor to leverage pagination concurrency
        msgs = extract_all_data(f'/chats/{chat_id}/messages', f'mensajes del chat {chat_id}')
        return chat_id, msgs
    with ThreadPoolExecutor(max_workers=MAX_HTTP_CONCURRENCY) as pool:
        futures = [pool.submit(fetch_chat_messages, chat) for chat in target_chats]
        for i, fut in enumerate(as_completed(futures), 1):
            cid, msgs = fut.result()
            chat_messages[cid] = msgs
            if i % 20 == 0:
                print(f"   Progreso: {i}/{len(target_chats)} chats procesados")
    
    data['chat_messages'] = chat_messages
    
    return data


# ==================== TRANSFORM ====================

def transform_data(raw_data):
    """
    Transforma los datos crudos en datos analíticos agregados.
    
    Los datos operacionales (mensajes individuales, usuarios) se convierten en:
    - Estadísticas por usuario
    - Estadísticas por chat
    - Métricas diarias
    - Resúmenes de actividad
    
    Esto es lo que hace útil un ETL: convierte datos transaccionales en
    información analítica procesada y lista para dashboards.
    """
    print("\n" + "="*70)
    print("⚙️  FASE 2: TRANSFORM (Transformación y agregación de datos)")
    print("="*70)
    
    users = raw_data['users']
    chats = raw_data['chats']
    messages = raw_data['messages']
    marketplace_items = raw_data.get('marketplace_items', [])
    categories = raw_data.get('categories', [])
    sellers = raw_data.get('sellers', [])
    
    print("\n📊 Calculando estadísticas por usuario...")
    user_stats = {}
    # Para calcular en cuántos chats ha participado cada usuario
    user_to_chat_ids = defaultdict(set)
    
    for user in users:
        user_id = user['id']
        user_stats[user_id] = {
            'user_id': user_id,
            'username': user['username'],
            'total_messages_sent': 0,
            'chats_participated': 0,
            'last_message_date': None,
            'is_active': user['is_active'],
            'created_at': user['created_at']
        }
    
    for message in messages:
        sender_id = message['sender_id']
        if sender_id in user_stats:
            user_stats[sender_id]['total_messages_sent'] += 1
            user_to_chat_ids[sender_id].add(message['chat_id'])
            
            sent_at = message['sent_at']
            if (user_stats[sender_id]['last_message_date'] is None or 
                sent_at > user_stats[sender_id]['last_message_date']):
                user_stats[sender_id]['last_message_date'] = sent_at
    
    # Completar chats_participated
    for user_id in user_stats:
        user_stats[user_id]['chats_participated'] = len(user_to_chat_ids[user_id])
    print(f"   ✅ {len(user_stats):,} usuarios procesados")
    
    print("\n📊 Calculando estadísticas por chat...")
    chat_stats = {}
    
    for chat in chats:
        chat_id = chat['id']
        chat_stats[chat_id] = {
            'chat_id': chat_id,
            'chat_name': chat.get('name', 'Chat Privado'),
            'chat_type': chat['chat_type'],
            'total_messages': 0,
            'unique_senders': set(),
            'first_message_date': None,
            'last_message_date': None,
            'created_at': chat['created_at']
        }
    
    for message in messages:
        chat_id = message['chat_id']
        if chat_id in chat_stats:
            chat_stats[chat_id]['total_messages'] += 1
            chat_stats[chat_id]['unique_senders'].add(message['sender_id'])
            
            sent_at = message['sent_at']
            
            if (chat_stats[chat_id]['first_message_date'] is None or 
                sent_at < chat_stats[chat_id]['first_message_date']):
                chat_stats[chat_id]['first_message_date'] = sent_at
            
            if (chat_stats[chat_id]['last_message_date'] is None or 
                sent_at > chat_stats[chat_id]['last_message_date']):
                chat_stats[chat_id]['last_message_date'] = sent_at
    
    for chat_id in chat_stats:
        chat_stats[chat_id]['unique_senders'] = len(chat_stats[chat_id]['unique_senders'])
    
    print(f"   ✅ {len(chat_stats):,} chats procesados")
    
    print("\n📊 Calculando métricas diarias...")
    daily_stats = defaultdict(lambda: {
        'total_messages': 0,
        'unique_users': set(),
        'unique_chats': set(),
        'private_messages': 0,
        'group_messages': 0
    })
    # Métricas por hora del día (0-23)
    hourly_stats = defaultdict(int)
    
    chat_type_map = {chat['id']: chat['chat_type'] for chat in chats}
    
    for message in messages:
        date_str = message['sent_at'][:10]  # YYYY-MM-DD
        # Contar por hora del día
        try:
            hour = int(message['sent_at'][11:13])
            if 0 <= hour <= 23:
                hourly_stats[hour] += 1
        except Exception:
            pass
        
        daily_stats[date_str]['total_messages'] += 1
        daily_stats[date_str]['unique_users'].add(message['sender_id'])
        daily_stats[date_str]['unique_chats'].add(message['chat_id'])
        
        chat_type = chat_type_map.get(message['chat_id'], 'private')
        if chat_type == 'private':
            daily_stats[date_str]['private_messages'] += 1
        else:
            daily_stats[date_str]['group_messages'] += 1
    
    daily_stats_list = []
    for date_str, stats in sorted(daily_stats.items()):
        daily_stats_list.append({
            'date': date_str,
            'total_messages': stats['total_messages'],
            'unique_users': len(stats['unique_users']),
            'unique_chats': len(stats['unique_chats']),
            'private_messages': stats['private_messages'],
            'group_messages': stats['group_messages']
        })
    
    print(f"   ✅ {len(daily_stats_list):,} días con actividad procesados")

    # Preparar métricas por hora del día
    hourly_stats_list = [
        {'hour': hour, 'total_messages': count}
        for hour, count in sorted(hourly_stats.items())
    ]
    
    print("\n📊 Analizando tipos de mensajes...")
    message_type_stats = defaultdict(int)
    
    for message in messages:
        message_type = message.get('message_type', 'text')
        message_type_stats[message_type] += 1
    
    message_type_list = [
        {'message_type': msg_type, 'total_count': count}
        for msg_type, count in message_type_stats.items()
    ]
    
    print(f"   ✅ {len(message_type_list)} tipos de mensajes encontrados")
    
    # Marketplace analytics
    print("\n📊 Calculando estadísticas de marketplace...")
    marketplace_stats = {
        'total_items': len(marketplace_items),
        'active_items': sum(1 for item in marketplace_items if item.get('status') == 'active'),
        'sold_items': sum(1 for item in marketplace_items if item.get('status') == 'sold'),
        'cancelled_items': sum(1 for item in marketplace_items if item.get('status') == 'cancelled'),
        'total_revenue': sum(float(item.get('price', 0)) for item in marketplace_items if item.get('status') == 'sold'),
        'average_price': 0,
        'top_sellers': {}
    }
    
    # Calculate average price
    priced_items = [item for item in marketplace_items if item.get('price')]
    if priced_items:
        marketplace_stats['average_price'] = sum(float(item.get('price', 0)) for item in priced_items) / len(priced_items)
    
    # Top sellers by items sold
    seller_item_count = defaultdict(int)
    seller_revenue = defaultdict(float)
    
    for item in marketplace_items:
        seller_id = item.get('seller_id')
        if seller_id:
            seller_item_count[seller_id] += 1
            if item.get('status') == 'sold':
                seller_revenue[seller_id] += float(item.get('price', 0))
    
    # Get top 10 sellers
    top_sellers_list = []
    for seller_id in sorted(seller_item_count.keys(), key=lambda x: seller_item_count[x], reverse=True)[:10]:
        username = next((u['username'] for u in users if u['id'] == seller_id), f"User {seller_id}")
        top_sellers_list.append({
            'seller_id': seller_id,
            'username': username,
            'items_sold': seller_item_count[seller_id],
            'total_revenue': seller_revenue.get(seller_id, 0)
        })
    
    marketplace_stats['top_sellers'] = top_sellers_list
    print(f"   ✅ {len(marketplace_items):,} items procesados")
    print(f"   ✅ {len(top_sellers_list)} top sellers identificados")
    
    # Weekday message stats (0=Monday)
    from datetime import datetime as _dt
    weekday_stats = defaultdict(lambda: {
        'total_messages': 0,
        'unique_users': set(),
        'unique_chats': set(),
    })
    for msg in messages:
        ts = msg.get('sent_at')
        wd = None
        try:
            # Try ISO parse
            wd = _dt.fromisoformat(ts.replace('Z', '+00:00')).weekday()
        except Exception:
            try:
                wd = int(ts[8:10])  # Fallback nonsense to avoid crash
                wd = wd % 7
            except Exception:
                wd = None
        if wd is not None:
            weekday_stats[wd]['total_messages'] += 1
            weekday_stats[wd]['unique_users'].add(msg['sender_id'])
            weekday_stats[wd]['unique_chats'].add(msg['chat_id'])
    weekday_names = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    weekday_stats_list = []
    for wd in range(7):
        s = weekday_stats[wd]
        weekday_stats_list.append({
            'weekday': wd,
            'weekday_name': weekday_names[wd],
            'total_messages': s['total_messages'],
            'unique_users': len(s['unique_users']),
            'unique_chats': len(s['unique_chats']),
        })

    # Category statistics (overall)
    category_name_by_id = {c['id']: c.get('name') for c in categories}
    category_agg = defaultdict(lambda: {
        'total': 0, 'active': 0, 'sold': 0, 'cancelled': 0, 'sum_price': 0.0, 'priced_count': 0
    })
    for item in marketplace_items:
        cat_id = item.get('category_id')
        status = item.get('status')
        category_agg[cat_id]['total'] += 1
        if status == 'active':
            category_agg[cat_id]['active'] += 1
        if status == 'sold':
            category_agg[cat_id]['sold'] += 1
        if status == 'cancelled':
            category_agg[cat_id]['cancelled'] += 1
        try:
            price_val = float(item.get('price')) if item.get('price') is not None else None
            if price_val is not None:
                category_agg[cat_id]['sum_price'] += price_val
                category_agg[cat_id]['priced_count'] += 1
        except Exception:
            pass
    category_statistics = []
    for cat_id, agg in category_agg.items():
        # Skip uncategorized items (category_id is None) to avoid NULL PK inserts
        if cat_id is None:
            continue
        avg_price = (agg['sum_price'] / agg['priced_count']) if agg['priced_count'] > 0 else 0
        category_statistics.append({
            'category_id': cat_id,
            'category_name': category_name_by_id.get(cat_id),
            'total_items': agg['total'],
            'active_items': agg['active'],
            'sold_items': agg['sold'],
            'cancelled_items': agg['cancelled'],
            'avg_price': avg_price,
        })

    # Seller statistics (overall)
    username_by_id = {u['id']: u['username'] for u in users}
    seller_agg = defaultdict(lambda: {
        'listed': 0, 'active': 0, 'sold': 0,
        'sum_list_price': 0.0, 'count_list_price': 0,
        'sum_sold_value': 0.0,
    })
    for item in marketplace_items:
        sid = item.get('seller_id')
        if sid is None:
            continue
        seller_agg[sid]['listed'] += 1
        if item.get('status') == 'active':
            seller_agg[sid]['active'] += 1
        if item.get('status') == 'sold':
            seller_agg[sid]['sold'] += 1
            try:
                seller_agg[sid]['sum_sold_value'] += float(item.get('price') or 0)
            except Exception:
                pass
        try:
            p = float(item.get('price')) if item.get('price') is not None else None
            if p is not None:
                seller_agg[sid]['sum_list_price'] += p
                seller_agg[sid]['count_list_price'] += 1
        except Exception:
            pass
    seller_statistics = []
    for sid, agg in seller_agg.items():
        avg_list_price = (agg['sum_list_price'] / agg['count_list_price']) if agg['count_list_price'] > 0 else 0
        seller_statistics.append({
            'seller_id': sid,
            'username': username_by_id.get(sid, f'User {sid}'),
            'total_items_listed': agg['listed'],
            'active_items': agg['active'],
            'sold_items': agg['sold'],
            'avg_listing_price': avg_list_price,
            'total_listed_value': agg['sum_list_price'],
            'total_sold_value': agg['sum_sold_value'],
        })

    # Chat marketplace statistics (overall)
    chat_name_by_id = {c['id']: (c.get('name') or 'Chat Privado') for c in chats}
    chat_mkt_agg = defaultdict(lambda: {'total': 0, 'active': 0, 'sold': 0})
    for item in marketplace_items:
        cid = item.get('chat_id')
        chat_mkt_agg[cid]['total'] += 1
        if item.get('status') == 'active':
            chat_mkt_agg[cid]['active'] += 1
        if item.get('status') == 'sold':
            chat_mkt_agg[cid]['sold'] += 1
    chat_marketplace_stats = []
    for cid, agg in chat_mkt_agg.items():
        chat_marketplace_stats.append({
            'chat_id': cid,
            'chat_name': chat_name_by_id.get(cid),
            'total_items': agg['total'],
            'active_items': agg['active'],
            'sold_items': agg['sold'],
        })

    # Daily marketplace statistics
    def _date_only(ts: str):
        try:
            return ts[:10]
        except Exception:
            return None
    daily_marketplace = defaultdict(lambda: {
        'items_listed': 0, 'items_sold': 0, 'sum_price_listed': 0.0, 'count_price_listed': 0
    })
    for item in marketplace_items:
        d = _date_only(item.get('created_at'))
        if d:
            daily_marketplace[d]['items_listed'] += 1
            try:
                p = float(item.get('price') or 0)
                daily_marketplace[d]['sum_price_listed'] += p
                daily_marketplace[d]['count_price_listed'] += 1
            except Exception:
                pass
        sold_d = _date_only(item.get('sold_at'))
        if sold_d:
            daily_marketplace[sold_d]['items_sold'] += 1
    daily_marketplace_stats = []
    for d, agg in sorted(daily_marketplace.items()):
        avg_list_price = (agg['sum_price_listed'] / agg['count_price_listed']) if agg['count_price_listed'] > 0 else 0
        daily_marketplace_stats.append({
            'date': d,
            'items_listed': agg['items_listed'],
            'items_sold': agg['items_sold'],
            'avg_listing_price': avg_list_price,
        })

    # Seller category coverage (count sellers per category)
    sellers_count_per_category = defaultdict(int)
    for sp in sellers:
        for cid in (sp.get('category_ids') or []):
            sellers_count_per_category[cid] += 1
    seller_category_stats = []
    for cid, count in sellers_count_per_category.items():
        seller_category_stats.append({
            'category_id': cid,
            'category_name': category_name_by_id.get(cid),
            'sellers_count': count,
        })

    return {
        'user_statistics': list(user_stats.values()),
        'chat_statistics': list(chat_stats.values()),
        'daily_message_stats': daily_stats_list,
        'message_type_summary': message_type_list,
        'hourly_message_stats': hourly_stats_list,
        'weekday_message_stats': weekday_stats_list,
        'marketplace_statistics': marketplace_stats,
        'category_statistics': category_statistics,
        'seller_statistics': seller_statistics,
        'chat_marketplace_stats': chat_marketplace_stats,
        'daily_marketplace_stats': daily_marketplace_stats,
        'seller_category_stats': seller_category_stats,
    }


# ==================== LOAD ====================

def create_analytics_tables(conn):
    """
    Crea las tablas en la base de datos analítica.
    
    Estas tablas son diferentes a las tablas operacionales:
    - Están optimizadas para lectura (no para escritura)
    - Contienen datos agregados y pre-calculados
    - Son perfectas para dashboards y reportes
    """
    print("\n📋 Creando tablas analíticas...")
    
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_statistics (
            user_id INTEGER PRIMARY KEY,
            username VARCHAR(50),
            total_messages_sent INTEGER DEFAULT 0,
            chats_participated INTEGER DEFAULT 0,
            last_message_date TIMESTAMP,
            is_active BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chat_statistics (
            chat_id INTEGER PRIMARY KEY,
            chat_name VARCHAR(100),
            chat_type VARCHAR(20),
            total_messages INTEGER DEFAULT 0,
            unique_senders INTEGER DEFAULT 0,
            first_message_date TIMESTAMP,
            last_message_date TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_message_stats (
            date DATE PRIMARY KEY,
            total_messages INTEGER DEFAULT 0,
            unique_users INTEGER DEFAULT 0,
            unique_chats INTEGER DEFAULT 0,
            private_messages INTEGER DEFAULT 0,
            group_messages INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS message_type_summary (
            message_type VARCHAR(20) PRIMARY KEY,
            total_count INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_message_stats (
            hour SMALLINT PRIMARY KEY,
            total_messages INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weekday_message_stats (
            weekday SMALLINT PRIMARY KEY, -- 0=Mon .. 6=Sun
            weekday_name VARCHAR(10),
            total_messages INTEGER DEFAULT 0,
            unique_users INTEGER DEFAULT 0,
            unique_chats INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS marketplace_statistics (
            id SERIAL PRIMARY KEY,
            total_items INTEGER DEFAULT 0,
            active_items INTEGER DEFAULT 0,
            sold_items INTEGER DEFAULT 0,
            cancelled_items INTEGER DEFAULT 0,
            total_revenue NUMERIC(12, 2) DEFAULT 0,
            average_price NUMERIC(10, 2) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_sellers (
            seller_id INTEGER PRIMARY KEY,
            username VARCHAR(50),
            items_sold INTEGER DEFAULT 0,
            total_revenue NUMERIC(12, 2) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category_statistics (
            category_id INTEGER PRIMARY KEY,
            category_name VARCHAR(100),
            total_items INTEGER DEFAULT 0,
            active_items INTEGER DEFAULT 0,
            sold_items INTEGER DEFAULT 0,
            cancelled_items INTEGER DEFAULT 0,
            avg_price NUMERIC(10, 2) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS seller_statistics (
            seller_id INTEGER PRIMARY KEY,
            username VARCHAR(50),
            total_items_listed INTEGER DEFAULT 0,
            active_items INTEGER DEFAULT 0,
            sold_items INTEGER DEFAULT 0,
            avg_listing_price NUMERIC(10, 2) DEFAULT 0,
            total_listed_value NUMERIC(12, 2) DEFAULT 0,
            total_sold_value NUMERIC(12, 2) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chat_marketplace_stats (
            chat_id INTEGER PRIMARY KEY,
            chat_name VARCHAR(100),
            total_items INTEGER DEFAULT 0,
            active_items INTEGER DEFAULT 0,
            sold_items INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_marketplace_stats (
            date DATE PRIMARY KEY,
            items_listed INTEGER DEFAULT 0,
            items_sold INTEGER DEFAULT 0,
            avg_listing_price NUMERIC(10, 2) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS seller_category_stats (
            category_id INTEGER PRIMARY KEY,
            category_name VARCHAR(100),
            sellers_count INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_runs (
            id SERIAL PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR(20),
            notes TEXT
        )
    """)
    
    conn.commit()
    cursor.close()
    print("   ✅ Tablas analíticas creadas/verificadas")


def load_data(transformed_data):
    """
    Carga los datos transformados en la base de datos analítica.
    
    Esta es la fase final del ETL donde los datos procesados
    se guardan para ser consumidos por dashboards y reportes.
    """
    print("\n" + "="*70)
    print("💾 FASE 3: LOAD (Carga de datos en base de datos analítica)")
    print("="*70)
    
    started_at = datetime.utcnow()
    run_status = "success"
    try:
        conn = psycopg2.connect(**ANALYTICS_DB_CONFIG)
        print(f"✅ Conectado a base de datos analítica: {ANALYTICS_DB_CONFIG['database']}")
    except Exception as e:
        print(f"❌ Error conectando a base de datos analítica: {e}")
        raise
    
    create_analytics_tables(conn)
    
    cursor = conn.cursor()
    
    print("\n📥 Cargando estadísticas de usuarios (upsert)...")
    
    user_data = [
        (
            stat['user_id'],
            stat['username'],
            stat['total_messages_sent'],
            stat['chats_participated'],
            stat['last_message_date'],
            stat['is_active'],
            stat['created_at']
        )
        for stat in transformed_data['user_statistics']
    ]
    
    t0 = time.time()
    execute_batch(cursor, """
        INSERT INTO user_statistics 
        (user_id, username, total_messages_sent, chats_participated, 
         last_message_date, is_active, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
          username = EXCLUDED.username,
          total_messages_sent = EXCLUDED.total_messages_sent,
          chats_participated = EXCLUDED.chats_participated,
          last_message_date = EXCLUDED.last_message_date,
          is_active = EXCLUDED.is_active,
          created_at = EXCLUDED.created_at
    """, user_data, page_size=1000)
    print(f"   ✅ Upsert usuarios: {len(user_data):,} filas en {time.time()-t0:.2f}s")
    
    print("\n📥 Cargando estadísticas de chats (upsert)...")
    
    chat_data = [
        (
            stat['chat_id'],
            stat['chat_name'],
            stat['chat_type'],
            stat['total_messages'],
            stat['unique_senders'],
            stat['first_message_date'],
            stat['last_message_date'],
            stat['created_at']
        )
        for stat in transformed_data['chat_statistics']
    ]
    
    t0 = time.time()
    execute_batch(cursor, """
        INSERT INTO chat_statistics 
        (chat_id, chat_name, chat_type, total_messages, unique_senders,
         first_message_date, last_message_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (chat_id) DO UPDATE SET
          chat_name = EXCLUDED.chat_name,
          chat_type = EXCLUDED.chat_type,
          total_messages = EXCLUDED.total_messages,
          unique_senders = EXCLUDED.unique_senders,
          first_message_date = EXCLUDED.first_message_date,
          last_message_date = EXCLUDED.last_message_date,
          created_at = EXCLUDED.created_at
    """, chat_data, page_size=1000)
    print(f"   ✅ Upsert chats: {len(chat_data):,} filas en {time.time()-t0:.2f}s")
    
    print("\n📥 Cargando métricas diarias (upsert)...")
    
    daily_data = [
        (
            stat['date'],
            stat['total_messages'],
            stat['unique_users'],
            stat['unique_chats'],
            stat['private_messages'],
            stat['group_messages']
        )
        for stat in transformed_data['daily_message_stats']
    ]
    
    t0 = time.time()
    execute_batch(cursor, """
        INSERT INTO daily_message_stats 
        (date, total_messages, unique_users, unique_chats, 
         private_messages, group_messages)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
          total_messages = EXCLUDED.total_messages,
          unique_users = EXCLUDED.unique_users,
          unique_chats = EXCLUDED.unique_chats,
          private_messages = EXCLUDED.private_messages,
          group_messages = EXCLUDED.group_messages
    """, daily_data, page_size=1000)
    print(f"   ✅ Upsert daily_message_stats: {len(daily_data):,} filas en {time.time()-t0:.2f}s")
    
    print("\n📥 Cargando resumen de tipos de mensaje (upsert)...")
    
    type_data = [
        (stat['message_type'], stat['total_count'])
        for stat in transformed_data['message_type_summary']
    ]
    
    t0 = time.time()
    execute_batch(cursor, """
        INSERT INTO message_type_summary (message_type, total_count)
        VALUES (%s, %s)
        ON CONFLICT (message_type) DO UPDATE SET
          total_count = EXCLUDED.total_count
    """, type_data, page_size=100)
    print(f"   ✅ Upsert message_type_summary: {len(type_data)} filas en {time.time()-t0:.2f}s")

    print("\n📥 Cargando métricas por hora del día (upsert)...")

    hourly_data = [
        (stat['hour'], stat['total_messages'])
        for stat in transformed_data['hourly_message_stats']
    ]

    t0 = time.time()
    execute_batch(cursor, """
        INSERT INTO hourly_message_stats (hour, total_messages)
        VALUES (%s, %s)
        ON CONFLICT (hour) DO UPDATE SET
          total_messages = EXCLUDED.total_messages
    """, hourly_data, page_size=100)
    print(f"   ✅ Upsert hourly_message_stats: {len(hourly_data)} filas en {time.time()-t0:.2f}s")
    
    print("\n📥 Cargando métricas por día de la semana (upsert)...")
    weekday_data = [
        (stat['weekday'], stat['weekday_name'], stat['total_messages'], stat['unique_users'], stat['unique_chats'])
        for stat in transformed_data['weekday_message_stats']
    ]
    t0 = time.time()
    execute_batch(cursor, """
        INSERT INTO weekday_message_stats (weekday, weekday_name, total_messages, unique_users, unique_chats)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (weekday) DO UPDATE SET
          weekday_name = EXCLUDED.weekday_name,
          total_messages = EXCLUDED.total_messages,
          unique_users = EXCLUDED.unique_users,
          unique_chats = EXCLUDED.unique_chats
    """, weekday_data, page_size=100)
    print(f"   ✅ Upsert weekday_message_stats: {len(weekday_data)} registros en {time.time()-t0:.2f}s")
    
    print("\n📥 Cargando estadísticas de marketplace (replace)...")
    
    marketplace_stats = transformed_data.get('marketplace_statistics', {})
    cursor.execute("""
        INSERT INTO marketplace_statistics 
        (total_items, active_items, sold_items, cancelled_items, total_revenue, average_price)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        marketplace_stats.get('total_items', 0),
        marketplace_stats.get('active_items', 0),
        marketplace_stats.get('sold_items', 0),
        marketplace_stats.get('cancelled_items', 0),
        marketplace_stats.get('total_revenue', 0),
        marketplace_stats.get('average_price', 0)
    ))
    
    print(f"   ✅ Estadísticas de marketplace cargadas")
    
    print("\n📥 Cargando top sellers (upsert)...")
    
    top_sellers = marketplace_stats.get('top_sellers', [])
    if top_sellers:
        seller_data = [
            (seller['seller_id'], seller['username'], seller['items_sold'], seller['total_revenue'])
            for seller in top_sellers
        ]
        
        t0 = time.time()
        execute_batch(cursor, """
            INSERT INTO top_sellers (seller_id, username, items_sold, total_revenue)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (seller_id) DO UPDATE SET
              username = EXCLUDED.username,
              items_sold = EXCLUDED.items_sold,
              total_revenue = EXCLUDED.total_revenue
        """, seller_data, page_size=100)
        print(f"   ✅ Upsert top_sellers: {len(seller_data)} filas en {time.time()-t0:.2f}s")
    else:
        print("   ⚠️  No hay top sellers para cargar")
    
    print("\n📥 Cargando estadísticas por categoría (upsert)...")
    category_rows = [
        (
            row['category_id'], row.get('category_name'), row['total_items'],
            row['active_items'], row['sold_items'], row['cancelled_items'], row['avg_price']
        )
        for row in transformed_data.get('category_statistics', [])
    ]
    if category_rows:
        t0 = time.time()
        execute_batch(cursor, """
            INSERT INTO category_statistics
            (category_id, category_name, total_items, active_items, sold_items, cancelled_items, avg_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (category_id) DO UPDATE SET
              category_name = EXCLUDED.category_name,
              total_items = EXCLUDED.total_items,
              active_items = EXCLUDED.active_items,
              sold_items = EXCLUDED.sold_items,
              cancelled_items = EXCLUDED.cancelled_items,
              avg_price = EXCLUDED.avg_price
        """, category_rows, page_size=1000)
        print(f"   ✅ Upsert category_statistics: {len(category_rows)} filas en {time.time()-t0:.2f}s")
    print(f"   ✅ {len(category_rows)} categorías cargadas")
    
    print("\n📥 Cargando estadísticas de vendedores (upsert)...")
    seller_rows = [
        (
            row['seller_id'], row.get('username'), row['total_items_listed'], row['active_items'],
            row['sold_items'], row['avg_listing_price'], row['total_listed_value'], row['total_sold_value']
        )
        for row in transformed_data.get('seller_statistics', [])
    ]
    if seller_rows:
        t0 = time.time()
        execute_batch(cursor, """
            INSERT INTO seller_statistics
            (seller_id, username, total_items_listed, active_items, sold_items, avg_listing_price, total_listed_value, total_sold_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (seller_id) DO UPDATE SET
              username = EXCLUDED.username,
              total_items_listed = EXCLUDED.total_items_listed,
              active_items = EXCLUDED.active_items,
              sold_items = EXCLUDED.sold_items,
              avg_listing_price = EXCLUDED.avg_listing_price,
              total_listed_value = EXCLUDED.total_listed_value,
              total_sold_value = EXCLUDED.total_sold_value
        """, seller_rows, page_size=1000)
        print(f"   ✅ Upsert seller_statistics: {len(seller_rows)} filas en {time.time()-t0:.2f}s")
    print(f"   ✅ {len(seller_rows)} vendedores cargados")
    
    print("\n📥 Cargando estadísticas de marketplace por chat (upsert)...")
    chat_rows = [
        (
            row['chat_id'], row.get('chat_name'), row['total_items'], row['active_items'], row['sold_items']
        )
        for row in transformed_data.get('chat_marketplace_stats', [])
    ]
    if chat_rows:
        t0 = time.time()
        execute_batch(cursor, """
            INSERT INTO chat_marketplace_stats (chat_id, chat_name, total_items, active_items, sold_items)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE SET
              chat_name = EXCLUDED.chat_name,
              total_items = EXCLUDED.total_items,
              active_items = EXCLUDED.active_items,
              sold_items = EXCLUDED.sold_items
        """, chat_rows, page_size=1000)
        print(f"   ✅ Upsert chat_marketplace_stats: {len(chat_rows)} filas en {time.time()-t0:.2f}s")
    print(f"   ✅ {len(chat_rows)} chats (marketplace) cargados")
    
    print("\n📥 Cargando métricas diarias de marketplace (upsert)...")
    daily_mkt_rows = [
        (
            row['date'], row['items_listed'], row['items_sold'], row['avg_listing_price']
        )
        for row in transformed_data.get('daily_marketplace_stats', [])
    ]
    if daily_mkt_rows:
        t0 = time.time()
        execute_batch(cursor, """
            INSERT INTO daily_marketplace_stats (date, items_listed, items_sold, avg_listing_price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
              items_listed = EXCLUDED.items_listed,
              items_sold = EXCLUDED.items_sold,
              avg_listing_price = EXCLUDED.avg_listing_price
        """, daily_mkt_rows, page_size=1000)
        print(f"   ✅ Upsert daily_marketplace_stats: {len(daily_mkt_rows)} filas en {time.time()-t0:.2f}s")
    print(f"   ✅ {len(daily_mkt_rows)} registros diarios de marketplace cargados")
    
    print("\n📥 Cargando conteo de vendedores por categoría (upsert)...")
    seller_cat_rows = [
        (
            row['category_id'], row.get('category_name'), row['sellers_count']
        )
        for row in transformed_data.get('seller_category_stats', [])
    ]
    if seller_cat_rows:
        t0 = time.time()
        execute_batch(cursor, """
            INSERT INTO seller_category_stats (category_id, category_name, sellers_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (category_id) DO UPDATE SET
              category_name = EXCLUDED.category_name,
              sellers_count = EXCLUDED.sellers_count
        """, seller_cat_rows, page_size=1000)
        print(f"   ✅ Upsert seller_category_stats: {len(seller_cat_rows)} filas en {time.time()-t0:.2f}s")
    print(f"   ✅ {len(seller_cat_rows)} filas de seller_category_stats cargadas")
    
    conn.commit()
    # Record ETL run
    try:
        cursor.execute(
            "INSERT INTO etl_runs (started_at, finished_at, status, notes) VALUES (%s, %s, %s, %s)",
            (started_at, datetime.utcnow(), run_status, "ETL completed and all analytical tables refreshed")
        )
        conn.commit()
    except Exception as e:
        print(f"⚠️  No se pudo registrar etl_runs: {e}")
    cursor.close()
    conn.close()
    
    print("\n✅ Todos los datos cargados exitosamente en la base de datos analítica")



def main():
    """
    Función principal que ejecuta el ETL completo.
    """
    print("\n" + "="*70)
    print("🚀 ETL PIPELINE - MESSAGING APP")
    print("="*70)
    print("\nEste proceso:")
    print("1. EXTRAE todos los datos del API con paginación automática")
    print("2. TRANSFORMA los datos en métricas y estadísticas útiles")
    print("3. CARGA los datos procesados en la base de datos analítica")
    print("\n" + "="*70)
    
    start_time = time.time()
    
    try:
        # FASE 1: Extract
        raw_data = extract_all_resources()
        
        # FASE 2: Transform
        transformed_data = transform_data(raw_data)
        
        # FASE 3: Load
        load_data(transformed_data)
        
        # Resumen final
        elapsed_time = time.time() - start_time
        print("\n" + "="*70)
        print("✅ ETL COMPLETADO EXITOSAMENTE")
        print("="*70)
        print(f"⏱️  Tiempo total: {elapsed_time:.2f} segundos")
        print("\n📊 Los datos analíticos están listos para:")
        print("   • Dashboards de visualización")
        print("   • Reportes de business intelligence")
        print("   • Análisis de tendencias y patrones")
        print("   • Métricas de uso de la plataforma")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error durante el ETL: {e}")
        raise


if __name__ == "__main__":
    main()

