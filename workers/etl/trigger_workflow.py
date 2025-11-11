#!/usr/bin/env python3
"""
Trigger Temporal ETL workflow from the worker container.
Run this inside the etl-worker container.
"""

import asyncio
import os
import sys
from temporalio.client import Client

async def trigger_etl_workflow():
    """Trigger ETL workflow via Temporal client"""
    address = os.getenv("TEMPORAL_ADDRESS", "temporal:7233")
    
    print(f"🔌 Connecting to Temporal at {address}...")
    try:
        client = await Client.connect(address)
        print("✅ Connected to Temporal")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return None
    
    print("📥 Importing ETLWorkflow...")
    try:
        from workers.etl.worker import ETLWorkflow
    except ImportError as e:
        print(f"❌ Failed to import ETLWorkflow: {e}")
        return None
    
    import uuid
    workflow_id = f"etl-{uuid.uuid4()}"
    
    print(f"🚀 Starting workflow: {workflow_id}")
    try:
        handle = await client.start_workflow(
            ETLWorkflow,
            id=workflow_id,
            task_queue="etl-task-queue",
        )
        print(f"✅ Workflow started successfully!")
        print(f"   Workflow ID: {handle.id}")
        print(f"   Run ID: {handle.result_run_id}")
        print(f"\n📊 View in Temporal UI:")
        print(f"   http://localhost:8233/namespaces/default/workflows/{handle.id}")
        return handle.id
    except Exception as e:
        print(f"❌ Failed to start workflow: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    workflow_id = asyncio.run(trigger_etl_workflow())
    if workflow_id:
        print(f"\n✅ Workflow triggered: {workflow_id}")
        sys.exit(0)
    else:
        print("\n❌ Failed to trigger workflow")
        sys.exit(1)

