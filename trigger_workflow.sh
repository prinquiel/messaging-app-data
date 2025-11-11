#!/bin/bash
# Trigger Temporal ETL Workflow
# This script triggers a workflow from the worker container

echo "🚀 Triggering ETL Workflow via Temporal..."
echo ""

# Copy trigger script to container
cat > /tmp/trigger_wf.py << 'PYEOF'
import asyncio
from temporalio.client import Client
from workers.etl.worker import ETLWorkflow
import uuid

async def main():
    client = await Client.connect("temporal:7233")
    workflow_id = f"etl-{uuid.uuid4()}"
    handle = await client.start_workflow(
        ETLWorkflow,
        id=workflow_id,
        task_queue="etl-task-queue",
    )
    print(f"✅ Workflow started: {handle.id}")
    print(f"")
    print(f"📊 View in Temporal UI:")
    print(f"   http://localhost:8233/namespaces/default/workflows/{handle.id}")
    print(f"")
    print(f"📝 Monitor logs:")
    print(f"   docker compose logs etl-worker -f")

asyncio.run(main())
PYEOF

# Copy to container and run
docker compose cp /tmp/trigger_wf.py etl-worker:/worker/trigger_wf.py
docker compose exec etl-worker python trigger_wf.py

