#!/bin/bash
# Test script to trigger ETL workflow

echo "🚀 Triggering ETL workflow..."
response=$(curl -s -X POST http://localhost:8000/etl -H "Content-Type: application/json")
workflow_id=$(echo $response | python3 -c "import sys, json; print(json.load(sys.stdin)['workflow_id'])")

echo "✅ Workflow triggered!"
echo "   Workflow ID: $workflow_id"
echo ""
echo "📊 Monitor in Temporal UI:"
echo "   http://localhost:8233"
echo ""
echo "📝 View logs:"
echo "   docker compose logs etl-worker --tail=50 -f"

