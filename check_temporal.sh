#!/bin/bash
# Workaround script to list and monitor Temporal workflows

echo "🔍 Temporal Workflow Status Checker"
echo "===================================="
echo ""

echo "📊 Checking Temporal Services..."
if docker compose ps temporal | grep -q "Up"; then
    echo "✅ Temporal server is running"
else
    echo "❌ Temporal server is not running"
    exit 1
fi

echo ""
echo "📋 Recent ETL Worker Activity:"
echo "---"
docker compose logs etl-worker --tail=10 | grep -E "(Worker started|ETL|workflow|activity|✅|📥)" | tail -8 || echo "No recent activity"

echo ""
echo "🌐 Temporal UI URLs:"
echo "   Main UI: http://localhost:8233"
echo "   Workflows (direct): http://localhost:8233/namespaces/default/workflows"
echo ""
echo "💡 If you see the filter error:"
echo "   1. Clear all filters/search boxes"
echo "   2. Select 'default' namespace from dropdown (top right)"
echo "   3. Refresh page (F5)"
echo "   4. Or use direct URL above"
echo ""
echo "🚀 To trigger a new workflow:"
echo "   curl -X POST http://localhost:8000/etl"
echo ""
echo "📝 To monitor workflow execution:"
echo "   docker compose logs etl-worker -f"
echo ""

