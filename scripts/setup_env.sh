

echo "🔧 Configurando variables de entorno para scripts..."

# Para el script de generación de datos fake
export DB_HOST=localhost
export DB_PORT=5433
export DB_USER=chatuser
export DB_PASSWORD=chatpassword
export DB_NAME=chatdb

# Para el ETL
export API_URL=http://localhost:8000
export ANALYTICS_DB_HOST=localhost
export ANALYTICS_DB_PORT=5434
export ANALYTICS_DB_USER=analyticsuser
export ANALYTICS_DB_PASSWORD=analyticspassword
export ANALYTICS_DB_NAME=analyticsdb

echo "✅ Variables de entorno configuradas"
echo ""
echo "Ahora puedes ejecutar:"
echo "  python generate_fake_data.py"
echo "  o"
echo "  cd ../etl && python etl_pipeline.py"

