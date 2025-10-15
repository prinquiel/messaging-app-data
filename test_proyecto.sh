
# Script para probar el proyecto completo de Messaging App
# Ejecuta: bash test_proyecto.sh

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     🚀 PROBANDO PROYECTO MESSAGING APP - ETL                 ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colores
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' 

# Función para pausar y esperar confirmación
pause() {
    echo ""
    echo -e "${YELLOW}Presiona ENTER para continuar...${NC}"
    read -r
}

# PASO 1: Verificar Docker
echo -e "${BLUE}[PASO 1/6]${NC} Verificando Docker..."
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker no está corriendo. Por favor inicia Docker Desktop.${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker está corriendo${NC}"
pause

# PASO 2: Levantar contenedores
echo -e "${BLUE}[PASO 2/6]${NC} Levantando contenedores de Docker..."
echo "Esto puede tomar unos minutos la primera vez..."
docker compose up -d --build

echo ""
echo "Esperando que los servicios estén listos (30 segundos)..."
sleep 30

# Verificar que los contenedores estén corriendo
echo ""
echo "Estado de los contenedores:"
docker compose ps
pause

# PASO 3: Verificar que el API funciona
echo -e "${BLUE}[PASO 3/6]${NC} Verificando que el API funciona..."
echo ""
echo "Probando endpoint de salud..."
HEALTH_RESPONSE=$(curl -s http://localhost:8000/health)
echo "Respuesta: $HEALTH_RESPONSE"

if [[ $HEALTH_RESPONSE == *"healthy"* ]]; then
    echo -e "${GREEN}✅ API está funcionando correctamente${NC}"
else
    echo -e "${RED}❌ API no responde correctamente${NC}"
    echo "Revisa los logs con: docker compose logs api"
    exit 1
fi

echo ""
echo "Puedes ver la documentación interactiva del API en:"
echo -e "${GREEN}👉 http://localhost:8000/docs${NC}"
pause

# PASO 4: Generar datos fake
echo -e "${BLUE}[PASO 4/6]${NC} ¿Quieres generar datos fake? (100k usuarios, 500k mensajes)"
echo -e "${YELLOW}Escribe 'si' para generar datos, o ENTER para saltar:${NC}"
read -r GENERATE_DATA

if [[ $GENERATE_DATA == "si" ]]; then
    echo ""
    echo "Instalando dependencias de Python..."
    cd scripts
    python3 -m pip install -q -r requirements.txt
    
    echo ""
    echo "Generando datos fake... "
    export DB_HOST=localhost
    export DB_PORT=5433
    export DB_USER=chatuser
    export DB_PASSWORD=chatpassword
    export DB_NAME=chatdb
    
    python3 generate_fake_data.py
    
    cd ..
    echo -e "${GREEN}✅ Datos generados exitosamente${NC}"
else
    echo -e "${YELLOW}⏭️  Saltando generación de datos${NC}"
    echo "Puedes generarlos después con:"
    echo "  cd scripts && python3 generate_fake_data.py"
fi
pause

# PASO 5: Probar el API con datos
echo -e "${BLUE}[PASO 5/6]${NC} Probando endpoints del API..."
echo ""

echo "📊 Estadísticas generales:"
curl -s http://localhost:8000/stats | python3 -m json.tool 2>/dev/null || curl -s http://localhost:8000/stats

echo ""
echo ""
echo "👥 Primeros 5 usuarios:"
curl -s "http://localhost:8000/users?page=1&page_size=5" | python3 -m json.tool 2>/dev/null || curl -s "http://localhost:8000/users?page=1&page_size=5"

pause

# PASO 6: Ejecutar ETL
echo -e "${BLUE}[PASO 6/6]${NC} ¿Quieres ejecutar el ETL?"
echo "Esto tomará ~10-15 minutos si generaste los datos completos"
echo -e "${YELLOW}Escribe 'si' para ejecutar ETL, o ENTER para saltar:${NC}"
read -r RUN_ETL

if [[ $RUN_ETL == "si" ]]; then
    echo ""
    echo "Instalando dependencias del ETL..."
    cd etl
    python3 -m pip install -q -r requirements.txt
    
    echo ""
    echo "Ejecutando ETL... "
    export API_URL=http://localhost:8000
    export ANALYTICS_DB_HOST=localhost
    export ANALYTICS_DB_PORT=5434
    export ANALYTICS_DB_USER=analyticsuser
    export ANALYTICS_DB_PASSWORD=analyticspassword
    export ANALYTICS_DB_NAME=analyticsdb
    
    python3 etl_pipeline.py
    
    cd ..
    echo -e "${GREEN}✅ ETL completado exitosamente${NC}"
else
    echo -e "${YELLOW}⏭️  Saltando ETL${NC}"
    echo "Puedes ejecutarlo después con:"
    echo "  cd etl && python3 etl_pipeline.py"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              ✅ PRUEBA DEL PROYECTO COMPLETADA                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📚 Recursos disponibles:"
echo ""
echo "  🌐 Documentación API:    http://localhost:8000/docs"
echo "  📊 Estadísticas:         http://localhost:8000/stats"
echo ""

