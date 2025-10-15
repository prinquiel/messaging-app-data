# 🚀 INICIO RÁPIDO - 5 Pasos

## ✅ PASO 1: Crear archivo .env

Crea un archivo llamado `.env` en la raíz del proyecto con este contenido:

```env
# Base de Datos Operacional (para el API)
DB_USER=chatuser
DB_PASSWORD=chatpassword
DB_NAME=chatdb
DB_HOST=db
DB_PORT=5432

# Base de Datos Analítica (para el ETL)
ANALYTICS_DB_USER=analyticsuser
ANALYTICS_DB_PASSWORD=analyticspassword
ANALYTICS_DB_NAME=analyticsdb
ANALYTICS_DB_HOST=analyticsdb
ANALYTICS_DB_PORT=5432

# API Configuration
API_URL=http://api:8000
```

**Comando rápido (Mac/Linux):**
```bash
cat > .env << 'EOF'
DB_USER=chatuser
DB_PASSWORD=chatpassword
DB_NAME=chatdb
DB_HOST=db
DB_PORT=5432
ANALYTICS_DB_USER=analyticsuser
ANALYTICS_DB_PASSWORD=analyticspassword
ANALYTICS_DB_NAME=analyticsdb
ANALYTICS_DB_HOST=analyticsdb
ANALYTICS_DB_PORT=5432
API_URL=http://api:8000
EOF
```

---

## ✅ PASO 2: Levantar los contenedores

```bash
docker compose up -d --build
```

**Verificar que todo está corriendo:**
```bash
docker compose ps
```

Deberías ver 3 contenedores:
- ✅ chatdb (puerto 5433)
- ✅ analyticsdb (puerto 5434)
- ✅ api (puerto 8000)

**Probar el API:**
```bash
curl http://localhost:8000/health
```

Respuesta esperada: `{"status":"healthy"}`

---

## ✅ PASO 3: Generar datos fake

```bash
cd scripts
pip install -r requirements.txt

export DB_HOST=localhost
export DB_PORT=5433
export DB_USER=chatuser
export DB_PASSWORD=chatpassword
export DB_NAME=chatdb

python generate_fake_data.py
```

⏱️ **Tiempo estimado:** 5-10 minutos

Al finalizar verás:
```
📊 ESTADÍSTICAS DE LA BASE DE DATOS
═══════════════════════════════════════════════════════════
👥 Usuarios totales:        100,000
💬 Chats totales:           50,000
💌 Mensajes totales:        500,000
```

---

## ✅ PASO 4: Verificar que el API funciona

```bash
# Ver documentación interactiva
open http://localhost:8000/docs

# O hacer requests desde terminal
curl "http://localhost:8000/users?page=1&page_size=10" | jq

curl http://localhost:8000/stats
```

---

## ✅ PASO 5: Ejecutar el ETL

```bash
cd ../etl
pip install -r requirements.txt

export API_URL=http://localhost:8000
export ANALYTICS_DB_HOST=localhost
export ANALYTICS_DB_PORT=5434
export ANALYTICS_DB_USER=analyticsuser
export ANALYTICS_DB_PASSWORD=analyticspassword
export ANALYTICS_DB_NAME=analyticsdb

python etl_pipeline.py
```

⏱️ **Tiempo estimado:** 10-15 minutos

El ETL hará:
1. **EXTRACT:** ~2,600 requests al API para obtener todos los datos
2. **TRANSFORM:** Calcular estadísticas y agregaciones
3. **LOAD:** Guardar resultados en base de datos analítica

---

## ✅ ¡COMPLETADO!

Ahora tienes:
- ✅ API funcionando con 100,000 usuarios
- ✅ Base de datos operacional con 500,000 mensajes
- ✅ Base de datos analítica con estadísticas procesadas

### Explorar los datos:

**Base de datos operacional:**
```bash
docker compose exec db psql -U chatuser -d chatdb
```

```sql
-- Ver usuarios
SELECT * FROM users LIMIT 5;

-- Ver mensajes
SELECT * FROM messages LIMIT 5;

-- Contar todo
SELECT 
  (SELECT COUNT(*) FROM users) as total_users,
  (SELECT COUNT(*) FROM chats) as total_chats,
  (SELECT COUNT(*) FROM messages) as total_messages;
```

**Base de datos analítica:**
```bash
docker compose exec analyticsdb psql -U analyticsuser -d analyticsdb
```

```sql
-- Ver estadísticas de usuarios
SELECT * FROM user_statistics ORDER BY total_messages_sent DESC LIMIT 10;

-- Ver actividad diaria
SELECT * FROM daily_message_stats ORDER BY date DESC LIMIT 7;

-- Ver chats más activos
SELECT * FROM chat_statistics ORDER BY total_messages DESC LIMIT 10;
```

---

## 🛑 Detener el proyecto

```bash
# Detener contenedores (mantiene los datos)
docker compose stop

# Eliminar contenedores (mantiene los datos)
docker compose down

# Eliminar TODO (contenedores + datos)
docker compose down -v
```

---

## 🆘 Solución de Problemas

### Error: "Cannot connect to Docker daemon"
**Solución:** Inicia Docker Desktop

### Error: "Port 5433 already in use"
**Solución:** 
```bash
# Ver qué está usando el puerto
lsof -i :5433

# Cambiar el puerto en docker-compose.yml
# Línea 10: "5435:5432" en lugar de "5433:5432"
```

### Error: "permission denied" en scripts
**Solución:**
```bash
chmod +x scripts/setup_env.sh
```

### El script de datos fake es muy lento
**Esto es normal.** Está insertando 650,000 registros. Puedes:
- Reducir las cantidades en `generate_fake_data.py` (líneas 21-23)
- Usar una máquina más potente
- Tomar un café ☕

---

## 📚 Documentación Completa

- `README.md` - Documentación técnica completa
- `EXPLICACION_DETALLADA.md` - Explicación para principiantes
- http://localhost:8000/docs - Documentación interactiva del API

---

**¡Listo para empezar! 🎉**

