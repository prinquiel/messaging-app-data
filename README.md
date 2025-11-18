# bash /Users/prindiquiel/Library/CloudStorage/OneDrive-UNADECA/coding-projects/messaging-app-data/trigger_workflow.sh

# 💬 Messaging App 

#  CÓMO PROBAR EL PROYECTO

##  OPCIÓN RÁPIDA (Script Automatizado)

Ejecuta desde la raíz del proyecto:

```bash
bash test_proyecto.sh
```

Este script te guiará paso a paso y te preguntará qué quieres hacer.


##  Arquitectura

```
┌─────────────────┐
│   Frontend      │  (No incluido - consumiría el API)
└────────┬────────┘
         │
         ↓
┌─────────────────────────────────────────────┐
│              API (FastAPI)                  │
│  • GET /users (paginado)                    │
│  • GET /chats (paginado)                    │
│  • GET /messages (paginado)                 │
│  • POST endpoints para crear datos          │
└────────┬────────────────────────────────────┘
         │
         ↓
┌──────────────────────┐
│  DB Operacional      │
│  (PostgreSQL)        │
│  • users             │
│  • chats             │
│  • messages          │
│  • chat_members      │
└──────────────────────┘

         ┌────────────┐
         │ Script     │
         │ Faker      │──► Genera 100k+ registros
         └────────────┘

┌─────────────────────────────────────────────┐
│           ETL Pipeline                      │
│  1. EXTRACT: Consume API con paginación     │
│  2. TRANSFORM: Agrega datos (stats, etc.)   │
│  3. LOAD: Carga en DB Analítica             │
└────────┬────────────────────────────────────┘
         │
         ↓
┌──────────────────────────┐
│  DB Analítica            │
│  (PostgreSQL)            │
│  • user_statistics       │
│  • chat_statistics       │
│  • daily_message_stats   │
│  • message_type_summary  │
└──────────────────────────┘
```

---

## 🔧 Requisitos

- **Docker** y **Docker Compose**
- **Python 3.11+** (para correr scripts fuera de Docker)


---

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
cd messaging-app-data```````
```

### 2. Crear archivo `.env`

Crea un archivo `.env` en la raíz del proyecto con el siguiente contenido:

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


### 3. Migraciones (Alembic)

Usamos Alembic para mantener sincronizada la estructura de la base de datos.

1. Entra a la carpeta del API:
   ```bash
   cd api
   ```
2. Instala dependencias (si aún no lo hiciste):
   ```bash
   pip install -r requirements.txt
   ```
3. Exporta las variables de conexión (DB_USER, DB_PASSWORD, etc.) y corre:
   ```bash
   alembic upgrade head
   ```

Esto crea la tabla `alembic_version` y agrega `password_hash` a `users` solo si falta.  
Cada vez que se agregue una nueva migración basta con volver a ejecutar `alembic upgrade head`.


## 📖 Uso Paso a Paso

### PASO 1: Levantar los Contenedores

```bash
docker compose up -d --build
```

Esto iniciará:
- ✅ Base de datos operacional (puerto **5433**)
- ✅ Base de datos analítica (puerto **5434**)
- ✅ API (puerto **8000**)

**Verificar que todo está corriendo:**

```bash
docker compose ps
```

**Probar el API:**

```bash
curl http://localhost:8000/health
# Respuesta: {"status":"healthy"}
```

**Ver la documentación del API:**

Abre en tu navegador: http://localhost:8000/docs

---

### PASO 2: Generar Datos Fake

Ahora vamos a llenar la base de datos con **100,000 usuarios**, **50,000 chats** y **500,000 mensajes**.

#### Desde máquina

```bash
# Instalar dependencias
cd scripts
pip install -r requirements.txt

# Configurar variables de entorno
export DB_HOST=localhost
export DB_PORT=5433
export DB_USER=chatuser
export DB_PASSWORD=chatpassword
export DB_NAME=chatdb

# Ejecutar el script (toma ~5-10 minutos)
python generate_fake_data.py


**Resultado esperado:**

```
📊 ESTADÍSTICAS DE LA BASE DE DATOS
═══════════════════════════════════════════════════════════
👥 Usuarios totales:        100,000
   └─ Activos:              95,000

💬 Chats totales:           50,000
   ├─ Privados:             35,000
   └─ Grupales:             15,000

💌 Mensajes totales:        500,000
   └─ Activos (no borrados): 490,000
═══════════════════════════════════════════════════════════


### PASO 3: Ejecutar el ETL

El ETL extraerá todos los datos del API y los transformará en datos analíticos.

#### Desde propia máquina:

```bash
cd etl
pip install -r requirements.txt

# Configurar variables
export API_URL=http://localhost:8000
export ANALYTICS_DB_HOST=localhost
export ANALYTICS_DB_PORT=5434
export ANALYTICS_DB_USER=analyticsuser
export ANALYTICS_DB_PASSWORD=analyticspassword
export ANALYTICS_DB_NAME=analyticsdb

# Ejecutar ETL (toma ~10-15 minutos para 500k mensajes)
python etl_pipeline.py
```

**El ETL hará:**

1. **EXTRACT**: Extraerá todos los datos del API
   - Usuarios: 100,000 registros (400 requests de 250 items c/u)
   - Chats: 50,000 registros (200 requests)
   - Mensajes: 500,000 registros (2,000 requests)

2. **TRANSFORM**: Calculará estadísticas
   - Mensajes por usuario
   - Actividad por chat
   - Métricas diarias
   - Tipos de mensajes

3. **LOAD**: Cargará en la DB analítica
   - `user_statistics`: 100,000 registros
   - `chat_statistics`: 50,000 registros
   - `daily_message_stats`: ~730 registros (2 años de datos)
   - `message_type_summary`: ~5 registros

---

## 📁 Estructura del Proyecto

```
messaging-app-data/
├── api/                          # Backend (FastAPI)
│   ├── app/
│   │   ├── main.py              # Endpoints del API
│   │   ├── models.py            # Modelos SQLAlchemy
│   │   ├── schemas.py           # Schemas Pydantic
│   │   ├── database.py          # Configuración DB
│   │   └── routers/             # (Para futuras rutas)
│   ├── Dockerfile
│   └── requirements.txt
│
├── scripts/                      # Scripts de utilidad
│   ├── generate_fake_data.py    # Genera datos con Faker
│   └── requirements.txt
│
├── etl/                          # Pipeline ETL
│   ├── etl_pipeline.py          # ETL completo
│   └── requirements.txt
│
├── docker-compose.yml            # Orquestación de contenedores
├── .env                          # Variables de entorno (crear)
└── README.md                     # Este archivo
```

---

## 🔌 API Endpoints

### Endpoints Principales (todos con paginación)

| Método | Endpoint | Descripción | Paginación |
|--------|----------|-------------|------------|
| `GET` | `/users` | Lista todos los usuarios | ✅ Max 250/página |
| `GET` | `/users/{id}` | Obtiene un usuario | ❌ |
| `GET` | `/chats` | Lista todos los chats | ✅ Max 250/página |
| `GET` | `/chats/{id}` | Obtiene un chat con miembros | ❌ |
| `GET` | `/chats/{id}/messages` | Mensajes de un chat | ✅ Max 250/página |
| `GET` | `/messages` | Lista todos los mensajes | ✅ Max 250/página |
| `GET` | `/messages/{id}` | Obtiene un mensaje | ❌ |
| `POST` | `/users` | Crea un usuario | ❌ |
| `POST` | `/chats` | Crea un chat | ❌ |
| `POST` | `/messages` | Crea un mensaje | ❌ |
| `GET` | `/stats` | Estadísticas generales | ❌ |

### Ejemplos de uso:

**Obtener usuarios (página 1, 50 items):**
```bash
curl "http://localhost:8000/users?page=1&page_size=50"
```

**Obtener usuarios (página 1, máximo 250 items):**
```bash
curl "http://localhost:8000/users?page=1&page_size=250"
```

**Respuesta (formato JSON):**
```json
{
  "items": [
    {
      "id": 1,
      "username": "john_doe123",
      "email": "john@example.com",
      "full_name": "John Doe",
      "is_active": true,
      "created_at": "2023-05-15T10:30:00"
    },
    ...
  ],
  "total": 100000,
  "page": 1,
  "page_size": 50,
  "total_pages": 2000
}
```

**Obtener mensajes de un chat específico:**
```bash
curl "http://localhost:8000/chats/123/messages?page=1&page_size=100"
```


## 🔍 Consultas Útiles

### En la Base de Datos Operacional:

```bash
# Conectar
docker compose exec db psql -U chatuser -d chatdb

# Top 10 usuarios más activos
SELECT u.username, COUNT(m.id) as total_messages
FROM users u
JOIN messages m ON u.id = m.sender_id
GROUP BY u.id, u.username
ORDER BY total_messages DESC
LIMIT 10;
```

### En la Base de Datos Analítica:

```bash
# Conectar
docker compose exec analyticsdb psql -U analyticsuser -d analyticsdb

# Ver todas las estadísticas de usuarios
SELECT * FROM user_statistics ORDER BY total_messages_sent DESC LIMIT 10;

# Actividad por día
SELECT * FROM daily_message_stats ORDER BY date DESC;

# Chats más activos
SELECT * FROM chat_statistics ORDER BY total_messages DESC LIMIT 10;
```

---

## 🧪 Testing

**Verificar que el API funciona:**
```bash
# Salud del API
curl http://localhost:8000/health

# Estadísticas generales
curl http://localhost:8000/stats

# Primeros 10 usuarios
curl "http://localhost:8000/users?page=1&page_size=10"
```





