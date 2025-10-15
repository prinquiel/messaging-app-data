import requests
import psycopg2
from psycopg2.extras import execute_batch
import os
from datetime import datetime
from collections import defaultdict
import time


API_BASE_URL = os.getenv('API_URL', 'http://localhost:8000')
MAX_PAGE_SIZE = 250  # Máximo permitido por el API

ANALYTICS_DB_CONFIG = {
    'host': os.getenv('ANALYTICS_DB_HOST', 'localhost'),
    'port': os.getenv('ANALYTICS_DB_PORT', '5434'),
    'database': os.getenv('ANALYTICS_DB_NAME', 'analyticsdb'),
    'user': os.getenv('ANALYTICS_DB_USER', 'analyticsuser'),
    'password': os.getenv('ANALYTICS_DB_PASSWORD', 'analyticspassword')
}


# ==================== EXTRACT ====================

def extract_all_data(endpoint, resource_name):
    """
    Extrae TODOS los datos de un endpoint con paginación automática.
    
    Esta función hace requests consecutivos al API hasta obtener todos los datos.
    Si hay 10,000 registros y el máximo por página es 250, hará 40 requests.
    
    Args:
        endpoint: URL del endpoint (ej: '/users')
        resource_name: Nombre del recurso para logs (ej: 'usuarios')
    
    Returns:
        Lista con todos los items extraídos
    """
    print(f"\n📥 EXTRAYENDO {resource_name.upper()}...")
    
    all_items = []
    page = 1
    
    while True:
        url = f"{API_BASE_URL}{endpoint}"
        params = {
            'page': page,
            'page_size': MAX_PAGE_SIZE
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            items = data.get('items', [])
            total = data.get('total', 0)
            total_pages = data.get('total_pages', 0)
            
            all_items.extend(items)
            
            print(f"   Página {page}/{total_pages}: {len(items)} {resource_name} "
                  f"(Total acumulado: {len(all_items):,}/{total:,})")
            
            if page >= total_pages or len(items) == 0:
                break
            
            page += 1
            time.sleep(0.1)  #
            
        except Exception as e:
            print(f"   ❌ Error extrayendo {resource_name} en página {page}: {e}")
            break
    
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
    
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
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
        'messages': extract_all_data('/messages', 'mensajes')
    }
    
    print("\n📥 EXTRAYENDO MENSAJES POR CHAT...")
    chat_messages = {}
    for i, chat in enumerate(data['chats'][:100], 1):  # Solo primeros 100 chats para ejemplo
        chat_id = chat['id']
        messages = extract_all_data(f'/chats/{chat_id}/messages', f'mensajes del chat {chat_id}')
        chat_messages[chat_id] = messages
        if i % 20 == 0:
            print(f"   Progreso: {i}/100 chats procesados")
    
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
    
    print("\n📊 Calculando estadísticas por usuario...")
    user_stats = {}
    
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
            
            sent_at = message['sent_at']
            if (user_stats[sender_id]['last_message_date'] is None or 
                sent_at > user_stats[sender_id]['last_message_date']):
                user_stats[sender_id]['last_message_date'] = sent_at
    
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
    
    chat_type_map = {chat['id']: chat['chat_type'] for chat in chats}
    
    for message in messages:
        date_str = message['sent_at'][:10]  # YYYY-MM-DD
        
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
    
    return {
        'user_statistics': list(user_stats.values()),
        'chat_statistics': list(chat_stats.values()),
        'daily_message_stats': daily_stats_list,
        'message_type_summary': message_type_list
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
    
    try:
        conn = psycopg2.connect(**ANALYTICS_DB_CONFIG)
        print(f"✅ Conectado a base de datos analítica: {ANALYTICS_DB_CONFIG['database']}")
    except Exception as e:
        print(f"❌ Error conectando a base de datos analítica: {e}")
        raise
    
    create_analytics_tables(conn)
    
    cursor = conn.cursor()
    
    print("\n📥 Cargando estadísticas de usuarios...")
    cursor.execute("DELETE FROM user_statistics")  # Limpiar datos antiguos
    
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
    
    execute_batch(cursor, """
        INSERT INTO user_statistics 
        (user_id, username, total_messages_sent, chats_participated, 
         last_message_date, is_active, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, user_data, page_size=1000)
    
    print(f"   ✅ {len(user_data):,} registros de usuarios cargados")
    
    print("\n📥 Cargando estadísticas de chats...")
    cursor.execute("DELETE FROM chat_statistics")
    
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
    
    execute_batch(cursor, """
        INSERT INTO chat_statistics 
        (chat_id, chat_name, chat_type, total_messages, unique_senders,
         first_message_date, last_message_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, chat_data, page_size=1000)
    
    print(f"   ✅ {len(chat_data):,} registros de chats cargados")
    
    print("\n📥 Cargando métricas diarias...")
    cursor.execute("DELETE FROM daily_message_stats")
    
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
    
    execute_batch(cursor, """
        INSERT INTO daily_message_stats 
        (date, total_messages, unique_users, unique_chats, 
         private_messages, group_messages)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, daily_data, page_size=1000)
    
    print(f"   ✅ {len(daily_data):,} registros diarios cargados")
    
    print("\n📥 Cargando resumen de tipos de mensaje...")
    cursor.execute("DELETE FROM message_type_summary")
    
    type_data = [
        (stat['message_type'], stat['total_count'])
        for stat in transformed_data['message_type_summary']
    ]
    
    execute_batch(cursor, """
        INSERT INTO message_type_summary (message_type, total_count)
        VALUES (%s, %s)
    """, type_data, page_size=100)
    
    print(f"   ✅ {len(type_data)} tipos de mensaje cargados")
    
    conn.commit()
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

