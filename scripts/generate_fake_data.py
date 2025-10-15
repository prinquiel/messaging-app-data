"""
Script para generar datos fake usando Faker
Genera usuarios, chats y mensajes para simular una aplicación de mensajería real

Uso:
    python generate_fake_data.py
"""

from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
import os
import random
from datetime import datetime, timedelta

fake = Faker()

def truncate(value: str, max_len: int) -> str:
    if value is None:
        return None
    text = str(value)
    return text[:max_len]

def sanitize_username(username: str) -> str:
    safe = username.replace(" ", "_")
    return truncate(safe, 50)

def sanitize_email(email: str) -> str:
    if email is None:
        return None
    email = str(email)
    if "@" not in email:
        return truncate(email, 100)
    local, domain = email.split("@", 1)
    max_local_len = max(1, 100 - 1 - len(domain))
    local = truncate(local, max_local_len)
    return f"{local}@{domain}"

def sanitize_full_name(name: str) -> str:
    return truncate(name, 100)

def sanitize_phone(phone: str) -> str:
    if phone is None:
        return None
    phone = str(phone)
    has_plus = phone.strip().startswith("+")
    digits = "".join(ch for ch in phone if ch.isdigit())
    if has_plus:
        digits = "+" + digits
    return truncate(digits, 20) if digits else None

# Configuración de conexión a la base de datos
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5433'),
    'database': os.getenv('DB_NAME', 'chatdb'),
    'user': os.getenv('DB_USER', 'chatuser'),
    'password': os.getenv('DB_PASSWORD', 'chatpassword')
}

NUM_USERS = int(os.getenv('NUM_USERS', '100000'))  # 100,000 usuarios
NUM_CHATS = int(os.getenv('NUM_CHATS', '50000'))   # 50,000 chats (mezcla de privados y grupales)
NUM_MESSAGES = int(os.getenv('NUM_MESSAGES', '500000'))  # 500,000 mensajes

MESSAGE_TEMPLATES = [
    "Hola! ¿Cómo estás?",
    "¿Nos vemos mañana?",
    "Gracias por tu ayuda 😊",
    "¿Ya viste el último episodio?",
    "Perfecto, nos vemos entonces",
    "jajaja no lo puedo creer",
    "Dale, confirmado",
    "Buen día! ☀️",
    "¿Qué planes tienes para el fin de semana?",
    "Ok, entendido",
    "Genial! Me parece bien",
    "No te preocupes",
    "Feliz cumpleaños! 🎉",
    "Estoy en camino",
    "¿Recibiste mi mensaje anterior?",
]


def connect_db():
    """Conectar a la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✅ Conectado a la base de datos: {DB_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        raise


def generate_users(conn, num_users):
    """Generar usuarios fake"""
    print(f"\n📝 Generando {num_users:,} usuarios...")
    
    cursor = conn.cursor()
    users = []
    seen_usernames = set()
    seen_emails = set()
    
    for i in range(num_users):
        raw_username = fake.user_name() + str(random.randint(1, 9999))
        raw_email = fake.email()
        raw_full_name = fake.name()
        raw_phone = fake.phone_number()

        # Username único dentro del batch
        base_username = sanitize_username(raw_username)
        username = base_username
        suffix = 1
        while not username or username in seen_usernames:
            username = sanitize_username(f"{base_username}_{suffix}")
            suffix += 1
        seen_usernames.add(username)

        # Email único derivado del username para minimizar colisiones
        email_candidate = f"{username}@example.com"
        email = sanitize_email(email_candidate)
        suffix = 1
        while not email or email in seen_emails:
            email = sanitize_email(f"{username}{suffix}@example.com")
            suffix += 1
        seen_emails.add(email)
        full_name = sanitize_full_name(raw_full_name)
        phone_number = sanitize_phone(raw_phone)
        bio = fake.text(max_nb_chars=200) if random.random() > 0.7 else None
        avatar_url = f"https://i.pravatar.cc/150?img={random.randint(1, 70)}"
        is_active = random.random() > 0.05  # 95% usuarios activos
        
        created_at = fake.date_time_between(start_date='-2y', end_date='now')
        
        users.append((
            username, email, full_name, phone_number, bio,
            truncate(avatar_url, 500), is_active, created_at
        ))
        
        if (i + 1) % 10000 == 0:
            print(f"   Progreso: {i + 1:,}/{num_users:,} usuarios generados")
    
    print("   Insertando en la base de datos...")
    execute_batch(cursor, """
        INSERT INTO users (username, email, full_name, phone_number, bio, 
                          avatar_url, is_active, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, users, page_size=1000)
    
    conn.commit()
    cursor.close()
    print(f"✅ {num_users:,} usuarios creados exitosamente")


def generate_chats(conn, num_chats):
    """Generar chats (privados y grupales)"""
    print(f"\n💬 Generando {num_chats:,} chats...")
    
    cursor = conn.cursor()
    
    # Obtener IDs de usuarios existentes
    cursor.execute("SELECT id FROM users ORDER BY id")
    user_ids = [row[0] for row in cursor.fetchall()]
    print(f"   Usuarios disponibles: {len(user_ids):,}")
    
    chats = []
    chat_members_data = []
    
    for i in range(num_chats):
        is_group = random.random() > 0.7
        
        if is_group:
            chat_type = 'group'
            name = fake.catch_phrase()
            description = fake.text(max_nb_chars=150)
            num_members = random.randint(3, 50)
        else:
            chat_type = 'private'
            name = None
            description = None
            num_members = 2
        
        avatar_url = f"https://picsum.photos/200?random={random.randint(1, 10000)}"
        created_at = fake.date_time_between(start_date='-1y', end_date='now')
        
        members = random.sample(user_ids, num_members)
        created_by = members[0]
        
        chats.append((
            name, chat_type, description, avatar_url, created_at, created_by
        ))
        
        chat_members_data.append((members, created_at))
        
        if (i + 1) % 5000 == 0:
            print(f"   Progreso: {i + 1:,}/{num_chats:,} chats generados")
    
    # Insertar chats
    print("   Insertando chats en la base de datos...")

    try:
        cursor.execute("SELECT setval('chats_id_seq', 1, false)")
    except Exception:
        pass
    execute_batch(cursor, """
        INSERT INTO chats (name, chat_type, description, avatar_url, created_at, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, chats, page_size=1000)
    
    conn.commit()
    
    # Obtener IDs de chats creados
    cursor.execute("SELECT id FROM chats ORDER BY id")
    chat_ids = [row[0] for row in cursor.fetchall()]
    
    # Insertar miembros de chats
    print("   Insertando miembros de chats...")
    members_insert = []
    for chat_id, (members, created_at) in zip(chat_ids, chat_members_data):
        for user_id in members:
            members_insert.append((user_id, chat_id, created_at))
    
    execute_batch(cursor, """
        INSERT INTO chat_members (user_id, chat_id, joined_at)
        VALUES (%s, %s, %s)
    """, members_insert, page_size=1000)
    
    conn.commit()
    cursor.close()
    print(f"✅ {num_chats:,} chats creados exitosamente")


def generate_messages(conn, num_messages):
    """Generar mensajes"""
    print(f"\n💌 Generando {num_messages:,} mensajes...")
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, created_at FROM users WHERE is_active = true")
    active_users = cursor.fetchall()
    
    cursor.execute("SELECT id, created_at FROM chats")
    chats = cursor.fetchall()
    
    cursor.execute("SELECT chat_id, user_id FROM chat_members")
    chat_members_map = {}
    for chat_id, user_id in cursor.fetchall():
        if chat_id not in chat_members_map:
            chat_members_map[chat_id] = []
        chat_members_map[chat_id].append(user_id)
    
    print(f"   Usuarios activos: {len(active_users):,}")
    print(f"   Chats disponibles: {len(chats):,}")
    
    messages = []
    
    for i in range(num_messages):
        # Seleccionar un chat aleatorio
        chat_id, chat_created_at = random.choice(chats)
        
        # Seleccionar un remitente que sea miembro del chat
        if chat_id in chat_members_map:
            sender_id = random.choice(chat_members_map[chat_id])
        else:
            continue 
        
        # Generar contenido del mensaje
        if random.random() > 0.3:
            content = random.choice(MESSAGE_TEMPLATES)
        else:
            content = fake.text(max_nb_chars=random.randint(20, 300))
        
        message_types = ['text'] * 9 + ['image', 'video', 'file']
        message_type = random.choice(message_types)
        
        sent_at = fake.date_time_between(
            start_date=chat_created_at if chat_created_at else '-1y',
            end_date='now'
        )
        
        is_deleted = random.random() < 0.02  
        
        messages.append((
            content, sender_id, chat_id, sent_at, is_deleted, message_type
        ))
        
        if (i + 1) % 50000 == 0:
            print(f"   Progreso: {i + 1:,}/{num_messages:,} mensajes generados")
    
    # Insertar mensajes
    print("   Insertando mensajes en la base de datos...")
    execute_batch(cursor, """
        INSERT INTO messages (content, sender_id, chat_id, sent_at, is_deleted, message_type)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, messages, page_size=1000)
    
    conn.commit()
    cursor.close()
    print(f"✅ {num_messages:,} mensajes creados exitosamente")


def show_stats(conn):
    """Mostrar estadísticas de la base de datos"""
    print("\n" + "="*60)
    print("📊 ESTADÍSTICAS DE LA BASE DE DATOS")
    print("="*60)
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = true")
    active_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM chats WHERE chat_type = 'private'")
    private_chats = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM chats WHERE chat_type = 'group'")
    group_chats = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM messages WHERE is_deleted = false")
    active_messages = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM messages")
    total_messages = cursor.fetchone()[0]
    
    print(f"👥 Usuarios totales:        {total_users:,}")
    print(f"   └─ Activos:              {active_users:,}")
    print(f"\n💬 Chats totales:           {private_chats + group_chats:,}")
    print(f"   ├─ Privados:             {private_chats:,}")
    print(f"   └─ Grupales:             {group_chats:,}")
    print(f"\n💌 Mensajes totales:        {total_messages:,}")
    print(f"   └─ Activos (no borrados): {active_messages:,}")
    print("="*60)
    
    cursor.close()


def main():
    """Función principal"""
    print("\n" + "="*60)
    print("🚀 GENERADOR DE DATOS FAKE - MESSAGING APP")
    print("="*60)
    
    try:
        # Conectar a la base de datos
        conn = connect_db()
        
        # Generar datos
        generate_users(conn, NUM_USERS)
        generate_chats(conn, NUM_CHATS)
        generate_messages(conn, NUM_MESSAGES)
        
        # Mostrar estadísticas
        show_stats(conn)
        
        print("\n✅ ¡Proceso completado exitosamente!\n")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error durante la generación: {e}")
        raise


if __name__ == "__main__":
    main()

