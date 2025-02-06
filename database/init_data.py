#!/usr/bin/env python3
import os
import random
import uuid
import psycopg2
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo envs/localhost.env
load_dotenv('../envs/localhost.env')

# Configuración de la base de datos obtenida desde las variables de entorno
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Parámetros de generación
NUM_USERS = 200
MAX_POSTS_PER_USER = 150
MAX_LIKES_PER_POST = 300

# Lista para ir guardando los IDs de usuario generados
user_ids = []

print("Starting\n")

# Conexión a la base de datos
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
except Exception as e:
    print("Error al conectar con la base de datos:", e)
    exit(1)

cursor = conn.cursor()

# Bucle principal para generar usuarios, posts y likes
for i in range(1, NUM_USERS + 1):
    num_posts = random.randint(1, MAX_POSTS_PER_USER)
    print(f"({i}/{NUM_USERS}) Generating user with {num_posts} posts")

    # Generar datos del usuario
    user_id = str(uuid.uuid4())
    user_ids.append(user_id)
    name = f"User_{i}"
    email = f"user_{i}@example.com"
    profile_picture = f"https://example.com/pictures/user_{i}.jpg"
    status = "active"

    # Lista de sentencias SQL a ejecutar en esta transacción
    stmts = []
    stmts.append(
        f"INSERT INTO users (id, name, email, profile_picture, status) VALUES ('{user_id}', '{name}', '{email}', '{profile_picture}', '{status}')"
    )

    # Generar posts para el usuario
    for j in range(1, num_posts + 1):
        num_likes = random.randint(1, MAX_LIKES_PER_POST)
        print(f"  → Generating post n° {j} with {num_likes} likes")
        post_id = str(uuid.uuid4())
        content = f"This is post {j} of user {i}"
        created_at = "2025-03-04 15:30:45"  # Fecha fija según el script original
        stmts.append(
            f"INSERT INTO posts (id, user_id, content, created_at) VALUES ('{post_id}', '{user_id}', '{content}', '{created_at}')"
        )

        # Generar likes para el post
        for k in range(1, num_likes + 1):
            like_id = str(uuid.uuid4())
            liked_at = "2025-03-05 15:30:45"
            # Elegir aleatoriamente un usuario de la lista (siempre habrá al menos uno)
            liker_id = random.choice(user_ids)
            stmts.append(
                f"INSERT INTO post_likes (id, post_id, user_id, liked_at) VALUES ('{like_id}', '{post_id}', '{liker_id}', '{liked_at}')"
            )

    # Ejecutar todas las sentencias en una transacción
    try:
        cursor.execute("BEGIN;")
        for stmt in stmts:
            cursor.execute(stmt)
        cursor.execute("COMMIT;")
    except Exception as e:
        print("Error en la transacción. Realizando rollback:", e)
        conn.rollback()

# Cerrar cursor y conexión
cursor.close()
conn.close()

print("Dooone!")
