#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pathlib import Path

# --- Obtener variables de conexión desde el archivo de entorno ---
DB_HOST = os.getenv("DB_HOST", "postgres-db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "test_view")

# --- Construir la URL JDBC ---
jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"Conectando a PostgreSQL en {DB_HOST}:{DB_PORT} / {DB_NAME}")

# --- Definir las propiedades de conexión ---
connection_properties = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

# --- Crear la SparkSession ---
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ReadPostgresView") \
    .master("spark://localhost:7077") \
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

# --- Leer datos de la vista "posts_view" ---
# La vista "post_likes" debe existir en la base de datos y tener la estructura definida.
df = spark.read.jdbc(url=jdbc_url, table="post_likes", properties=connection_properties)

# --- Mostrar algunas filas y el esquema ---
print("Mostrando las primeras 10 filas de post_likes:")
df.show(10)

print("Esquema del DataFrame:")
df.printSchema()

# --- Finalizar la SparkSession ---
spark.stop()
