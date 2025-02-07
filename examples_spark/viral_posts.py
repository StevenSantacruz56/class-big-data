from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg
from pyspark.sql.window import Window

# 1. Inicializar SparkSession
spark = SparkSession.builder \
    .appName("ViralPostsAnalysis") \
    .master("spark://localhost:7077") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()


# 2. Configurar la conexión JDBC a PostgreSQL.
# Dado que trabajamos en el entorno docker-compose, usamos "postgres-db" como hostname.
jdbc_url = "jdbc:postgresql://postgres-db:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "test_view",
    "driver": "org.postgresql.Driver"
}

# 3. Leer las tablas "posts" y "post_likes" desde PostgreSQL usando JDBC.
df_posts = spark.read.jdbc(url=jdbc_url, table="posts", properties=connection_properties)
df_likes = spark.read.jdbc(url=jdbc_url, table="post_likes", properties=connection_properties)

# 4. Convertir la columna "liked_at" a una fecha (sin hora) y agrupar para contar los likes diarios por post.
df_daily_likes = df_likes.withColumn("like_date", to_date(col("liked_at"))) \
    .groupBy("post_id", "like_date") \
    .agg(count("id").alias("daily_likes"))

# 5. Definir una ventana para calcular el promedio móvil de los likes diarios.
# La ventana se particiona por "post_id", se ordena por "like_date" y abarca las 3 filas (días) anteriores (incluyendo la actual).
windowSpec = Window.partitionBy("post_id").orderBy("like_date").rowsBetween(-2, 0)

# 6. Calcular el promedio móvil de los likes diarios para cada post.
df_with_moving_avg = df_daily_likes.withColumn("moving_avg", avg("daily_likes").over(windowSpec))

# 7. Calcular el ratio de spike: (likes diarios) / (promedio móvil)
df_spike = df_with_moving_avg.withColumn("spike_ratio", col("daily_likes") / col("moving_avg"))

# 8. Filtrar aquellos registros donde el spike_ratio supera un umbral (por ejemplo, 2.0).
umbral_spike = 2.0
df_viral = df_spike.filter(col("spike_ratio") > umbral_spike)

# 9. Unir con la tabla "posts" para obtener detalles adicionales de la publicación (contenido, fecha de creación, etc.).
df_viral_posts = df_viral.join(df_posts, df_viral.post_id == df_posts.id, "inner") \
    .select(
    "post_id",
    "like_date",
    "daily_likes",
    "moving_avg",
    "spike_ratio",
    "content",
    "created_at",
    "user_id"
)

# 10. Mostrar los resultados en consola (ordenados por post y fecha)
print("Publicaciones con spike de likes (potencialmente virales):")
df_viral_posts.orderBy("post_id", "like_date").show(truncate=False)

# 11. Escribir el resultado en una nueva tabla en PostgreSQL, por ejemplo "viral_posts".
df_viral_posts.write.jdbc(
    url=jdbc_url,
    table="viral_posts",
    mode="overwrite",
    properties=connection_properties
)
