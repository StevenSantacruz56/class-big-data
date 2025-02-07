from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_sub, current_timestamp, desc

# 1. Inicializar SparkSession con el master del entorno Docker y configurando el driver JDBC para PostgreSQL.
spark = SparkSession.builder \
    .appName("AnalisisPostsLikes") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.5.jar") \
    .getOrCreate()

# 2. Configurar la conexión JDBC a PostgreSQL.
# Nota: En el entorno docker-compose, el hostname para la base de datos es "postgres-db".
jdbc_url = "jdbc:postgresql://postgres-db:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "test_view",
    "driver": "org.postgresql.Driver"
}

# 3. Leer las tablas "users", "posts" y "post_likes" desde PostgreSQL utilizando JDBC.
df_users = spark.read.jdbc(url=jdbc_url, table="users", properties=connection_properties)
df_posts = spark.read.jdbc(url=jdbc_url, table="posts", properties=connection_properties)
df_likes = spark.read.jdbc(url=jdbc_url, table="post_likes", properties=connection_properties)

# 4. Filtrar las publicaciones creadas en los últimos 30 días
df_posts_recent = df_posts.filter(col("created_at") >= date_sub(current_timestamp(), 30))

# 5. Realizar un LEFT JOIN entre las publicaciones recientes y los likes (para incluir publicaciones sin likes)
df_posts_with_likes = df_posts_recent.join(
    df_likes,
    df_posts_recent.id == df_likes.post_id,
    how="left"
)

# 6. Agrupar las publicaciones y calcular el total de likes por publicación
df_top_posts = df_posts_with_likes.groupBy(
    df_posts_recent.id.alias("post_id"),
    df_posts_recent.user_id,
    df_posts_recent.content,
    df_posts_recent.created_at
).agg(
    count(df_likes.id).alias("total_likes")
)

# 7. Ordenar las publicaciones por la cantidad de likes en forma descendente y limitar la salida a las 10 con mayor cantidad de likes.
top10_posts = df_top_posts.orderBy(desc("total_likes")).limit(10)

# 8. Mostrar el resultado
top10_posts.show(truncate=False)
