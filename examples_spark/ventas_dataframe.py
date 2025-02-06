from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, desc

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("VentasDataFrameExample") \
    .master("spark://localhost:7077") \
    .getOrCreate()


# Crear datos ficticios de ventas
data = [
    # (ID, Producto, Categoría, Precio, Cantidad)
    (1, "Producto A", "Electrónica", 100, 2),
    (2, "Producto B", "Electrónica", 200, 1),
    (3, "Producto C", "Hogar", 50, 5),
    (4, "Producto D", "Hogar", 70, 3),
    (5, "Producto E", "Electrónica", 150, 4),
    (6, "Producto F", "Moda", 30, 10),
    (7, "Producto G", "Moda", 40, 8),
    (8, "Producto H", "Hogar", 60, 7),
    (9, "Producto I", "Electrónica", 300, 1),
    (10, "Producto J", "Moda", 20, 20)
]

# Definir esquema de columnas
columns = ["ID", "Producto", "Categoria", "Precio", "Cantidad"]

# Crear DataFrame
df = spark.createDataFrame(data, schema=columns)

# Calcular columna de ingresos (Precio * Cantidad)
df = df.withColumn("Ingresos", col("Precio") * col("Cantidad"))

# Mostrar el DataFrame inicial
print("=== Datos Iniciales ===")
df.show()

# 1. Total de ventas por categoría
total_ventas = df.groupBy("Categoria").agg(
    sum("Ingresos").alias("Total_Ventas"))
print("=== Total de Ventas por Categoría ===")
total_ventas.show()

# 2. Producto más vendido por categoría (en cantidad)
producto_mas_vendido = df.groupBy("Categoria", "Producto") \
                         .agg(sum("Cantidad").alias("Total_Cantidad")) \
                         .orderBy("Categoria", desc("Total_Cantidad"))

# Mostrar el producto más vendido por categoría
print("=== Producto Más Vendido por Categoría ===")
producto_mas_vendido.show()

# 3. Promedio de ingresos por categoría
promedio_ingresos = df.groupBy("Categoria").agg(
    avg("Ingresos").alias("Promedio_Ingresos"))
print("=== Promedio de Ingresos por Categoría ===")
promedio_ingresos.show()

# 4. Combinación de todas las métricas
analisis_final = total_ventas.join(
    promedio_ingresos, on="Categoria", how="inner")
print("=== Análisis Final de Ventas ===")
analisis_final.show()
