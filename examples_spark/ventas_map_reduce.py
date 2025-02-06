from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("VentasRDDExample") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Crear un contexto de Spark
sc = spark.sparkContext

# Datos ficticios de ventas
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

# Crear un RDD
rdd = sc.parallelize(data)

# 1. Total de ventas por categoría
ventas_por_categoria = (
    # (Categoría, Ingresos = Precio * Cantidad)
    rdd.map(lambda x: (x[2], x[3] * x[4]))
       .reduceByKey(lambda a, b: a + b)      # Sumar los ingresos por categoría
)

print("=== Total de Ventas por Categoría ===")
for categoria, total in ventas_por_categoria.collect():
    print(f"{categoria}: {total}")

# 2. Producto más vendido por categoría (en cantidad)
producto_mas_vendido = (
    # ((Categoría, Producto), Cantidad)
    rdd.map(lambda x: ((x[2], x[1]), x[4]))
    # Sumar las cantidades por producto
       .reduceByKey(lambda a, b: a + b)
    # (Categoría, (Producto, Cantidad))
       .map(lambda x: (x[0][0], (x[0][1], x[1])))
    # Mantener el producto con más cantidad
       .reduceByKey(lambda a, b: a if a[1] > b[1] else b)
)

print("\n=== Producto Más Vendido por Categoría ===")
for categoria, (producto, cantidad) in producto_mas_vendido.collect():
    print(f"{categoria}: {producto} ({cantidad})")

# 3. Promedio de ingresos por categoría
promedio_ingresos = (
    rdd.map(lambda x: (x[2], (x[3] * x[4], 1)))  # (Categoría, (Ingresos, 1))
    # Sumar ingresos y contar elementos
       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
       .map(lambda x: (x[0], x[1][0] / x[1][1]))  # Calcular el promedio
)

print("\n=== Promedio de Ingresos por Categoría ===")
for categoria, promedio in promedio_ingresos.collect():
    print(f"{categoria}: {promedio:.2f}")
