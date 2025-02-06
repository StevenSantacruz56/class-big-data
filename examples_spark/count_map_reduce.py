from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("WordCountMapReduce") \
    .master("spark://localhost:7077") \
    .getOrCreate()
# Crear un contexto de Spark
sc = spark.sparkContext

# Datos de entrada (puede ser un archivo de texto en HDFS o local)
text_data = ["Hola mundo este es un ejemplo de conteo de palabras",
             "Mundo de Big Data y Spark",
             "Spark es increíble para procesar datos",
             "Big Data es el futuro"]

# Crear un RDD a partir de los datos
rdd = sc.parallelize(text_data)

# Paso 1: Map - Dividir en palabras y asignar el valor 1 a cada una
words_rdd = rdd.flatMap(lambda line: line.lower().split()) \
               .map(lambda word: (word, 1))

# Paso 2: Reduce - Sumar las ocurrencias de cada palabra
word_count_rdd = words_rdd.reduceByKey(lambda a, b: a + b)

# Mostrar los resultados
results = word_count_rdd.collect()
for word, count in results:
    print(f"{word}: {count}")
