# class-big-data

## Descripción del Proyecto

Este proyecto tiene como objetivo analizar datos de publicaciones y likes utilizando Apache Spark y PostgreSQL. Se incluyen varios scripts para leer datos desde PostgreSQL, realizar análisis de likes y detectar publicaciones virales.

## Requisitos

- Python 3.12
- Apache Spark
- PostgreSQL
- Docker
- Poetry

## Instalación

1. **Configurar el entorno virtual con Poetry:**

    ```sh
    poetry env use python3.12
    poetry install
    ```

2. **Iniciar el clúster de Spark con Docker:**

    ```sh
    docker compose -f docker/compose-spark.yml up -d
    ```

## Estructura del Proyecto

- `examples_spark/read_postgres.py`: Script para leer datos de la vista `post_likes` desde PostgreSQL y mostrarlos.
- `examples_spark/viral_posts.py`: Script para analizar publicaciones virales basándose en el ratio de likes diarios.
- `examples_spark/analisis_likes.py`: Script para analizar los likes de publicaciones recientes y mostrar las publicaciones más populares.

## Uso

### Leer datos de PostgreSQL

Ejecutar el script `read_postgres.py` para leer y mostrar datos de la vista `post_likes`:

```sh
python examples_spark/read_postgres.py
```
### Viral Post Analysis

Run the viral_posts.py script to detect viral posts:

```sh
python examples_spark/viral_posts.py
```

### Recent Post Like Analysis

Run the analisis_likes.py script to analyze likes of recent posts:

```sh
python examples_spark/analisis_likes.py
```

