# class-big-data

poetry env use python3.12

poetry install

docker compose -f docker/compose-spark.yml up -d   # Start Spark cluster


