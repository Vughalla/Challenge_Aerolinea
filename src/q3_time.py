from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count

spark = SparkSession.builder \
    .appName("Procesamiento de tweets") \
    .getOrCreate()


def q3_time(file_path: str) -> List[Tuple[str, int]]:
    # Se cargan los registros en un pyspark dataframe.
    mentioned_users_df = spark.read.json(file_path)

    # Selecciona los valores del campo mentionedUsers
    mentioned_users_df = mentioned_users_df.select(col("mentionedUsers").getField("username").alias("mentioned_username"))

    # Se filtran aquellos valores donde mentionedUsers sea nulo. El resultado es una lista que puede tener los nombresd de uno o más usuarios mencionados.
    mentioned_users_df = mentioned_users_df.filter(mentioned_users_df.mentioned_username.isNotNull())

    # A partir de la lista anteriormente obtenida, se extraen de las mismas todos los nombres devolviendo un dataframe donde cada registro es el nombre de un usuario.
    mentioned_users_df = mentioned_users_df.select(explode(col("mentioned_username")).alias("mentioned_username"))

    # Se cuentan la cantidad de veces que cada usuario ha sido arrobado. Devuelve un df con los nombres de usuario y cantidad de veces que fue citado.
    mentioned_users_df = mentioned_users_df.groupBy("mentioned_username").agg(count("*").alias("frequency")).orderBy(col("frequency").desc())

    # Se extraen los 10 primeros registros, los cuales corresponden a los 10 usuarios más citados.
    mentioned_users_df = mentioned_users_df.limit(10)

    # Se ordenan los resultados según el formato requerido.
    return [(row.mentioned_username, row.frequency) for row in mentioned_users_df.collect()]