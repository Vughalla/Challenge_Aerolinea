from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract_all, lit, size, explode, count, col

spark = SparkSession.builder \
    .appName("Procesamiento de tweets") \
    .getOrCreate()


def q2_time(file_path: str) -> List[Tuple[str, int]]:
    # Lee los datos JSON en un DataFrame de Spark
    json_df = spark.read.json(file_path)

    # Se filtran del dataframe todas las columnas excepto la deseada para obtener los datos.
    json_df = json_df.select(col('content'))

    # Esta expresion regular hace match con todos los codigos Unicode existentes para emojis.
    emoji_pattern = r'[\uD83C-\uDBFF\uDC00-\uDFFF\uD800-\uDB7F\uDC00-\uDFFF]'

    # Se aplica la regex para eliminar de la columna 'content' todos los caracteres que no hagan match con la misma.
    json_df = json_df.select(regexp_extract_all('content', lit(emoji_pattern),0).alias('emojis'))

    # Se eliminan los registros cuyo lenght == 0, es decir, aquellos que no contenian emojis.
    json_df = json_df.filter(size(json_df.emojis) > 0)

    # Se aplica un flatten a las listas de registros para que cada registro corresponda a un emoji.
    json_df = json_df.select(explode(col("emojis")).alias("emojis"))

    # Se agrupan los registros por emoji, contando la frecuencia de cada uno. Además se acorta el df a 15 resultados.
    json_df = json_df.groupBy("emojis").agg(count("*").alias("frequency")).orderBy(col("frequency").desc()).limit(15)

    # Ya que dentro de los codigos Unicode matcheados existían Emoji's Modifiers (modificadores de color por ejemplo) los cuales no son propiamente emojis, se filtran antes de retornar los valores correspondientes.
    return [(row.emojis, row.frequency) for row in json_df.collect() if ord(row.emojis) not in (45,65039,127995,127997)][0:10]