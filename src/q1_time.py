from typing import List, Tuple
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, desc, row_number
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("Procesamiento de tweets") \
    .getOrCreate()


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Crea un dataframe a partir del archivo JSON.
    json_df = spark.read.json(file_path)

    # Cambia el formato de la columna date a YYYY-MM-DD para dejar solo la fecha.
    json_df = json_df.withColumn('formated_date', to_date(col('date').substr(1, 10)))

    # Crea un df con las 10 fechas donde más se crearon más cantidad de tweets. 
    date_df = json_df.groupBy(col('formated_date')).count().orderBy(desc('count')).limit(10)

    # Elimina del df leido a partir del json aquellos registros que no pertenezcan a las 10 fechas con más actividad.
    json_df = json_df.join(date_df, json_df['formated_date'] == date_df['formated_date'], 'inner').select(json_df['*'])

    # Se agrupan los registros por fecha y nombre de usuario y se cuentan los tweets de cada usuario para cada una de las 10 fechas más activas.
    grouped_df = json_df.groupBy('formated_date', col('user.username').alias('username')).count()

    # Se filtran los registros para obtener a los 10 usuarios más activos.
    window_spec = Window.partitionBy('formated_date').orderBy(desc('count'))
    max_tweets_df = grouped_df.withColumn('rank', row_number().over(window_spec)) \
            .filter(col('rank') == 1) \
            .drop('rank') \
            .orderBy('formated_date').limit(10)

    # Se retornan los resultados en el formato deseado.
    return [(row.formated_date, row.username) for row in max_tweets_df.collect()]


  