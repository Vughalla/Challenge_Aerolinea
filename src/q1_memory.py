from typing import List, Tuple
from datetime import datetime
import pandas as pd


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:

    # Crea un dataframe de pandas a partir del json.
    json_df = pd.read_json(file_path, lines=True)

    user_df = json_df['user'].apply(pd.Series)
    json_df = pd.concat([json_df, user_df], axis=1)

    # Convierte el formato de fecha a YYYY-MM-DD
    json_df['date'] = pd.to_datetime(json_df['date']).dt.date

    # Agrupa por fecha y filtra todas excepto aquellas con mayor cantidad de tweets / dia.
    date_df = json_df.groupby(['date']).size().reset_index(name='count').sort_values(by='count').tail(10)

    # Remueve del dataframe original todos los registros donde la fecha sea diferente a aquellas donde hubo más actividad.
    json_df = json_df[json_df.date.isin(date_df['date'])]

    # Agrupa por fecha y usuario, y cuenta el número de tweets por fecha y usuario
    grouped_df = json_df.groupby(['date', 'username']).size().reset_index(name='count')

    # Encuentra el usuario con más tweets por fecha
    max_tweets_df = grouped_df.loc[grouped_df.groupby('date')['count'].idxmax()]

    # Ordena los resultados por fecha filtrando aquellas donde hayan menos actividad.
    max_tweets_df = max_tweets_df.sort_values(by='date').tail(10)

    # Recopila los resultados
    return list(zip(max_tweets_df['date'], max_tweets_df['username']))