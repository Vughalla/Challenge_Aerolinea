from typing import List, Tuple
from datetime import datetime
import pandas as pd

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:

    df_concat = pd.DataFrame(columns=['date', 'username'])
    for json_df in pd.read_json(file_path, lines=True, chunksize=100):
        # Se extraen los key/value del json anidado y se guarda en un nuevo df.
        user_df = json_df['user'].apply(pd.Series)

        # Se remueven todas las columnas excepto date para reducir la carga en memoria de la variable.
        json_df = json_df['date']

        # Se remueven todas las columnas excepto username para reducir la carga en memoria de la variable.
        user_df = user_df['username']

        # Ambos dataframes se unen nuevamente.
        json_df = pd.concat([json_df, user_df], axis=1)

        # Se agregan los registros del nuevo chuck, ya fitrando toda la data que no será usada, al df acumulador.
        df_concat = pd.concat([df_concat, json_df])

    json_df = df_concat

    # Se elimina el df para liberar espacio en memoria.
    del user_df
    del df_concat

    # Convierte el formato de fecha a YYYY-MM-DD
    json_df['date'] = pd.to_datetime(json_df['date']).dt.date

    # Agrupa por fecha y filtra todas excepto aquellas con mayor cantidad de tweets / dia.
    date_df = json_df.groupby(['date']).size().reset_index(name='count').sort_values(by='count').tail(10)

    # Remueve del dataframe original todos los registros donde la fecha sea diferente a aquellas donde hubo más actividad.
    json_df = json_df[json_df.date.isin(date_df['date'])]

    # Se elimina el df para liberar espacio en memoria.
    del date_df

    # Agrupa por fecha y usuario, y cuenta el número de tweets por fecha y usuario
    json_df = json_df.groupby(['date', 'username']).size().reset_index(name='count')

    # Encuentra el usuario con más tweets por fecha
    json_df = json_df.loc[json_df.groupby('date')['count'].idxmax()]

    # Ordena los resultados por fecha filtrando aquellas donde hayan menos actividad.
    json_df = json_df.sort_values(by='date').tail(10)

    # Recopila los resultados
    return list(zip(json_df['date'], json_df['username']))