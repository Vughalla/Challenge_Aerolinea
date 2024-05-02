from typing import List, Tuple
import pandas as pd


def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Lee los datos JSON y los carga en un DataFrame de Pandas.
    json_df = pd.read_json(file_path, lines=True)

    # Se filtran todas las columnas menos la que contiene la información deseada a la vez que se eliminan los valores nulos de la misma.
    json_df = json_df['mentionedUsers'].dropna()

    # Se realiza un flatten a la lista de jsons contenida en la columna mentionedUsers y se extraen de la misma solo los username.
    json_df = json_df.explode('mentionedUsers').apply(lambda x: x['username']).to_frame() 

    # Agrupando por nombre de usuario, se realiza un count para saber la cantidad de menciones que cada usuario tuvo en el set de datos.
    grouped_df = json_df.groupby(['mentionedUsers']).size().reset_index(name='count')

    # Se ordenan los valores en orden descendente y se toman sólo los primeros diez registros, que corresponden a los usuarios más citados.
    grouped_df = grouped_df.sort_values(by='count', ascending=False).head(10)

    # Se ordenan los resultados según el formato requerido.
    return list(zip(grouped_df['mentionedUsers'], grouped_df['count']))