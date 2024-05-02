from typing import List, Tuple
import pandas as pd


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    # Lee los datos JSON en un DataFrame de Pandas
    json_df = pd.read_json(file_path, lines=True, encoding='utf-8')

    # Se filtran del dataframe todas las columnas excepto la deseada para obtener los datos.
    json_df = json_df['content'].to_frame() 

    # Esta expresion regular hace match con todos los codigos Unicode existentes para emojis.
    emoji_pattern = r'[\U0001F000-\U0001FFFF]'

    # Aplica la regex para extraer emojis y crea una nueva columna 'emojis'
    json_df['content'] = json_df['content'].str.findall(emoji_pattern)

    # Se eliminan los registros cuyo lenght == 0, es decir, aquellos que no contenian emojis.
    json_df = json_df[json_df['content'].apply(len) > 0]

    # Se aplica un flatten a las listas de registros para que cada registro corresponda a un emoji.
    json_df = json_df.explode('content')

    # Se agrupan los registros por emoji, contando la frecuencia de cada uno.
    json_df = json_df.groupby('content').size().reset_index(name='frequency')

    # Ordena los resultados por frecuencia y selecciona los primeros 15 resultados
    json_df = json_df.sort_values(by='frequency', ascending=False).head(15)

    # Ya que dentro de los codigos Unicode matcheados existían Emoji's Modifiers (modificadores de color por ejemplo) los cuales no son propiamente emojis, se filtran antes de retornar los valores correspondientes.
    json_df = json_df[json_df['content'].apply(lambda x: ord(x) not in (45, 65039, 127995, 127997))][0:10]

    # Se aplica el formato de salida deseado al dataframe.
    return list(zip(json_df['content'], json_df['frequency']))
