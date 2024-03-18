from typing import List, Tuple
import pandas as pd
import numpy as np
import json
from pandas import json_normalize
import cProfile
from typing import List, Tuple
from datetime import datetime
from memory_profiler import memory_usage
import emoji
from collections import Counter

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Leer el archivo JSON y cargarlo en un DataFrame
    data = []
    # Crear conjunto para la claves únicas 
    unique_keys = set()

    with open(file_path, 'r') as file:
        for line in file:
            json_data = json.loads(line.strip())
            data.append(json_data)
            unique_keys.update(json_data.keys())

    #Crear el dataframe inicial en función de los datos leídos y con columnas nombradas tal como las claves únicas obtenidas
    df_inicial = pd.DataFrame(data).reindex(columns=unique_keys)
    
    #Dado que se ha validado previamente que no hay twits duplicados, se puede usar la columna mentionedUsers, para ser explotada (explode) y luego expandida (json_normalize) y con esta hacer todo el procesamiento solicitado. Por otro lado, antes de explotar y expandir.
    
    df_relevantes =  json_normalize(pd.DataFrame(df_inicial['mentionedUsers']).dropna().explode(column='mentionedUsers').reset_index(drop=True)['mentionedUsers'])
    
    # Crear DataFrame con los usuarios relevantes y su frecuencia
    df_relev = pd.DataFrame(Counter(df_relevantes['username']).most_common(), columns=['username', 'Cuenta'])
    
    #Se creas lista con las tuplas de nombre de uduario y el conteo de apareiocnes y se filtra para dejar solo los primeros 10 registros.
    lista_resultado = [(row['username'], row['Cuenta']) for _, row in df_relev.iterrows()][:10]
    
    return lista_resultado

file_path = r"D:\\Personal\\Prueba_Tecnica\\LATAM\\challenge_DE\\src\\farmers-protest-tweets-2021-2-4.json"
print(q3_memory(file_path))