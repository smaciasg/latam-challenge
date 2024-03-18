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

def q2_time(file_path: str) -> List[Tuple[str, int]]:
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
    
    #Usando la librería de emoji se crea una lista con los emojies "c" dentro un texto que se pasa, a la función
    def extraer_emojis(texto):
        return [c for c in texto if c in emoji.EMOJI_DATA]

    #Se aplica la función sobre la columna de contenido y se crea una nueva con el fin de procesar los emojies posteriormente
    df_inicial['emojis'] = df_inicial['content'].apply(extraer_emojis)
    
    lista_emojies = []

    #Se crea un ciclo para obtener todos los emojies en una sola lista.
    for fila in df_inicial['emojis']:
        lista_emojies.extend(fila)

    df_ef = pd.DataFrame(pd.DataFrame(lista_emojies,columns=['emojie']).value_counts(),columns=['Cuenta']).reset_index()
    
    #Se filtran los símbolos que representan la modificación de color de los emojies, se crea esta función bajo el supuesto que por ahora solo se necesita la figura del emojie no su color.
    df_ef = df_ef[~df_ef['emojie'].isin(['🏻','🏽','🏼','🏾','🏿'])][0:10]
    
    lista_resultado = [(row['emojie'], row['Cuenta']) for _, row in df_ef.iterrows()]

    return lista_resultado

file_path = r"D:\\Personal\\Prueba_Tecnica\\LATAM\\challenge_DE\\src\\farmers-protest-tweets-2021-2-4.json"
print(q2_time(file_path))