from typing import List, Tuple
from datetime import datetime
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

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]] :
    # Punto 1: Lista para almacenar los datos JSON
    data = []

    # Punto 2: Conjunto para almacenar los nombres de las claves únicas
    unique_keys = set()

    # Punto 3: Abrir el archivo JSON y leer línea por línea
    with open(file_path, 'r') as file:
        for line in file:
            # Punto 4: Cargar la línea como un objeto JSON
            json_data = json.loads(line.strip())
            
            # Punto 5: Agregar el objeto JSON a la lista
            data.append(json_data)
            
            # Punto 6: Actualizar el conjunto de claves únicas
            unique_keys.update(json_data.keys())

    # Punto 7: Crear DataFrame con los datos JSON
    df_inicial = pd.DataFrame(data).reindex(columns=unique_keys)

    # Punto 8: Modificar la columna de fechas para obtener el formato AAAA-MM-DD
    df_inicial['date_tw'] = pd.to_datetime(df_inicial['date']).dt.date
    
    # Punto 9: Crear un dataframe con los datos expandidos de los usuarios que luego se une al df inicial
    df_user = json_normalize(df_inicial['user'])
    ## Se asigna el prefijo a los nombres de las columnas para no perder el origen
    df_user.columns = ['user.'+col for col in df_user.columns ]
    ## Se une el df de usuarios el df inicial y se eliminan los ínidices
    df_inicial = pd.concat([df_inicial[['id','date_tw']].copy(),df_user],axis=1).reset_index(drop=True)   
    
    # Punto 10: Se crean groupby para procesar solo la información de fechas y nombres de usuario, agregando la columna del conteo de interacciones de usuarios
    df_tw_u = df_inicial.groupby(['date_tw','user.username'])['id'].count().reset_index(name='count_user')
    
    # Punto 11: Se crea un df nuevo que contiene solo el total de interacciones por fecha.
    df_tw_ug = df_tw_u.groupby('date_tw')['count_user'].sum().sort_values(ascending=False).reset_index(name='count_total')
    ## Se unen el df original de fechas, usuarios, conteo de interacciones por usuario al df que contiene las fechas e interacciones por fecha, se ordena de tal forma que el conteo total y por usuario es descendente
    df_tw_u = pd.merge(left=df_tw_u, right=df_tw_ug, how='left', on='date_tw').sort_values(by=['count_total','count_user'],ascending=[False,False])
    ## Se buscan los índices de la primera fila de cada fecha.
    indeices_max = df_tw_u.groupby('date_tw')['count_user'].idxmax()
    ## Se filtra del df de twits de tal forma que se ordene de la fecha de mayor interaccion a la menor, solo los primeros 10 registros
    df_tw_u_f = df_tw_u.loc[indeices_max].sort_values(by=['count_total'], ascending=False).iloc[0:10,::].reset_index(drop=True)
    
    # Punto 12: Se crea un ciclo para llenar una lista con la tuplas de fecha y el usuario con mayor cantidad de interacciones en X para ese día 
    lista_resultado = []
    for i in range(len(df_tw_u_f)):
        lista_resultado.append((df_tw_u_f['date_tw'][i],df_tw_u_f['user.username'][i]))
    
    return lista_resultado


file_path = r"D:\\Personal\\Prueba_Tecnica\\LATAM\\challenge_DE\\src\\farmers-protest-tweets-2021-2-4.json"
print(q1_memory(file_path))