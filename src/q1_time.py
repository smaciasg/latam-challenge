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

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    data = []
    unique_keys = set()

    with open(file_path, 'r') as file:
        for line in file:
            json_data = json.loads(line.strip())
            data.append(json_data)
            unique_keys.update(json_data.keys())

    df_inicial = pd.DataFrame(data).reindex(columns=unique_keys)
    df_inicial['date_tw'] = pd.to_datetime(df_inicial['date']).dt.date

    df_user = json_normalize(df_inicial['user'])
    df_user.columns = ['user.'+col for col in df_user.columns]
    df_inicial = pd.concat([df_inicial[['id','date_tw']], df_user['user.username']], axis=1).reset_index(drop=True)

    df_tw_u = df_inicial.groupby(['date_tw', 'user.username'])['id'].count().reset_index(name='count_user')
    df_tw_ug = df_tw_u.groupby('date_tw')['count_user'].sum().reset_index(name='count_total')

    df_tw_u = pd.merge(left=df_tw_u, right=df_tw_ug, how='left', on='date_tw').sort_values(by=['count_total', 'count_user'], ascending=[False, False])

    indices_max = df_tw_u.groupby('date_tw')['count_user'].idxmax()
    df_tw_u_f = df_tw_u.loc[indices_max].sort_values(by=['count_total'], ascending=False).iloc[0:10].reset_index(drop=True)

    lista_resultado = [(row['date_tw'], row['user.username']) for _, row in df_tw_u_f.iterrows()]

    return lista_resultado



file_path = r"D:\\Personal\\Prueba_Tecnica\\LATAM\\challenge_DE\\src\\farmers-protest-tweets-2021-2-4.json"
print(q1_time(file_path))