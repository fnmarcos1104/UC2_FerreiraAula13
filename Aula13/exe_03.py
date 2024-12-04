import pandas as pd
import polars as pl
from datetime import datetime
import os
import gc


ENDERECO_DADOS = r'./Dados/'

 
try:
    print('Obtendo dados...')
   
    inicio = datetime.now()

    lista_arquivos = []

    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)
 
    hora_impressao = datetime.now()
 
    # print(f'Tempo de execução: {hora_impressao - inicio}')

    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)

    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df
        
        del df

        print()

        gc.collect()

        print(f'Tempo de execução: {hora_impressao - inicio}')
 
except ImportError as e:  
    print('Erro ao obter dados', e)