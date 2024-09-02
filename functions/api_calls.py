import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import pandas as pd
import pytest

# Carregar as variáveis de ambiente
caminho_env = 'c:/Users/Augusto/Desktop/FIAP/enterprise_challange/config/.env'
load_dotenv(caminho_env)

API_KEY = os.getenv('API_KEY')

# Função para gerar todas as datas do ano até uma data específica
def gerar_datas_ate(data_final):
    datas = []
    data_atual = datetime(2024, 1, 1).date()
    while data_atual <= data_final:
        datas.append(data_atual.strftime('%Y-%m-%d'))
        data_atual += timedelta(days=1)
    return datas

# Função para realizar a requisição à API para uma data específica
def requisitar_dados(data):
    url = f'http://api.weatherapi.com/v1/history.json?key={API_KEY}&q=Sao Paulo&dt={data}'
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        print(f'Erro na requisição para {data}: {resposta.status_code}')
        return None

# Gera a lista de datas até data atual
data_final = datetime(2024, 6, 30).date()
datas = gerar_datas_ate(data_final)

# Realiza as requisições e armazena os resultados
resultados = []
for data in datas:
    dados = requisitar_dados(data)
    if dados:
        resultados.append(dados)

# Converter a lista de dicionários em um DataFrame
df = pd.json_normalize(resultados)

# Caminho para salvar o arquivo .parquet
caminho_parquet = 'c:/Users/Augusto/Desktop/FIAP/enterprise_challange/resultado.parquet'

# Salvar o DataFrame como um arquivo .parquet
df.to_parquet("C:/Users/Augusto/Desktop/FIAP/enterprise_challange/data/data/wheater_sp.parquet", engine='pyarrow')

print(f'DataFrame salvo em {"data/data"}')