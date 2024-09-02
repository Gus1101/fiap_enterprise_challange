import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import pandas as pd

# Variáveis de ambiente
caminho_env = 'c:/Users/Augusto/Desktop/FIAP/enterprise_challange/config/.env'
load_dotenv(caminho_env)

API_KEY = os.getenv('API_KEY')

def gerar_datas_ate(data_final):
    """
    Função para gerar uma lista com datas no ano de 2024
    """
    datas = []
    data_atual = datetime(2024, 1, 1).date()
    while data_atual <= data_final:
        datas.append(data_atual.strftime('%Y-%m-%d'))
        data_atual += timedelta(days=1)
    return datas

def requisitar_dados(data):
    """
    Função para realizar requisições em uma API
    """
    url = f'http://api.weatherapi.com/v1/history.json?key={API_KEY}&q=Sao Paulo&dt={data}'
    resposta = requests.get(url)
    if resposta.status_code == 200:
        return resposta.json()
    else:
        print(f'Erro na requisição para {data}: {resposta.status_code}')
        return None

# Gera a lista de datas até data final
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

# Normalizar a parte 'forecastday' dentro de 'forecast'
df_forecast = pd.json_normalize(df['forecast.forecastday'].explode())

# Normalizar a parte 'hour' dentro de 'forecastday'
if 'hour' in df_forecast.columns:
    df_hour = pd.json_normalize(df_forecast['hour'].explode())
    df_forecast = df_forecast.drop(columns=['hour']).join(df_hour, rsuffix='_hour')

# Concatenar com o DataFrame principal
df_final = pd.concat([df.drop(columns='forecast.forecastday'), df_forecast], axis=1)

# Caminho para salvar o arquivo .parquet
caminho_parquet = 'c:/Users/Augusto/Desktop/FIAP/enterprise_challange/data/weather_sp.parquet'

# Salvar o DataFrame como um arquivo .parquet
df_final.to_parquet(caminho_parquet, engine='pyarrow')

print(f'DataFrame salvo em {caminho_parquet}')
