import requests
import pandas as pd
import numpy as np

siglas = pd.read_csv("data/siglas_nomes.csv")

# DataFrame com as localidades
data = siglas[["Sigla","Nome"]] 

df = pd.DataFrame(data)

# Função para buscar coordenadas
def get_coordinates(location, city='São Paulo'):
    try:
        # Monta a URL da API Nominatim
        url = f"https://nominatim.openstreetmap.org/search?format=json&q={location}, {city}"
        response = requests.get(url).json()
        if response:
            lat = response[0]["lat"]
            lon = response[0]["lon"]
            return lat, lon
        else:
            return None, None
    except Exception as e:
        print(f"Erro ao buscar coordenadas para {location}: {e}")
        return None, None

# Adiciona colunas de latitude e longitude ao DataFrame
df[['Latitude', 'Longitude']] = df.apply(lambda row: get_coordinates(row['Nome']), axis=1, result_type='expand')
df[["Latitude","Longitude"]] = df[["Latitude","Longitude"]].astype(np.float64)
df = pd.DataFrame(df)

print(df)
# Export da bases
df.to_csv("data/coordenates.parquet")