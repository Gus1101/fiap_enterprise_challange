# Importa libs
import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()

# Importa Bases
sp = pd.read_parquet("data/weather_sp.parquet")
interrupcoes = pd.read_parquet("data/interrupcoes-energia-eletrica-2024.parquet")
subestacoes = pd.read_csv("data/siglas_nomes.csv",sep=",", encoding="utf-8")

# Trata coluna
sp.columns = sp.columns.str.lower().str.strip()
interrupcoes.columns = interrupcoes.columns.str.lower().str.strip()

# Trata base de clima em SP
sp_columns_mantain = ["date","day.avgtemp_c","day.maxwind_kph","day.totalprecip_mm","day.avghumidity",
                         "day.daily_will_it_rain","day.daily_chance_of_rain","wind_kph","precip_mm",
                         "will_it_rain","chance_of_rain"]

sp_columns_rename = {"wind_kph":"forecast.wind_kph","precip_mm":"forecast.precip_mm",
                        "will_it_rain":"forecast.will_it_rain","chance_of_rain":"forecast.chance_of_rain"}

sp = sp[sp_columns_mantain]
sp = sp.rename(columns=sp_columns_rename)

sp["date"] = pd.to_datetime(sp["date"])

# Trata bases de ocorrencias em SP
interrupcoes_columns_mantain = ["datiniciointerrupcao","dscsubestacaodistribuicao","dsctipointerrupcao","idemotivointerrupcao",
                            "numniveltensao","numunidadeconsumidora","numconsumidorconjunto"]
interrupcoes = interrupcoes[interrupcoes_columns_mantain]

interrupcoes["idemotivointerrupcao"] = interrupcoes["idemotivointerrupcao"].astype(str)
interrupcoes["idemotivointerrupcao"] = interrupcoes["idemotivointerrupcao"].fillna(0)
interrupcoes = interrupcoes[interrupcoes["idemotivointerrupcao"].isin(["0.0","3.0","6.0","8.0"])]
interrupcoes["dsctipointerrupcao"] = np.where(interrupcoes["dsctipointerrupcao"] == "Não Programada",1,0)
interrupcoes["falha_na_rede"] = np.where((interrupcoes["dsctipointerrupcao"] == 0) & (interrupcoes["idemotivointerrupcao"] == 0),0,1)

interrupcoes["datiniciointerrupcao"] = pd.to_datetime(interrupcoes["datiniciointerrupcao"])
interrupcoes["date"] = interrupcoes["datiniciointerrupcao"].dt.date
interrupcoes["date"] = pd.to_datetime(interrupcoes["date"])

# Merge para recuperar regiões

# Trata subestacoes
subestacoes = subestacoes[["Sigla","Nome"]]
subestacoes = subestacoes.rename(columns={"Sigla":"dscsubestacaodistribuicao","Nome":"nome"})

interrupcoes = interrupcoes[interrupcoes["dscsubestacaodistribuicao"].isin(subestacoes["dscsubestacaodistribuicao"])]
interrupcoes = interrupcoes.merge(subestacoes,left_on="dscsubestacaodistribuicao",right_on="dscsubestacaodistribuicao",how="left")

interrupcoes_ocorrencias_sp = interrupcoes.merge(sp,left_on="date",right_on="date",how="outer")

lista = {"dscsubestacaodistribuicao":subestacoes["dscsubestacaodistribuicao"],"nome":subestacoes["nome"]}

# Função para gerar uma linha de dados sintéticos
def generate_synthetic_data(num_records):
    data = []
    for _ in range(num_records):
        index = np.random.randint(len(lista['dscsubestacaodistribuicao']))
        data.append([
            fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),  # datiniciointerrupcao
            lista['dscsubestacaodistribuicao'][index],  # dscsubestacaodistribuicao
            1,  # dsctipointerrupcao
            np.random.choice([3.0, 6.0, 8.0]),  # idemotivointerrupcao
            np.random.choice([127, 220, 13800]),  # numniveltensao (127, 220 ou 13800)
            np.random.randint(0, 39924),  # numunidadeconsumidora (exemplo: 1 a 1000)
            np.random.randint(1257.0, 157837.0),  # numconsumidorconjunto (exemplo: 1 a 1000)
            fake.date_this_year().strftime('%Y-%m-%d'),  # date
            lista['nome'][index],  # nome
            round(np.random.uniform(15, 35), 1),  # day.avgtemp_c (exemplo: 15°C a 35°C)
            round(np.random.uniform(0, 60), 1),  # day.maxwind_mph (exemplo: 0 a 32 mph)
            round(np.random.uniform(0, 50), 1),  # day.totalprecip_mm (exemplo: 0 a 50 mm)
            round(np.random.uniform(30, 90), 1),  # day.avghumidity (exemplo: 30% a 90%)
            np.random.randint(0, 2),  # day.daily_will_it_rain (0 ou 1)
            round(np.random.uniform(0, 100), 1),  # day.daily_chance_of_rain (exemplo: 0% a 100%)
            round(np.random.uniform(0, 50), 1),  # forecast.wind_kph (exemplo: 0 a 50 km/h)
            round(np.random.uniform(0.00, 0.08)),
            np.random.randint(0, 2),
            round(np.random.uniform(0, 100), 1),
            0,
        ])
    return data

# Número de registros sintéticos a serem gerados
num_records = 20000

# Gera os dados sintéticos
synthetic_data = generate_synthetic_data(num_records)

# Cria um DataFrame com os dados gerados
df_synthetic = pd.DataFrame(synthetic_data, columns=[
    'datiniciointerrupcao',
    'dscsubestacaodistribuicao',
    'dsctipointerrupcao',
    'idemotivointerrupcao',
    'numniveltensao',
    'numunidadeconsumidora',
    'numconsumidorconjunto',
    'date',
    'nome',
    'day.avgtemp_c',
    'day.maxwind_kph',
    'day.totalprecip_mm',
    'day.avghumidity',
    'day.daily_will_it_rain',
    'day.daily_chance_of_rain',
    'forecast.wind_kph',
    'forecast.precip_mm',
    'forecast.will_it_rain',
    'forecast.chance_of_rain',
    'falha_na_rede'
])

final_df = pd.concat([interrupcoes_ocorrencias_sp,df_synthetic])

tipos = {
    'datiniciointerrupcao': 'datetime64[ns]',
    'dscsubestacaodistribuicao': 'str',  
    'dsctipointerrupcao': 'int',  
    'idemotivointerrupcao': 'float',    
    'numniveltensao':'float',
    'numunidadeconsumidora':'float',
    'numconsumidorconjunto':'float',
    'date':'datetime64[ns]',
    'nome':'str',
    'day.avgtemp_c':'float',
    'day.maxwind_kph':'float',
    'day.totalprecip_mm':'float',
    'day.avghumidity':'float',
    'day.daily_will_it_rain':'int',
    'forecast.wind_kph':'float',
    'forecast.precip_mm':'float',
    'forecast.will_it_rain':'int',
    'forecast.chance_of_rain':'float',
    'falha_na_rede':'int',
}

final_df["forecast.will_it_rain"] = final_df["forecast.will_it_rain"].fillna(0)
final_df["day.daily_will_it_rain"] = final_df["day.daily_will_it_rain"].fillna(0)
final_df = final_df.astype(tipos) 

# Export dos dados
final_df.to_parquet("data/interrupcoes_ocorrencias_sp.parquet")