# Importa libs
import pandas as pd
import numpy as np

# Importa Bases
sp = pd.read_parquet("data/weather_sp.parquet")
interrupcoes = pd.read_parquet("data/interrupcoes-energia-eletrica-2024.parquet")
subestacoes = pd.read_csv("data/siglas_nomes.csv",sep=",", encoding="utf-8")

# Trata coluna
sp.columns = sp.columns.str.lower().str.strip()
interrupcoes.columns = interrupcoes.columns.str.lower().str.strip()

# Trata base de ocorrencias em SP
sp_columns_mantain = ["date","day.avgtemp_c","day.maxwind_mph","day.totalprecip_mm","day.avghumidity",
                         "day.daily_will_it_rain","day.daily_chance_of_rain","wind_kph","precip_mm",
                         "will_it_rain","chance_of_rain"]

sp_columns_rename = {"wind_kph":"forecast.wind_kph","precip_mm":"forecast.precip_mm",
                        "will_it_rain":"forecast.will_it_rain","chance_of_rain":"forecast.chance_of_rain"}

sp = sp[sp_columns_mantain]
sp = sp.rename(columns=sp_columns_rename)

sp["date"] = pd.to_datetime(sp["date"])

# Trata bases de registros de clima em SP
interrupcoes_columns_mantain = ["datiniciointerrupcao","dscsubestacaodistribuicao","dsctipointerrupcao","idemotivointerrupcao",
                            "numniveltensao","numunidadeconsumidora","numconsumidorconjunto"]
interrupcoes = interrupcoes[interrupcoes_columns_mantain]

interrupcoes["idemotivointerrupcao"] = interrupcoes["idemotivointerrupcao"].astype(str)
interrupcoes["idemotivointerrupcao"] = interrupcoes["idemotivointerrupcao"].fillna(0)
interrupcoes = interrupcoes[interrupcoes["idemotivointerrupcao"].isin(["3.0","6.0"])]
interrupcoes["dsctipointerrupcao"] = np.where(interrupcoes["dsctipointerrupcao"] == "Não Programada",1,0)
interrupcoes = interrupcoes[interrupcoes["dsctipointerrupcao"] == 1]

interrupcoes["datiniciointerrupcao"] = pd.to_datetime(interrupcoes["datiniciointerrupcao"])
interrupcoes["date"] = interrupcoes["datiniciointerrupcao"].dt.date
interrupcoes["date"] = pd.to_datetime(interrupcoes["date"])

# Merge para recuperar regiões

# Trata subestacoes
subestacoes = subestacoes[["Sigla","Nome"]]
subestacoes = subestacoes.rename(columns={"Sigla":"dscsubestacaodistribuicao","Nome":"nome"})

interrupcoes = interrupcoes[interrupcoes["dscsubestacaodistribuicao"].isin(subestacoes["dscsubestacaodistribuicao"])]
interrupcoes = interrupcoes.merge(subestacoes,left_on="dscsubestacaodistribuicao",right_on="dscsubestacaodistribuicao",how="left")

interrupcoes_ocorrencias_sp = interrupcoes.merge(sp,left_on="date",right_on="date",how="left")

interrupcoes_ocorrencias_sp.to_parquet("data/interrupcoes_ocorrencias_sp.parquet")