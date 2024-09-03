import pandas as pd
import sqlite3

coordenates = pd.read_csv("data/coordenates.csv",sep=",",encoding="latin1")
ml_data = pd.read_parquet("data/ml_enhanced_df.parquet")

complete_df = ml_data.merge(coordenates[["Sigla","Longitude","Latitude"]],left_on="dscsubestacaodistribuicao",right_on="Sigla",how="left")
complete_df = complete_df.drop(columns="Sigla")

complete_df.to_parquet("data/complete_df.parquet")

# Conecta ao banco de dados SQLite (cria o banco se não existir)
conn = sqlite3.connect("data/complete_df.sqlite")

# Exporta o DataFrame para uma tabela no banco de dados SQLite
complete_df.to_sql("complete_df", conn, if_exists="replace", index=False)

# Fecha a conexão
conn.close()