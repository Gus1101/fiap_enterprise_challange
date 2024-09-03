import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix

def process_and_predict(df):
    # Selecione as colunas de entrada e a coluna alvo
    X = df[['day.avgtemp_c', 'day.maxwind_kph', 'day.totalprecip_mm', 'day.avghumidity', 'day.daily_will_it_rain']].fillna(0)
    y = df['falha_na_rede']

    # Divida os dados em conjuntos de treino e teste
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Normalização dos dados
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Criação e treinamento do modelo
    model = LogisticRegression(max_iter=1000)  # Ajuste max_iter se necessário
    model.fit(X_train_scaled, y_train)

    # Predição e avaliação no conjunto de teste
    y_pred = model.predict(X_test_scaled)
    conf_matrix = confusion_matrix(y_test, y_pred)
    class_report = classification_report(y_test, y_pred)

    # Normaliza os dados originais para previsão
    X_scaled = scaler.transform(X)

    # Previsões de probabilidade para todos os dados
    df['probabilidade_falha_na_rede'] = model.predict_proba(X_scaled)[:, 1]

    print("Confusion Matrix:")
    print(conf_matrix)
    print("\nClassification Report:")
    print(class_report)

    return df, model, scaler

df = pd.read_parquet('data/interrupcoes_ocorrencias_sp.parquet')  # Carregar seu DataFrame

# Processar os dados e obter as previsões
df_com_probabilidades, model, scaler = process_and_predict(df)

df_com_probabilidades.to_parquet("data/ml_enhanced_df.parquet")