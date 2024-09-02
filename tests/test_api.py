import pytest
from datetime import datetime, timedelta
import pandas as pd
from unittest.mock import Mock

from functions.api_calls import gerar_datas_ate, requisitar_dados

def test_gerar_datas_ate():
    # Mock da data final
    data_final = datetime(2024, 6, 30).date()
    
    datas = gerar_datas_ate(data_final)
    
    # Verifica se a lista contém todas as datas corretas
    esperado = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(181)]
    assert datas == esperado

def test_requisitar_dados(mocker):
    # Mock da resposta da API
    mock_resposta = Mock()
    mock_resposta.status_code = 200
    mock_resposta.json.return_value = {'data': 'fake_data'}
    mock_get = mocker.patch('functions.api_calls.requests.get', return_value=mock_resposta)
    
    dados = requisitar_dados('2024-06-01')
    
    assert dados == {'data': 'fake_data'}
    mock_get.assert_called_once_with('http://api.weatherapi.com/v1/history.json?key=YOUR_API_KEY&q=Sao Paulo&dt=2024-06-01')

def test_salvar_dataframe(mocker):
    # Mock do DataFrame
    df_mock = Mock()
    mock_json_normalize = mocker.patch('functions.api_calls.pd.json_normalize', return_value=df_mock)
    mock_to_parquet = mocker.patch('pandas.DataFrame.to_parquet', return_value=None)
    
    # Caminho para salvar o arquivo .parquet
    caminho_parquet = 'c:/Users/Augusto/Desktop/FIAP/enterprise_challange/resultado.parquet'
    
    # Simule a chamada para salvar o DataFrame
    df = mock_json_normalize([])
    df.to_parquet(caminho_parquet, engine='pyarrow')
    
    # Verifique se o método `to_parquet` foi chamado com o caminho correto
    mock_to_parquet.assert_called_once_with(caminho_parquet, engine='pyarrow')