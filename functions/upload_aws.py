from dotenv import load_dotenv
import boto3
import os

# Variáveis de ambiente
caminho_env = 'c:/Users/Augusto/Desktop/FIAP/enterprise_challange/config/.env'
load_dotenv(caminho_env)

AWS_KEY = os.getenv('AWS_KEY')
AWS_S_KEY = os.getenv('AWS_S_KEY')
AWS_REGION = os.getenv('AWS_REGION')


# Criar uma sessão com as credenciais padrão
session = boto3.Session(
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_S_KEY,
    region_name=AWS_REGION  
)

# Crie um cliente S3
s3 = session.client('s3')

# Especifique o bucket e o caminho do arquivo
bucket_name = 'fiap.ec'
file_path = 'data/complete_df.parquet'
object_name = 'ml_enhanced_data.parquet'

# Faça o upload
s3.upload_file(file_path, bucket_name, object_name)