import boto3

bucket_name = 'projeto-pucminas-g3bigdata-raw'
csv_key = 'pucminas-g3bigdata/dados_aneel.csv'
local_file = 'dados_aneel.csv'

# Criar cliente S3
s3 = boto3.client('s3')

# Baixar CSV para a EC2
s3.download_file(bucket_name, csv_key, local_file)

print(f"CSV baixado para {local_file}")
