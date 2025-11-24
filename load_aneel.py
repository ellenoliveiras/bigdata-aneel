import pandas as pd
from sqlalchemy import create_engine

# Configuração do banco
db_user = 'dbadmin'
db_password = 'Grupo3PUC'
db_host = 'bigdata-aneel.cvom4e8sikgz.us-east-1.rds.amazonaws.com'
db_name = 'bigdata-aneel'

# Engine SQLAlchemy
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}')

# Ler csv em chunks e insere no banco
chunksize = 3000
first_chunk = True
chunk_index = 0
for df in pd.read_csv('dados_aneel.csv', encoding='latin1', sep=',', chunksize=chunksize):
    if_exists_mode = 'replace' if first_chunk else 'append'
    df.to_sql('dados_aneel', engine, if_exists=if_exists_mode, index=False, method='multi')
    first_chunk = False
    chunk_index += 1
    print(f"Chunk {chunk_index} inserido com {len(df)} linhas.")

print("Dados inseridos!")