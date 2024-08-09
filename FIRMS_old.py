import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv
import logging
import sys

# Recebe os argumentos do arquivo .bat
geojson_file = sys.argv[1]
log_filename = sys.argv[2]

# Configuração do logger
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

try:
    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv()

    # Configurações do banco de dados PostGIS
    POSTGRES_CONN = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    schema_name = os.getenv('NOME_SCHEMA')
    tabela_trabalhada = "julho_2024"

    # Função para obter o nome da tabela baseado no mês e ano de uma data
    def get_table_name(date_str):
        meses = [
            "janeiro", "fevereiro", "março", "abril", "maio", "junho",
            "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
        ]
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        mes = meses[date_obj.month - 1]
        ano = date_obj.year
        return f"{mes}_{ano}"

    # URL da API do FIRMS com a data dinâmica
    today = datetime.today().strftime('%Y-%m-%d')
    url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/70e04b8239b80f583c2af0f373766037/VIIRS_NOAA20_NRT/BRA/1/{today}"

    # Fazer o download dos dados CSV
    response = requests.get(url)
    response.raise_for_status()  # Levantar um erro se a requisição falhar

    # Salvar os dados CSV em um arquivo temporário
    csv_filename = "firms_data.csv"
    with open(csv_filename, 'wb') as file:
        file.write(response.content)

    # Ler o arquivo CSV usando pandas
    df = pd.read_csv(csv_filename)

    # Adicionar coluna 'hora_deteccao'
    def convert_acq_time(acq_time):
        hours = int(acq_time) // 100
        minutes = int(acq_time) % 100
        return f"{hours:02d}:{minutes:02d}:00"

    df['hora_deteccao'] = df['acq_time'].apply(convert_acq_time)

    # Converter DataFrame do CSV para GeoDataFrame
    geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
    firms_gdf = gpd.GeoDataFrame(df, geometry=geometry)
    firms_gdf.set_crs(epsg=4326, inplace=True)

    # Transformar a geometria para SIRGAS 2000 (EPSG:4674)
    firms_gdf = firms_gdf.to_crs(epsg=4674)

    # Carregar o GeoJSON
    geojson_gdf = gpd.read_file(geojson_file)

    # Transformar a geometria do GeoJSON para SIRGAS 2000 (EPSG:4674)
    geojson_gdf = geojson_gdf.to_crs(epsg=4674)

    # Filtrar pontos que estão dentro do polígono GeoJSON
    filtered_gdf = firms_gdf[firms_gdf.geometry.within(geojson_gdf.unary_union)]

    # Remover duplicatas com base na latitude e longitude
    filtered_gdf = filtered_gdf.drop_duplicates(subset=['latitude', 'longitude'])

    # Transformar 'acq_date' em DATE e 'hora_deteccao' em TIME
    filtered_gdf['acq_date'] = pd.to_datetime(filtered_gdf['acq_date'], format='%Y-%m-%d').dt.date
    filtered_gdf['hora_deteccao'] = pd.to_datetime(filtered_gdf['hora_deteccao'], format='%H:%M:%S').dt.time

    # Filtrar dados apenas para a data de hoje
    filtered_gdf = filtered_gdf[filtered_gdf['acq_date'] == datetime.today().date()]

    # Verificar se as colunas existem, se não, adicionar com valor padrão
    required_columns = [
        'latitude', 'longitude', 'acq_date', 'acq_time', 'confidence', 'scan', 'daynight',
        'version', 'frp', 'instrument', 'satellite', 'track'
    ]
    for column in required_columns:
        if column not in df.columns:
            df[column] = None

    # Conectar ao banco de dados PostgreSQL
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()

    # Criar o esquema se não existir
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')

    # Função para criar a tabela do mês se não existir
    def create_monthly_table(cur, schema_name, table_name):
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                acq_date DATE,
                acq_time TEXT,
                confidence TEXT,
                scan DOUBLE PRECISION,
                daynight TEXT,
                version TEXT,
                frp DOUBLE PRECISION,
                instrument TEXT,
                satellite TEXT,
                track DOUBLE PRECISION,
                hora_deteccao TIME WITHOUT TIME ZONE,
                geom GEOMETRY(Point, 4674)
            )
        ''')

    # Obter a última hora_deteccao do banco de dados
    def get_last_detection_time(cur, schema_name, table_name):
        cur.execute(f'''
            SELECT hora_deteccao FROM {schema_name}.{table_name}
            WHERE acq_date = %s
            ORDER BY hora_deteccao DESC LIMIT 1
        ''', (datetime.today().date(),))
        result = cur.fetchone()
        return result[0] if result else None

    # Verificar se os dados já existem no banco
    last_detection_time = get_last_detection_time(cur, schema_name, tabela_trabalhada)
    if last_detection_time:
        filtered_gdf = filtered_gdf[filtered_gdf['hora_deteccao'] > last_detection_time]

    # Inserir os dados na tabela usando ST_GeomFromText para a coluna geom
    insert_query = f'''
        INSERT INTO {schema_name}.{{}} (
            latitude, longitude, acq_date, acq_time, confidence, scan, daynight,
            version, frp, instrument, satellite, track, hora_deteccao, geom
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4674))
    '''

    # Agrupar por mês e inserir os dados na tabela correspondente
    data_to_insert = {}
    for idx, row in filtered_gdf.iterrows():
        table_name = get_table_name(row['acq_date'].strftime('%Y-%m-%d'))
        if table_name not in data_to_insert:
            create_monthly_table(cur, schema_name, table_name)
            data_to_insert[table_name] = []

        data_to_insert[table_name].append(
            (
                row['latitude'], row['longitude'], row['acq_date'], row['acq_time'], row['confidence'],
                row['scan'], row['daynight'], row['version'], row['frp'], row['instrument'],
                row['satellite'], row['track'], row['hora_deteccao'], f'POINT({row.geometry.x} {row.geometry.y})'
            )
        )

    for table_name, data in data_to_insert.items():
        cur.executemany(insert_query.format(table_name), data)

    # Fazer o commit e fechar a conexão
    conn.commit()
    cur.close()
    conn.close()

    # Remover o arquivo temporário CSV
    os.remove(csv_filename)

    total_inserido = sum(len(data) for data in data_to_insert.values())
    logging.info(f'{total_inserido} linhas inseridas nas tabelas no esquema {schema_name}')
    print(f'{total_inserido} linhas inseridas nas tabelas no esquema {schema_name}')

except Exception as e:
    logging.error(f'Erro durante a execução: {e}', exc_info=True)
    print(f'Erro durante a execução: {e}')
