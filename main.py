import sys
import os
from datetime import datetime
import pandas as pd
import psycopg2
from config.config import load_env_variables
from utils.utils import setup_logger, get_table_name
from data.data_processing import download_csv, process_csv_to_gdf, filter_gdf_by_geojson
from data.database import create_monthly_table, get_last_detection_time, insert_data_to_db
import logging

def main():
    geojson_file = sys.argv[1]
    log_filename = sys.argv[2]

    setup_logger(log_filename)

    try:

        env_vars = load_env_variables()
        POSTGRES_CONN = {
            'dbname': env_vars['dbname'],
            'user': env_vars['user'],
            'password': env_vars['password'],
            'host': env_vars['host'],
            'port': env_vars['port']
        }

        schema_name = env_vars['schema_name']
        tabela_trabalhada = "agosto_2024"
        today = datetime.today().strftime('%Y-%m-%d')
        url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/70e04b8239b80f583c2af0f373766037/VIIRS_NOAA20_NRT/BRA/1/{today}"
        csv_filename = "firms_data.csv"


        download_csv(url, csv_filename)



        firms_gdf = process_csv_to_gdf(csv_filename)
        filtered_gdf = filter_gdf_by_geojson(firms_gdf, geojson_file)

        filtered_gdf['acq_date'] = pd.to_datetime(filtered_gdf['acq_date'], format='%Y-%m-%d').dt.date
        filtered_gdf['hora_deteccao'] = pd.to_datetime(filtered_gdf['hora_deteccao'], format='%H:%M:%S').dt.time

        conn = psycopg2.connect(**POSTGRES_CONN)
        cur = conn.cursor()

        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
        last_detection_time = get_last_detection_time(cur, schema_name, tabela_trabalhada)
        logging.info(f'Última hora de detecção no banco de dados: {last_detection_time}')

        if last_detection_time:
            filtered_gdf = filtered_gdf[filtered_gdf['hora_deteccao'] > last_detection_time]
            logging.info(f'Dados filtrados a partir de {last_detection_time}, total de linhas: {len(filtered_gdf)}')

        data_to_insert = {}
        for _, row in filtered_gdf.iterrows():
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

        insert_data_to_db(data_to_insert, schema_name, conn)
        conn.close()

        os.remove(csv_filename)

        total_inserido = sum(len(data) for data in data_to_insert.values())
        logging.info(f'{total_inserido} linhas inseridas nas tabelas no esquema {schema_name}')
        print(f'{total_inserido} linhas inseridas nas tabelas no esquema {schema_name}')

    except Exception as e:
        logging.error(f'Erro durante a execução: {e}', exc_info=True)
        print(f'Erro durante a execução: {e}')

if __name__ == '__main__':
    main()
