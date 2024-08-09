import psycopg2
from datetime import datetime
from typing import Optional

def create_monthly_table(cur, schema_name: str, table_name: str) -> None:
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

today = '2024-07-26'

def get_last_detection_time(cur, schema_name: str, table_name: str) -> Optional[datetime.time]:
    cur.execute(f'''
        SELECT hora_deteccao FROM {schema_name}.{table_name}
        WHERE acq_date = %s
        ORDER BY hora_deteccao DESC LIMIT 1
    ''', (datetime.today().date(),))
    result = cur.fetchone()
    if result:
        print(f'Pegando a hora da ultima detecção --- {result[0]} --- {datetime.today()}' )
        return result[0]
    return None

def insert_data_to_db(data_to_insert: dict, schema_name: str, conn) -> None:
    cur = conn.cursor()
    insert_query = f'''
        INSERT INTO {schema_name}.{{}} (
            latitude, longitude, acq_date, acq_time, confidence, scan, daynight,
            version, frp, instrument, satellite, track, hora_deteccao, geom
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4674))
    '''

    for table_name, data in data_to_insert.items():
        cur.executemany(insert_query.format(table_name), data)

    conn.commit()
    cur.close()
