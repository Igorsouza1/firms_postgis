import logging
from datetime import datetime

def setup_logger(log_filename: str) -> None:
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def get_table_name(date_str: str) -> str:
    meses = [
        "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
    ]
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    mes = meses[date_obj.month - 1]
    ano = date_obj.year
    return f"{mes}_{ano}"

def convert_acq_time(acq_time: int) -> str:
    hours = int(acq_time) // 100
    minutes = int(acq_time) % 100
    return f"{hours:02d}:{minutes:02d}:00"
