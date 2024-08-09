from datetime import datetime
import logging

def setup_logger(log_filename: str) -> None:
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def get_table_name(date_str: str) -> str:
    print('Pegando o nome da tabela')
    meses = [
        "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
    ]
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    mes = meses[date_obj.month - 1]
    ano = date_obj.year
    print(f'Nome da tabela:{mes}_{ano}')
    return f"{mes}_{ano}"


def is_past_date(date_str: str) -> bool:
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    print(f'Verifica se a data é diferente da atual para dados retrogrados --- {date_obj < datetime.today().date()}')
    return date_obj < datetime.today().date()
