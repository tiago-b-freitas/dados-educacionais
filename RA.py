import io
import os

import pandas as pd
import requests

import comum

RA_FILENAME = 'Seade-Regionalizacoes-SP.csv'
RA_PATH_OUT = comum.PATH_RA + RA_FILENAME

def obter_info():
    '''
    Fonte: http://produtos.seade.gov.br/produtos/divpolitica/
    '''

    url_inicial = 'http://produtos.seade.gov.br/produtos/divpolitica/'
    url = 'http://produtos.seade.gov.br/produtos/divpolitica/index.php'
    url_csv = 'http://produtos.seade.gov.br/produtos/divpolitica/csv.php'
    
    with requests.Session() as s:
        s.get(url_inicial)
        r = s.post(url, params={'page': 'tabela', 'action': 'load', 'nivel': 70})
        r = s.post(url_csv, params={'cod': -7, 'nivel': 70})
    return r

def tratar_info():
    with open(RA_PATH_OUT) as f:
        raw_file = f.read()

    lines = []
    for line in raw_file.split('\n'):
        line = line.strip()
        elems = line.split(';')
        if not line or len(elems) != 7:
            continue
        
        line = ';'.join(elem.strip('"') for elem in elems[:-1])
        lines.append(line)

    csv_buf = io.StringIO('\n'.join(lines))
    df = pd.read_csv(csv_buf, sep=';', na_values='-')
    return df

def obter_df():
    if not os.path.isfile(RA_PATH_OUT):
        print(f'[INFO]: Arquivo {RA_FILENAME} não existente. Fazendo download.')
        info = obter_info()
        print(f'[INFO]: Download concluído. Gravando arquivo em {RA_PATH_OUT}.')
        comum.salvar_info(info.text, RA_PATH_OUT)
    df = tratar_info()
    return df

if __name__ == '__main__':
    print(obter_df())

