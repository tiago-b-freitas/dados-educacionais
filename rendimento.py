#!/usr/bin/env python

from enum import Enum
import zipfile
import io
import os
import glob

import pandas as pd
import requests
from bs4 import BeautifulSoup

import comum

RENDIMENTO_ESCOLAR_URL = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-rendimento-escolar'

PATH_OUT = comum.PATH_RENDIMENTO
CERTIFICADO_PATH = 'inep-gov-br-chain.pem'

colunas_brasil = [
    'NU_ANO_CENSO', 'UNIDGEO', 'NO_CATEGORIA', 'NO_DEPENDENCIA',   
    
    'APROVACAO_EF',    'APROVACAO_EF_AI', 'APROVACAO_EF_AF',
    'APROVACAO_EF_01', 'APROVACAO_EF_02', 'APROVACAO_EF_03',
    'APROVACAO_EF_04', 'APROVACAO_EF_05', 'APROVACAO_EF_06',
    'APROVACAO_EF_07', 'APROVACAO_EF_08', 'APROVACAO_EF_09',
    'APROVACAO_EM',    'APROVACAO_EM_01', 'APROVACAO_EM_02',
    'APROVACAO_EM_03', 'APROVACAO_EM_04', 'APROVACAO_EM_NS',

    'REPROVACAO_EF',    'REPROVACAO_EF_AI', 'REPROVACAO_EF_AF',
    'REPROVACAO_EF_01', 'REPROVACAO_EF_02', 'REPROVACAO_EF_03',
    'REPROVACAO_EF_04', 'REPROVACAO_EF_05', 'REPROVACAO_EF_06',
    'REPROVACAO_EF_07', 'REPROVACAO_EF_08', 'REPROVACAO_EF_09',
    'REPROVACAO_EM',    'REPROVACAO_EM_01', 'REPROVACAO_EM_02',
    'REPROVACAO_EM_03', 'REPROVACAO_EM_04', 'REPROVACAO_EM_NS',

    'ABANDONO_EF',    'ABANDONO_EF_AI', 'ABANDONO_EF_AF',
    'ABANDONO_EF_01', 'ABANDONO_EF_02', 'ABANDONO_EF_03',
    'ABANDONO_EF_04', 'ABANDONO_EF_05', 'ABANDONO_EF_06',
    'ABANDONO_EF_07', 'ABANDONO_EF_08', 'ABANDONO_EF_09',
    'ABANDONO_EM',    'ABANDONO_EM_01', 'ABANDONO_EM_02',
    'ABANDONO_EM_03', 'ABANDONO_EM_04', 'ABANDONO_EM_NS',
]

colunas_municipio = [
    'NU_ANO_CENSO', 'NO_REGIAO', 'SG_UF', 'CO_MUNICIPIO',
    'NO_MUNICIPIO', 'NO_CATEGORIA', 'NO_DEPENDENCIA',

    'APROVACAO_EF',    'APROVACAO_EF_AI', 'APROVACAO_EF_AF',
    'APROVACAO_EF_01', 'APROVACAO_EF_02', 'APROVACAO_EF_03',
    'APROVACAO_EF_04', 'APROVACAO_EF_05', 'APROVACAO_EF_06',
    'APROVACAO_EF_07', 'APROVACAO_EF_08', 'APROVACAO_EF_09',
    'APROVACAO_EM',    'APROVACAO_EM_01', 'APROVACAO_EM_02',
    'APROVACAO_EM_03', 'APROVACAO_EM_04', 'APROVACAO_EM_NS',

    'REPROVACAO_EF',    'REPROVACAO_EF_AI', 'REPROVACAO_EF_AF',
    'REPROVACAO_EF_01', 'REPROVACAO_EF_02', 'REPROVACAO_EF_03',
    'REPROVACAO_EF_04', 'REPROVACAO_EF_05', 'REPROVACAO_EF_06',
    'REPROVACAO_EF_07', 'REPROVACAO_EF_08', 'REPROVACAO_EF_09',
    'REPROVACAO_EM',    'REPROVACAO_EM_01', 'REPROVACAO_EM_02',
    'REPROVACAO_EM_03', 'REPROVACAO_EM_04', 'REPROVACAO_EM_NS',

    'ABANDONO_EF',    'ABANDONO_EF_AI', 'ABANDONO_EF_AF',
    'ABANDONO_EF_01', 'ABANDONO_EF_02', 'ABANDONO_EF_03',
    'ABANDONO_EF_04', 'ABANDONO_EF_05', 'ABANDONO_EF_06',
    'ABANDONO_EF_07', 'ABANDONO_EF_08', 'ABANDONO_EF_09',
    'ABANDONO_EM',    'ABANDONO_EM_01', 'ABANDONO_EM_02',
    'ABANDONO_EM_03', 'ABANDONO_EM_04', 'ABANDONO_EM_NS',
]

class Tipo(Enum):
    BRASIL    = 'brasil'
    MUNICIPIO = 'municipio'
    ESCOLA    = 'escola'

def unzip(zf):
    with zipfile.ZipFile(zf, 'r') as z:
        for filename in z.namelist():
            if 'xls' in filename.lower():
                with z.open(filename) as f:
                    excelfile = io.BytesIO(f.read())    
                    df = pd.read_excel(excelfile, na_values='--')
                break
    return df

def obter_url_filename(medium, ano, tipo):
    r = medium.get(f'{RENDIMENTO_ESCOLAR_URL}/{ano}') 
    soup = BeautifulSoup(r.text, 'html.parser')

    file_urls = [a['href'] for a in soup.find('div', id='parent-fieldname-text').find_all('a')]
    
    for file_url in file_urls:
        if file_url.lower().find(tipo.value) != -1:
            break

    filename = file_url.rsplit('/')[-1]
    return file_url, filename

def tratar_info(filename, tipo):
    df = unzip(filename)

    df = df.iloc[8:-2]
    match tipo:
        case Tipo.BRASIL:
            df.columns = colunas_brasil
        case Tipo.MUNICIPIO:
            df.columns = colunas_municipio
        case Tipo.ESCOLA:
            pass

    for col in df.columns[df.columns.str.startswith(('APROVACAO', 'REPROVACAO', 'ABANDONO'))]:
        df[col] = df[col].astype('Float32')

    return df
 
def obter_df(ano, tipo):
    url, filename = obter_url_filename(requests, ano, tipo)
    pickle_filepath = f'{PATH_OUT}pickles/{ano}rendimento{tipo.value}.pickle' 
    if os.path.isfile(pickle_filepath):
        return pd.read_pickle(pickle_filepath)
    full_filename = f'{PATH_OUT}{filename}'
    if not os.path.isfile(full_filename):
        print(f'[INFO]: Arquivo {filename} não existente. Fazendo download.')
        info = comum.obter_info(requests, url, CERTIFICADO_PATH)
        print(f'[INFO]: Download concluído. Gravando arquivo em {PATH_OUT}.')
        comum.salvar_info(info.content, full_filename)
    df = tratar_info(full_filename, tipo)
    df.to_pickle(pickle_filepath)
    return df
 
if __name__ == '__main__':
    df = obter_df(2022, Tipo.MUNICIPIO)

