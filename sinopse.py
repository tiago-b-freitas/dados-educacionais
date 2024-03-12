#!/usr/bin/env python

import zipfile
import io
import os

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

import comum

SINOPSE_URL  = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/sinopses-estatisticas/educacao-basica'
PATH_OUT = comum.PATH_SINOPSE
CERTIFICADO_PATH = 'inep-gov-br-chain.pem'

def unzip(zf):
    with zipfile.ZipFile(zf, 'r') as z:
        for filename in z.namelist():
            if 'sinopse' in filename.lower() and '.xlsx' in filename.lower(): 
                with z.open(filename) as f:
                    df_dict = pd.read_excel(f, sheet_name=['1.23', '1.28'], header=None)
                    break
    df_dict['1.23'].to_pickle(f'{PATH_OUT}pickles/ef_af.pickle')
    df_dict['1.28'].to_pickle(f'{PATH_OUT}pickles/em.pickle')

def obter_url_filename(medium, ano):
    r = medium.get(SINOPSE_URL)

    soup = BeautifulSoup(r.text, 'html.parser')

    file_url = [file_url['href'] for file_url in soup.find('div', id='parent-fieldname-text').find_all('a')
                if file_url.string is not None and str(ano) in file_url.string][0]

    filename = file_url.rsplit('/')[-1]
    return file_url, filename

def tratar_df(df, tipo):
    df.columns = ['0', 'NO_UF', 'NO_MUNICIPIO', 'CO_MUNICIPIO', f'QT_MAT_{tipo}'] + df.iloc[7, 5:].tolist()
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'^\s+$', lambda e: pd.NA, regex=True).str.strip()
    df = df.iloc[9:5612].reset_index(drop=True).copy()
    df.iloc[:, 2] = df.iloc[:, 2].fillna(df.iloc[:, 1]).fillna(df.iloc[:, 0])

    return df

def tratar_info(full_filename):
    if not os.path.isfile(f'{PATH_OUT}pickles/ef_af.pickle'):
        unzip(full_filename)

    df_EF_AF = tratar_df(pd.read_pickle(f'{PATH_OUT}pickles/ef_af.pickle'), 'EF_AF')
    df_EM = tratar_df(pd.read_pickle(f'{PATH_OUT}pickles/em.pickle'), 'EM')

    COLUNAS_GEO = ['NO_UF', 'NO_MUNICIPIO', 'CO_MUNICIPIO']
    df_EF_AF = df_EF_AF[COLUNAS_GEO + ['QT_MAT_EF_AF', '11 a 14 anos']].set_index(COLUNAS_GEO).rename(columns={'11 a 14 anos': 'QT_MAT_IDADE_ADEQUADA_EF_AF'})
    df_EM = df_EM[COLUNAS_GEO + ['QT_MAT_EM', '15 a 17 anos']].set_index(COLUNAS_GEO).rename(columns={'15 a 17 anos': 'QT_MAT_IDADE_ADEQUADA_EM'})
    df = pd.concat([df_EF_AF, df_EM], axis=1).reset_index()
    for col in df.iloc[:, 2:].columns:
        df[col] = pd.to_numeric(df[col]).astype('UInt32')

    return df

def obter_df(ano):
    pickle_filepath = f'{PATH_OUT}pickles/{ano}sinopse.pickle' 
    if os.path.isfile(pickle_filepath):
        return pd.read_pickle(pickle_filepath)

    url, filename = obter_url_filename(requests, ano)
    full_filename = f'{PATH_OUT}{filename}'
    if not os.path.isfile(full_filename):
        print(f'[INFO]: Arquivo {filename} não existente. Fazendo download.')
        info = comum.obter_info(requests, url, CERTIFICADO_PATH)
        print(f'[INFO]: Download concluído. Gravando arquivo em {PATH_OUT}.')
        comum.salvar_info(info.content, full_filename)

    df = tratar_info(full_filename)
    df.to_pickle(pickle_filepath)

    return pd.read_pickle(pickle_filepath)

if __name__ == '__main__':
    df = obter_df(2023)
    print(df.head())

