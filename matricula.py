#!/usr/bin/env python

import zipfile
import io
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

import comum

CENSO_ESCOLAR_URL = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar'
PATH_OUT = comum.PATH_MATRICULAS
CERTIFICADO_PATH = 'inep-gov-br-chain.pem'

def unzip(zf): 
    with zipfile.ZipFile(zf, 'r') as z:
        for filename in z.namelist():
            if 'microdados' in filename.lower() and '.csv' in filename.lower() and 'suplemento' not in filename.lower():
                with z.open(filename) as f:
                    df = pd.read_csv(f, sep=';', decimal='.', encoding='windows-1252', low_memory=False)
                    break
    return df

def obter_url_filename(medium, ano):
    r = medium.get(CENSO_ESCOLAR_URL)

    soup = BeautifulSoup(r.text, 'html.parser')

    file_url = [file_url['href'] for file_url in soup.find('div', id='content-core').find_all('a') if str(ano) in file_url.string][0]

    filename = file_url.rsplit('/')[-1]
    return file_url, filename

def tratar_info(full_filename):
    df = unzip(full_filename)
    return df

def obter_dtype(s, df):
    if pd.notna(s.Categoria):
        return 'category'
    elif s.Tipo == 'Char':
        return 'string'
    elif s.Tipo == 'Data':
        return 'datetime'
    elif s.Tipo == 'Num':
        max_ = df[s['Nome da Variável']].max()
        if isinstance(max_, str):
            try:
                max_ = int(max_)
            except ValueError:
                return 'string'
        if max_ >= 2**32:
            return 'UInt64'
        elif max_ >= 2**16:
            return 'UInt32'
        elif max_ >= 2**8:
            return 'UInt16'
        else:
            return 'UInt8'

def otimizar_espaco(df, ano):
    # Dicionário mais recente em 2024-03, tem informações sobre os censo escolares dos anos passados
    with zipfile.ZipFile(f'{PATH_OUT}microdados_censo_escolar_{ano}.zip') as zf:
        for fn in zf.namelist():
            if 'xlsx' in fn and 'dicion' in fn and '~' not in fn:
                df_dict_tmp = pd.read_excel(zf.open(fn), header=None)
    df_dict_tmp = df_dict_tmp[df_dict_tmp[0].notna() &
                              (df_dict_tmp.iloc[:, ano % 2000 - 1] != 'n')].reset_index(drop=True)
    df_dict = df_dict_tmp[df_dict_tmp[0].astype(str).str.isdecimal()]
    header_index = df_dict.index[0] - 1
    df_dict = df_dict.set_axis(df_dict_tmp.iloc[header_index, :], axis=1)
    
    df_dict['dtype'] = df_dict.apply(obter_dtype, axis=1, df=df)

    dtype_dict = {nome: dtype for nome, dtype
                              in df_dict[['Nome da Variável', 'dtype']]
                                         .itertuples(index=False, name=None)
                              if dtype != 'datetime'}

    for col, dtype in dtype_dict.items():
        try:
            df[col] = df[col].astype(dtype)
        except TypeError:
            print(f"TypeError: {col}")
            df[col] = df[col].astype('string')
        except ValueError:
            print(f"ValueError: {col}")
            df[col] = df[col].astype('string')

    if ano == 2023:
        format_ = '%d%b%y:%X'
    elif ano == 2022:
        format_ = '%d%b%Y:%X'
    for col in df.select_dtypes('O').columns:
       df[col] = pd.to_datetime(df[col], format=format_)

    return df

def obter_df(ano, colunas):
    url, filename = obter_url_filename(requests, ano)
    feather_filepath = f'{PATH_OUT}feathers/{ano}matricula.feather' 
    if os.path.isfile(feather_filepath):
        return pd.read_feather(feather_filepath, columns=colunas)

    full_filename = f'{PATH_OUT}{filename}'
    if not os.path.isfile(full_filename):
        print(f'[INFO]: Arquivo {filename} não existente. Fazendo download.')
        info = comum.obter_info(requests, url, CERTIFICADO_PATH)
        print(f'[INFO]: Download concluído. Gravando arquivo em {PATH_OUT}.')
        comum.salvar_info(info.content, full_filename)

    df = tratar_info(full_filename)
    df = otimizar_espaco(df, ano)
    df.to_feather(feather_filepath)

    return pd.read_feather(feather_filepath, columns=colunas)

if __name__ == '__main__':
    df = obter_df(2022, [])
    print(df.head())

