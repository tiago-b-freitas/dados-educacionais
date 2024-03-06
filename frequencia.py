#!/usr/bin/env python

import collections
import pathlib
import zipfile
import re
import io
import os

import pandas as pd
import requests

import comum

URL = 'http://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/{ano}/'
PATH_OUT = comum.PATH_FREQUENCIA

def criar_dicionario():
    df_dict = collections.defaultdict(list)
    pat = re.compile(r'\s+')
    path = f'{PATH_OUT}Dicionario_e_input_20221031.txt'
    if not os.path.isfile(path):
        r = requests.get(URL.format(ano='Documentacao')+'Dicionario_e_input_20221031.zip')   
        bf = io.BytesIO(r.content)
        with zipfile.ZipFile(bf) as zf:
            with zf.open('input_PNADC_trimestral.txt') as fin:
                with open(path, 'wb') as fout:
                    fout.write(fin.read())

    with open(path, encoding='latin-1') as f:
        for line in f.readlines():
            if not line.startswith('@'):
                continue
            
            posicao, codigo, tipo, descricao = pat.split(line, maxsplit=3)
            
            posicao = int(posicao[1:]) - 1
            descricao = descricao.strip('/*/ ')
            if descricao.startswith('Peso REPLICADO'):
                continue
            if tipo[0] == '$':
                tamanho = int(tipo[1:-1])
                tipo = 'category'
                
            else:
                tamanho = int(tipo[:-1])
                tipo = ''
            
            df_dict['posicao'].append(posicao)
            df_dict['codigo'].append(codigo)
            df_dict['tipo'].append(tipo)
            df_dict['tamanho'].append(tamanho)
            df_dict['descricao'].append(descricao)

    df_dict = pd.DataFrame(df_dict)
    return df_dict

def tratar_info(full_filename):
    df_dict = criar_dicionario()
    colspecs = [(posicao, posicao+tamanho) for posicao, tamanho in
             df_dict[['posicao', 'tamanho']].itertuples(index=False, name=None)]

    dtypes = {codigo: tipo for codigo, tipo in
              df_dict[['codigo', 'tipo']].itertuples(index=False, name=None)
              if tipo}
    with zipfile.ZipFile(full_filename) as zf:
        with zf.open(pathlib.Path(full_filename).stem + '.txt') as f:
            df = pd.read_fwf(f, names=df_dict.codigo, colspecs=colspecs, dtype=dtypes)
            for col in df.select_dtypes('float'):
                df[col] = pd.to_numeric(df[col], downcast='float')
            for col in df.select_dtypes('int'):
                df[col] = pd.to_numeric(df[col], downcast='unsigned')

    return df

def obter_df(ano, trimestre, colunas=None):
    if colunas is None:
        colunas = []
    feather_filepath = f'{PATH_OUT}feathers/{ano}{trimestre}frequencia.feather' 
    if os.path.isfile(feather_filepath):
        return pd.read_feather(feather_filepath, columns=colunas)
    
    filename = f'PNADC_{trimestre:02}{ano}.zip'
    full_filename = f'{PATH_OUT}{filename}'
    if not os.path.isfile(full_filename):
        print(f'[INFO]: Arquivo {filename} não existente. Fazendo download.')
        info = comum.obter_info(requests, URL.format(ano=ano)+filename)
        print(f'[INFO]: Download concluído. Gravando arquivo em {PATH_OUT}.')
        comum.salvar_info(info.content, full_filename)

    df = tratar_info(full_filename)
    df.to_feather(feather_filepath)

    return pd.read_feather(feather_filepath, columns=colunas)

if __name__ == '__main__':
    df = obter_df(2023, 4)
    print(df.head())

