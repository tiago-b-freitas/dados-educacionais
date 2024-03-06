import collections
import os

import pandas as pd
import requests

import comum

# fonte: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
PATH_OUT = comum.PATH_POPULACAO
def obter_df(ano):
    URL = f'https://servicodados.ibge.gov.br/api/v3/agregados/9514/periodos/{ano}/variaveis/93?localidades=N1[all]|N6[N3[35]]|N3[35]&classificacao=2[6794]|287[6563,6564,6565,6566,6567,6568,6569,6570,6571,6572,6573,6574]|286[113635]'
    FILENAME = f'{ano}-censo-pop.pickle'
    pickle_filepath = f'{PATH_OUT}pickles/{FILENAME}'
    if os.path.isfile(pickle_filepath):
        return pd.read_pickle(pickle_filepath)
        
    r = requests.get(URL)
    dados = r.json()[0]['resultados']

    df_dict = collections.defaultdict(list)

    for dado in dados:
        idade = list(dado['classificacoes'][1]['categoria'].values())[0]
        idade = int(idade.replace(' anos', ''))
        for row in dado['series']:
            cod   = int(row['localidade']['id'])
            nome  = row['localidade']['nome']
            nome  = nome.replace(' - SP', '')
            valor = int(row['serie'][str(ano)])

            df_dict['CO_MUNICIPIO'].append(cod)
            df_dict['NO_MUNICIPIO'].append(nome)
            df_dict['col_idade'].append(idade)
            df_dict['valor'].append(valor)

    df = pd.DataFrame(df_dict)
    df = df.pivot(columns='col_idade', index=['CO_MUNICIPIO', 'NO_MUNICIPIO'], values='valor')
    for col in df.columns:
        df[col] = df[col].astype('UInt64')
    df.to_pickle(pickle_filepath)

    return df

if __name__ == '__main__':
    df = obter_df(2022)
    print(df)
