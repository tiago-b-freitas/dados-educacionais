import collections
import os

import pandas as pd
import requests

import comum

# fonte: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
URL = 'https://servicodados.ibge.gov.br/api/v3/agregados/9514/periodos/2022/variaveis/93?localidades=N1[all]|N6[N3[35]]|N3[35]&classificacao=2[6794]|287[6563,6564,6565,6566,6567,6568,6569,6570,6571,6572,6573,6574]|286[113635]'
PATH_OUT = comum.PATH_POPULACAO
FILENAME = '2023-censo-pop.spickle'
def obter_df():
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
            valor = int(row['serie']['2022'])

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
    df = obter_df()
    print(df)
