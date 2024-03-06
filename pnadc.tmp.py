import pandas as pd

import comum

COLUNAS_FREQUENCIA = [
        'UF',     # Unidade da Federação
        'V1028',  # Peso com calibração
        'V2009',  # Idade do morador na data de referência
        'V3002A', # A escola que frequenta é #1: privada e #2 pública
        'V3003A', # Qual é o curso que frequenta? #04 EF regular #06 EM regular
        'V3006A', # Qual é a etapa do ensino fundamental que frequenta #1 AI e #2 AF
        'VD3004', # Nível de instrução mais elevado alcançado #3 Fundamental completo e #5 Médio completo
]

df = pd.read_feather(f'{comum.PATH_OUT}/pnadc/feathers/202304pnadc.feather', columns=COLUNAS_FREQUENCIA)

pop_brasil_15a17 = df.loc[df.V2009.between(15, 17), 'V1028'].sum()
pop_brasil_11a14 = df.loc[df.V2009.between(11, 14), 'V1028'].sum()

pop_sp_15a17 = df.loc[df.V2009.between(15, 17) & (df.UF == '35'), 'V1028'].sum()
pop_sp_11a14 = df.loc[df.V2009.between(11, 14) & (df.UF == '35'), 'V1028'].sum()


