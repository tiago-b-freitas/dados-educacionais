#!/usr/bin/env python

import pandas as pd

import RA
import rendimento
import matricula
import populacao
import sinopse

ANO_MATRICULA  = 2023
ANO_RENDIMENTO = 2022
TRI_FREQUENCIA = 4
ANO_FREQUENCIA = 2023
ANO_SINOPSE    = 2023
ANO_POPULACAO  = 2022

COLUNAS_MAT = ['QT_MAT_MED', 'QT_MAT_FUND_AF', 'SG_UF', 'TP_DEPENDENCIA',
               'TP_SITUACAO_FUNCIONAMENTO', 'CO_MUNICIPIO', 'NO_MUNICIPIO']
COLUNAS_MAT_QT = ['QT_MAT_EF_AF', 'QT_MAT_EM',]
COLUNAS_MAT_QT_IDADE = ['QT_MAT_EF_AF_11_14_ANOS', 'QT_MAT_EM_15_17_ANOS']
COLUNAS_REN_TAXA = ['APROVACAO_EF_AF', 'APROVACAO_EM', 'REPROVACAO_EF_AF',
                    'REPROVACAO_EM', 'ABANDONO_EF_AF', 'ABANDONO_EM']
COLUNAS_MUN = ['CO_MUNICIPIO', 'NO_MUNICIPIO']

def padronizar_mat(df):
    df = df.rename(columns={'QT_MAT_MED': 'QT_MAT_EM',
                            'QT_MAT_FUND_AF': 'QT_MAT_EF_AF'}) 
    df.TP_DEPENDENCIA = df.TP_DEPENDENCIA.map({1: 'Federal',
                                               2: 'Estadual',
                                               3: 'Municipal',
                                               4: 'Privada'})
    return df

def padronizar_ren(df):
    df = df.rename(columns={'NO_DEPENDENCIA': 'TP_DEPENDENCIA'})
    return df

def padronizar_RA(df):
    df.columns = [
        'CO_MUNICIPIO', # Código do município
        'NO_MUNICIPIO', # Nome do Município
        'NO_RA',        # Nome da Região Administrativa
        'NO_RG',        # Nome da Região de Governo
        'NO_RM',        # Nome da Região Metropolitana
        'NO_AU']        # Nome das Aglomerações Urbanas
    
    df.NO_RA = df.NO_RA.fillna(df.NO_RM)
    df = df[['CO_MUNICIPIO', 'NO_MUNICIPIO', 'NO_RA']].copy()
    df.CO_MUNICIPIO = df.CO_MUNICIPIO.astype('O')
    return df


def montar_tabela_brasil_uf():
    # Dados de matrícula 
    df = df_mat_brasil.groupby('TP_DEPENDENCIA')[COLUNAS_MAT_QT].sum().reset_index()
    df['UNIDGEO'] = 'Brasil'

    df_tmp = df_mat_brasil[COLUNAS_MAT_QT].sum()
    df_tmp['TP_DEPENDENCIA'] = 'Total'
    df_tmp['UNIDGEO'] = 'Brasil'
    df = pd.concat([df, df_tmp.to_frame().T], ignore_index=True)
    
    df_tmp = df_mat_brasil.loc[df_mat_brasil.TP_DEPENDENCIA != 'Privada', COLUNAS_MAT_QT].sum()
    df_tmp['TP_DEPENDENCIA'] = 'Pública'
    df_tmp['UNIDGEO'] = 'Brasil'
    df = pd.concat([df, df_tmp.to_frame().T], ignore_index=True)

    df_tmp = df_mat_sp.groupby('TP_DEPENDENCIA')[COLUNAS_MAT_QT].sum().reset_index()
    df_tmp['UNIDGEO'] = 'São Paulo'
    df = pd.concat([df, df_tmp], ignore_index=True)
    
    df_tmp = df_mat_sp[COLUNAS_MAT_QT].sum()
    df_tmp['TP_DEPENDENCIA'] = 'Total'
    df_tmp['UNIDGEO'] = 'São Paulo'
    df = pd.concat([df, df_tmp.to_frame().T], ignore_index=True)

    df_tmp = df_mat_sp.loc[df_mat_sp.TP_DEPENDENCIA != 'Privada', COLUNAS_MAT_QT].sum()
    df_tmp['TP_DEPENDENCIA'] = 'Pública'
    df_tmp['UNIDGEO'] = 'São Paulo'
    df = pd.concat([df, df_tmp.to_frame().T], ignore_index=True)

    df = df.set_index(keys=['UNIDGEO', 'TP_DEPENDENCIA'])

    # Dados de rendimento
    df_tmp = df_ren_brasil.loc[df_ren_brasil.UNIDGEO.isin(['Brasil', 'São Paulo']) & (df_ren_brasil.NO_CATEGORIA == 'Total')]
    df_tmp = df_tmp[['UNIDGEO', 'TP_DEPENDENCIA'] + COLUNAS_REN_TAXA].set_index(keys=['UNIDGEO', 'TP_DEPENDENCIA'])

    df = pd.concat([df, df_tmp], axis=1)

    return df


def montar_tabela_RA():
    df_RA = padronizar_RA(RA.obter_df())

    # Dados de matrícula
    df = df_RA.merge(df_mat_sp_mun_est).groupby('NO_RA')[COLUNAS_MAT_QT].sum()

    # Dados de rendimento
    df_ren_tmp =  df_ren_mun.loc[(df_ren_mun.SG_UF == 'SP') &
                                 (df_ren_mun.NO_CATEGORIA == 'Total') &
                                 (df_ren_mun.TP_DEPENDENCIA == 'Estadual')]

    df_ren_tmp = df_ren_tmp[COLUNAS_MUN + COLUNAS_REN_TAXA]

    df_mat_tmp = padronizar_mat(matricula.obter_df(ANO_RENDIMENTO, colunas=COLUNAS_MAT))
    df_mat_tmp = df_mat_tmp[(df_mat_tmp.SG_UF == 'SP') &
                            (df_mat_tmp.TP_DEPENDENCIA == 'Estadual') &
                            (df_mat_tmp.TP_SITUACAO_FUNCIONAMENTO == 1)]

    df_mat_tmp = df_mat_tmp.groupby('CO_MUNICIPIO')[COLUNAS_MAT_QT].sum().reset_index()

    df_tmp = df_RA.merge(df_ren_tmp).merge(df_mat_tmp)
    g = df_tmp.groupby('NO_RA')

    cols = [] 
    for tipo_rendimento in ('APROVACAO', 'REPROVACAO', 'ABANDONO'):
        for etapa in ('EM', 'EF_AF'):
            col = f'{tipo_rendimento}_{etapa}'
            cols.append(g.apply(lambda g: (g[f'QT_MAT_{etapa}'] * g[col]).sum()
                                / g[f'QT_MAT_{etapa}'].sum()).to_frame(f'{tipo_rendimento}_{etapa}'))

    df = pd.concat([df, *cols], axis=1)
    return df


def montar_tabela_municipio():
    # Dados de matrícula da rede estadual
    df     = df_mat_sp_mun.set_index(COLUNAS_MUN)
    df_est = df_mat_sp_mun_est.set_index(COLUNAS_MUN).rename(columns={col: col+'_EST' for col in COLUNAS_MAT_QT})
    df     = pd.concat([df_pop_EF_AF.iloc[2:], df_pop_EM.iloc[2:], df, df_est, df_mat_sp_11a17_mun], axis=1)

    # Dados de escolarização bruta
    df['TX_ESC_BRUTA_EF_AF']     = df.QT_MAT_EF_AF.div(df.POP_11_14ANOS)
    df['TX_ESC_BRUTA_EM']        = df.QT_MAT_EM.div(df.POP_15_17ANOS)
    df['TX_ESC_BRUTA_EF_AF_EST'] = df.QT_MAT_EF_AF_EST.div(df.POP_11_14ANOS)
    df['TX_ESC_BRUTA_EM_EST']    = df.QT_MAT_EM_EST.div(df.POP_15_17ANOS)

    # Dados de escolarização líquida
    df['TX_ESC_LIQUIDA_EF_AF']     = df.QT_MAT_EF_AF_11_14_ANOS.div(df.POP_11_14ANOS)
    df['TX_ESC_LIQUIDA_EM']        = df.QT_MAT_EM_15_17_ANOS.div(df.POP_15_17ANOS)

    # Dados de rendimento
    df_ren_tmp =  df_ren_mun.loc[(df_ren_mun.SG_UF == 'SP') &
                                 (df_ren_mun.NO_CATEGORIA == 'Total') &
                                 (df_ren_mun.TP_DEPENDENCIA == 'Total')]
    df_ren_tmp = df_ren_tmp[COLUNAS_MUN + COLUNAS_REN_TAXA].set_index(COLUNAS_MUN)
    df = pd.concat([df, df_ren_tmp], axis=1)

    df_ren_tmp =  df_ren_mun.loc[(df_ren_mun.SG_UF == 'SP') &
                                 (df_ren_mun.NO_CATEGORIA == 'Total') &
                                 (df_ren_mun.TP_DEPENDENCIA == 'Estadual')]
    df_ren_tmp = df_ren_tmp[COLUNAS_MUN + COLUNAS_REN_TAXA].set_index(COLUNAS_MUN)
    df_ren_tmp = df_ren_tmp.rename(columns={col: col+'_EST' for col in COLUNAS_REN_TAXA})
    df = pd.concat([df, df_ren_tmp], axis=1)

    return df

if __name__ == '__main__':
    
    df_mat = padronizar_mat(matricula.obter_df(ANO_MATRICULA, colunas=COLUNAS_MAT))

    df_mat_brasil = df_mat[df_mat.TP_SITUACAO_FUNCIONAMENTO == 1]
    df_mat_sp = df_mat_brasil[df_mat_brasil.SG_UF == 'SP']
    df_mat_sp_mun = df_mat_sp.groupby(COLUNAS_MUN)[COLUNAS_MAT_QT].sum().reset_index()
    df_mat_sp_mun_est = (df_mat_sp[df_mat_sp.TP_DEPENDENCIA == 'Estadual']
                         .groupby(COLUNAS_MUN)[COLUNAS_MAT_QT].sum().reset_index())

    df_ren_brasil = padronizar_ren(rendimento.obter_df(ANO_RENDIMENTO, rendimento.Tipo.BRASIL))
    df_ren_mun = padronizar_ren(rendimento.obter_df(ANO_RENDIMENTO, rendimento.Tipo.MUNICIPIO))

    df_populacao = populacao.obter_df(ANO_POPULACAO)
    df_pop_EF_AF = df_populacao.loc[:, 11:14].sum(axis=1).to_frame('POP_11_14ANOS').astype('UInt64')
    df_pop_EM    = df_populacao.loc[:, 15:17].sum(axis=1).to_frame('POP_15_17ANOS').astype('UInt64')

    df_sinopse = sinopse.obter_df(ANO_SINOPSE)
    
    df_mat_brasil_11a17  = df_sinopse.loc[df_sinopse.NO_MUNICIPIO == 'Brasil', COLUNAS_MAT_QT_IDADE]
    df_mat_sp_11a17      = df_sinopse.loc[df_sinopse.NO_MUNICIPIO == 'São Paulo', COLUNAS_MAT_QT_IDADE] 
    df_mat_sp_11a17_mun  = df_sinopse.loc[(df_sinopse.NO_UF == 'São Paulo') & df_sinopse.CO_MUNICIPIO.notna(), COLUNAS_MUN + COLUNAS_MAT_QT_IDADE].set_index(COLUNAS_MUN)

    df = montar_tabela_municipio()
    df.to_excel('tabela_municipios.xlsx')
