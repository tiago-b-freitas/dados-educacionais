from collections import namedtuple, defaultdict

import pandas as pd

TAMANHO_PADRAO = 14
HEADER_PADRAO = 'h1'

Coluna = namedtuple('Coluna', ['nome', 'titulo', 'tamanho', 'estilo'],
                    defaults=['', '', TAMANHO_PADRAO, HEADER_PADRAO])

def contar_rec(est):
    if isinstance(est, list):
        return len(est)
    elif isinstance(est, dict):
        return sum(contar_rec(est[e]) for e in est)
    else:
        return 1

def ordenar_colunas(estrutura, sufixo=''):
    if isinstance(estrutura, list):
        return [val.nome+sufixo for val in estrutura]
    ordem_colunas = []
    for val in estrutura:
        ordem_colunas.extend(ordenar_colunas(estrutura[val], sufixo+'_'+val.nome if val.nome else sufixo))
    return ordem_colunas

#UN_GEO = Coluna(titulo='Unidade Geográfica')
#RA_    = Coluna(nome='NO_RA', titulo='Região Administrativa', tamanho=50, estilo='h_left')
#NO_MUN = Coluna(titulo='Município')
#CO_MUN = Coluna(titulo='Código do Município')
#TP_DEP = Coluna(nome='NO_DEPENDENCIA', titulo='Dependência Administrativa')
#EF_AF  = Coluna(nome='EF_AF', titulo='Ensino Fundamental – Anos Finais' )
#EM     = Coluna(nome='EM', titulo='Ensino Médio')
#DEP_TO = Coluna(nome='', titulo='Total')
#DEP_ET = Coluna(nome='EST', titulo='Rede Estadual')
#QT_MAT = Coluna(nome='QT_MAT', titulo='Número de Matrículas', estilo='int')
#QT_EST = Coluna(nome=f'{QT_MAT.nome}_EST', titulo=f'{QT_MAT.titulo} da Rede Estadual', estilo='int')
#QT_IDA = Coluna(nome=f'{QT_MAT.nome}_IDADE_ADEQUADA', titulo=f'{QT_MAT.titulo} na Faixa Etária Adequada', estilo='int')
#TX_LIQ = Coluna(nome='TAXA_LIQUIDA', titulo='Taxa de Escolarização Líquida', estilo='%')
#TX_BRU = Coluna(nome='TAXA_BRUTA', titulo='Taxa de Escolarização Bruta', estilo='%')
#TX_APR = Coluna(nome='APROVACAO', titulo='Taxa de Aprovação', estilo='%')
#TX_REP = Coluna(nome='REPROVACAO', titulo='Taxa de Reprovação', estilo='%')
#TX_ABN = Coluna(nome='ABANDONO', titulo='Taxa de Abandono', estilo='%')

#estrutura = {
    #EF_AF: {DEP_TO: [QT_MAT, QT_IDA, TX_APR, TX_REP, TX_ABN],
            #DEP_ET: [QT_MAT, TX_APR, TX_REP, TX_ABN]},
    #EM:    {DEP_TO: [QT_MAT, QT_IDA, TX_APR, TX_REP, TX_ABN],
            #DEP_ET: [QT_MAT, TX_APR, TX_REP, TX_ABN]}
#}

def contar_header(estrutura, n=1):
    if isinstance(estrutura, list):
        return n
    for val in estrutura:
        return contar_header(estrutura[val], n+1)

def parse_estrutura(worksheet, estrutura, coluna_offset, estilos, linha_inicial=0):
    col_nivel0 = coluna_offset
    col_nivel1 = coluna_offset
    
    if isinstance(estrutura, list):
        col = coluna_offset
        for val in estrutura:
            worksheet.write(linha_inicial,
                            col,
                            val.titulo,
                            estilos[HEADER_PADRAO])
            worksheet.set_column(col, col, val.tamanho, estilos[val.estilo])
            col += 1
        return col

    for val in estrutura:
        n_colunas = contar_rec(estrutura[val]) 
        col_nivel0_futura = col_nivel0 + n_colunas
        worksheet.merge_range(linha_inicial,
                              col_nivel0,
                              linha_inicial,
                              col_nivel0_futura - 1,
                              val.titulo,
                              estilos[val.estilo])
        col_nivel0 = col_nivel0_futura
        col_nivel1 = parse_estrutura(worksheet,
                        estrutura[val],
                        col_nivel1,
                        estilos,
                        linha_inicial=linha_inicial+1)
    return col_nivel1

def setup(workbook, titulo, autor):

    workbook.set_properties(
        {
            'title': titulo,
            'author': autor,
        }
    )

    # https://xlsxwriter.readthedocs.io/format.html
    normal = {'font_name': 'Arial', 'font_size': 10}
    header = {'text_wrap': True, 'valign': 'vcenter', 'align': 'center', 'bold': True}
    format_white = workbook.add_format({'bg_color': 'white', 'pattern': 1})
    format_int  = workbook.add_format({**normal, 'num_format': '#,##0'})
    format_perc = workbook.add_format({**normal, 'num_format': '0.0%'})
    format_header = workbook.add_format({**normal,
                                         **header})
    format_h1 = workbook.add_format({**normal,
                                     **header,
                                     'bottom': True,
                                     'left': True})
    format_hleft = workbook.add_format({**normal,
                                        **header,
                                        'text_wrap': False,
                                        'align': 'left'})
    format_hcenter = workbook.add_format({**normal,
                                        **header,
                                        'text_wrap': False,
                                        'align': 'center'})
    format_top = workbook.add_format({**normal, 'top': True})
    format_fonte = workbook.add_format({**normal, 'top': True, 'font_size': 8})

    estilos = {'int': format_int,
               '%':   format_perc,
               'header': format_header,
               'h1': format_h1,
               'h_left': format_hleft,
               'h_center': format_hcenter,
               'top': format_top,
               'fonte': format_fonte,
               'white': format_white}

    return estilos

def criar_worksheet(writer, df, estrutura, estrutura_header, sheet_name, fonte, estilos):

    ordem_colunas = ordenar_colunas(estrutura)
    df = df.reset_index()[[v.nome for v in estrutura_header]+ordem_colunas]
    header_size = contar_header(estrutura)
    df.to_excel(writer, sheet_name=sheet_name, startrow=header_size,  header=False, index=False)

    worksheet = writer.sheets[sheet_name]

    for i in range(header_size):
        size = 20 if i != header_size-1 else 60
        worksheet.set_row(i, size, estilos['header'])
        
    coluna_offset = len(estrutura_header)
    
    for i, val in enumerate(estrutura_header):
        worksheet.merge_range(0, i, header_size-1, i, val.titulo, estilos[HEADER_PADRAO])
        worksheet.set_column(i, i, val.tamanho, estilos[val.estilo])


    for i in range(df.shape[1]):
        if i == 0:
            worksheet.write(df.shape[0]+header_size, i, fonte, estilos['fonte'])
        else:
            worksheet.write_blank(df.shape[0]+header_size, i, '', estilos['fonte'])

    parse_estrutura(worksheet, estrutura, coluna_offset, estilos)
