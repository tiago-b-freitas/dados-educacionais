import os

import requests

# Setup inicial
with open('path.txt') as f:
    PATH = f.read().strip()
PATH_MATRICULAS    = PATH + 'censo-escolar/'
PATH_POPULACAO     = PATH + 'censo-demografico/'
PATH_RA            = PATH + 'RA/'
PATH_RENDIMENTO    = PATH + 'taxas-de-rendimento/'
PATH_ESCOLARIZACAO = PATH + 'pnadc/'
PATH_SINOPSE       = PATH + 'sinopse/'
PATH_FREQUENCIA    = PATH + 'pnadc/'

modulos = [
    PATH_MATRICULAS,
    PATH_POPULACAO,
    PATH_RA,
    PATH_RENDIMENTO,
    PATH_ESCOLARIZACAO,
    PATH_SINOPSE,
    PATH_FREQUENCIA,
]

for modulo in modulos:
    if not os.path.isdir(modulo):
        os.mkdir(modulo)
    if not os.path.isdir(modulo+'pickles'):
        os.mkdir(modulo+'pickles')
    if not os.path.isdir(modulo+'feathers'):
        os.mkdir(modulo+'feathers')

def obter_info(medium, url, cert=True):
    r = medium.get(url, verify=cert)
    return r

def salvar_info(info, filename):
    mode = 'w' if isinstance(info, str) else 'wb'
    with open(filename, mode) as f:
        f.write(info)

