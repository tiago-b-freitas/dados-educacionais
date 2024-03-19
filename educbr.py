import requests
from bs4 import BeautifulSoup

class handleDatabase(object):
    def __init__(self, year):
        self.year = year
        self.root = '.'

    def get_database(self, medium, url, cert=True):
        r = medium.get(url, verify=cert)
        return r

    def save_database(self, content, filename):
        if isinstance(content, str):
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
        else:
            with open(filename, 'wb') as f:
                f.write(content)

    def get_url(self, medium, criterion):
        if hasattr(self, 'file_url'):
            print(f'Endereço já existente.\n{self.basic_names()}\nEndereço={self.file_url}')
            return self.file_url
        print('Obtendo endereço para extração da Base de dados.\n'
             f'{self.basic_names()}')
        r = medium.get(self.url)
        soup = BeautifulSoup(r.text, 'html.parser')
        year = self.year
        file_urls = eval(criterion)
        self.assert_url(file_urls)
        self.file_url = file_urls[0]
        print(f'Endereço {self.file_url} obtido com sucesso!')
        return self.file_url

    def get_df(self, columns=None):
        pass

    def assert_url(self, file_urls):
        assert len(file_urls) == 1, (f'Mais de um link para extração da base de dados.\n'
                                     f'{self.basic_names}\nFile_urls={self.file_url}')

    def basic_names(self):
        return f'Base de dados={self.name}\nAno={self.year}'


class handleCensoEscolar(handleDatabase):
    def __init__(self, year):
        super().__init__(year)
        self.name = 'Censo Escolar'
        self.path = f'{self.root}/censo-escolar'
        self.url = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar'

    def get_url(self, medium):
        criterion = "[file_url['href'] for file_url in soup.find('div', id='content-core').find_all('a') if str(year) in file_url['href']]"
        file_url = super().get_url(medium, criterion)
        return file_url


with requests.Session() as s:
    for year in range(2023, 2024):
        censo_escolar = handleCensoEscolar(year)
        censo_escolar.get_url(s)
        censo_escolar.get_url(s)
