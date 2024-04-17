import os

from urllib.parse import unquote_plus

import requests

from educbr import handleDatabase, RAW_FILES_PATH, CERT_PATH, print_info,\
        print_error


class handleIdesp(handleDatabase):
    SARESP_PATH = 'saresp'
    SARESP_URL = 'https://dados.educacao.sp.gov.br/dataset/microdados-de-alunos-do-sistema-de-avalia%C3%A7%C3%A3o-de-rendimento-escolar-do-estado-de-s%C3%A3o-paulo'
    SARESP_CRITERION = ('[a["href"] for a in soup.find("div", id="data-and-resources")'
                                                '.find_all("a")'
                       ' if "saresp" in a["href"].lower()'
                       ' and str(self.year) in unquote_plus(a["href"])]')
    SARESP_CERT = 'educacao-sp-gov-br-chain.pem'
    SARESP_FIRST_YEAR = 2007
    SARESP_LAST_YEAR = 2022
                        

    def __init__(self, medium, year):
        if (year < self.SARESP_FIRST_YEAR
            or year > self.SARESP_LAST_YEAR
            or year == 2020): # Não houve saresp neste ano
            print_error(f'Não há dados disponíveis para o ano {year}')
            raise ValueError
        super().__init__(medium, year)
        self.name = 'saresp'
        self.filename = 'saresp'
        self.path = os.path.join(self.root, self.SARESP_PATH)
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.raw_files_path = os.path.join(self.path, RAW_FILES_PATH)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.url = unquote_plus(self.SARESP_URL)
        self.is_zipped = False

    def get_url(self):
        cert = os.path.join('.', CERT_PATH, self.SARESP_CERT)
        if not os.path.isfile(cert):
            cert = False
        criterion = self.SARESP_CRITERION
        file_url = super().get_url(criterion, cert=cert)
        return self.file_url

    def get_save_raw_database(self):
        cert = os.path.join('.', CERT_PATH, self.SARESP_CERT)
        if not os.path.isfile(cert):
            cert = False
        self.get_url()
        super().get_save_raw_database(cert)


if __name__ == '__main__':
    with requests.Session() as s:
        for year in range(2007, 2023):
            if year == 2020:
                continue
            sarespDB = handleIdesp(s, year)
            sarespDB.get_save_raw_database()
