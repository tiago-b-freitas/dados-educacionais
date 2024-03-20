import os
import zipfile

from bs4 import BeautifulSoup
import pandas as pd
import requests

###############################################################################
# Definições globais

RAW_FILES = 'raw-files'

###############################################################################
# Paths para as raízes das bases de dados

CERT_PATH = 'certificados'

###############################################################################
# Endereços para as raízes das bases de dados

CENSO_ESCOLAR_URL = ('https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-'
                     'abertos/microdados/censo-escolar')

###############################################################################
# Critérios para encontrar os endereços das bases de dados

CENSO_ESCOLAR_CRITERION = ('[file_url["href"] for file_url in soup.find("div",'
                           ' id="content-core").find_all("a")'
                           ' if str(year) in file_url["href"]]')

###############################################################################
# Certificados 

CENSO_ESCOLAR_CERT = 'inep-gov-br-chain.pem'

def obter_dtype(series, df):
    if pd.notna(series.Categoria):
        return 'category'
    elif series.Tipo == 'Char':
        return 'string'
    elif series.Tipo == 'Data':
        return 'datetime'
    elif series.Tipo == 'Num':
        max_ = df[series['Nome da Variável']].max()
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


def print_info(*args):
    logging('INFO', args)


def print_error(*args):
    logging('ERROR', args)


def logging(type_, args):
    print(*[f'[{type_}] {msg}' for msg in args], sep='\n')


class handleDatabase:
    def __init__(self, medium, year):
        self.medium = medium
        self.year = year
        with open('root.txt', 'r', encoding='utf-8') as f:
            self.root = f.read().strip()
        if not os.path.isdir(self.root):
            os.mkdir(self.root)
        self.is_zipped = False
        self.otimized = False
        self.stardardized = False

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

    def get_url(self, criterion):
        if hasattr(self, 'file_url'):
            print_info('Endereço já existente.',
                       *self.basic_names(),
                       f'Endereço={self.file_url}'
            )
            return self.file_url
        print_info('Obtendo endereço para extração da Base de dados.',
                   *self.basic_names())
        r = self.medium.get(self.url)
        soup = BeautifulSoup(r.text, 'html.parser')
        year = self.year
        file_urls = eval(criterion)
        self.assert_url(file_urls)
        self.file_url = file_urls[0]
        print_info(f'Endereço {self.file_url} obtido com sucesso!')
        return self.file_url

    def get_save_raw_database(self, cert=True):
        filename = os.path.split(self.file_url)[-1]
        self.filepath = os.path.join(self.raw_files_path, filename)
        if os.path.isfile(self.filepath):
            print_info(f'{self.filepath} já existente.')
            return
        print_info(f'{self.filepath} não existente. Fazendo download.')
        r = self.medium.get(self.file_url, verify=cert)
        print_info('Download concluído!',
                  f'Gravando arquivo.')
        with open(self.filepath, 'wb') as f:
            f.write(r.content)
        print_info('Arquivo gravado com sucesso!')

    def assert_url(self, file_urls):
        assert len(file_urls) == 1, print_error(
             'Mais de um link para extração da base de dados.', 
            f'File_urls={file_urls}'
        )

    def basic_names(self):
        return [f'Base de dados = "{self.name}"', f'Ano = "{self.year}"']

    def unzip(self):
        pass

    def wraper_unzip(self, func):
        print_info('Descomprimindo arquivo...')
        func()
        print_info('Descompressão concluída!')

    def otimize_df(self):
        pass
    
    def wraper_otimize_df(self, func):
        print_info('Otimizando base de dados...')
        func()
        print_info('Otimização concluída!')

    def standard_df(self):
        pass

    def wraper_standard_df(self, func): 
        print_info('Padronizando base de dados...')
        func()
        print_info('Padronização conluída!')

    def get_df(self, type_, columns=None):
        if columns is None:
            columns = []
        match type_:
            case 'feather':
                self.feather_path = os.path.join(self.path, 'feathers')
                if not os.path.isdir(self.feather_path):
                    os.mkdir(self.feather_path)
                self.feather_path = os.path.join(self.feather_path,
                                            f'{self.year}-{self.name}.feather')
                if os.path.isfile(self.feather_path):
                    print_info(f'Arquivo {self.feather_path} já existente')
                    self.df = pd.read_feather(self.feather_path,
                                              columns=columns)
                    
                    return self.df

        if not hasattr(self, 'filepath'):
            self.get_save_raw_database()
        if not hasattr(self, 'df') and self.is_zipped:
            self.wraper_unzip(self.unzip)
        if not self.otimized:
            self.wraper_otimize_df(self.otimize_df)
        if not self.stardardized:
            self.wraper_standard_df(self.standard_df)
        self.save(type_)   
        return self.df
                
    def save(self, type_):
        success = False
        print_info(f'Salvando no formato {type_}...')
        match type_:
            case 'feather':
                self.df.to_feather(self.feather_path)
                success = True
        if success:
            print_info('Arquivo salvo com sucesso!')
        else:
            print_error('O arquivo não foi salvo por algum erro')


class handleCensoEscolar(handleDatabase):
    def __init__(self, medium, year):
        super().__init__(medium, year)
        self.name = 'Censo Escolar'
        self.path = os.path.join(self.root, 'censo-escolar')
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.raw_files_path = os.path.join(self.path, RAW_FILES)
        if not os.path.isdir(self.raw_files_path):
            os.mkdir(self.raw_files_path)
        self.url = CENSO_ESCOLAR_URL
        self.is_zipped = True

    def get_url(self):
        criterion = CENSO_ESCOLAR_CRITERION
        file_url = super().get_url(criterion)
        self.file_url
        return self.file_url

    def get_save_raw_database(self):
        cert = os.path.join('.', CERT_PATH, CENSO_ESCOLAR_CERT)
        if not os.path.isfile(cert):
            cert = False
        self.get_url()
        super().get_save_raw_database(cert)

    def unzip(self):
        if not hasattr(self, 'filepath'):
            self.get_save_raw_database()
        match self.year:
            case 2022 | 2023:
                selections = ['microdados', '.csv', '~suplemento']
        with zipfile.ZipFile(self.filepath, 'r') as zf:
            for filename in zf.namelist():
                correct_file = True
                for sel in selections:
                    if sel[0] == '~':
                        correct_file &= not sel in filename.lower()
                    else:
                        correct_file &= sel in filename.lower()
                    if not correct_file:
                        break
                if correct_file:
                    with zf.open(filename) as f:
                        df = pd.read_csv(f,
                                         sep=';',
                                         decimal='.',
                                         encoding='windows-1252',
                                         low_memory=False)
                        self.df = df
                        return self.df
                    
    def make_database_dict(self):
        with zipfile.ZipFile(self.filepath) as zf:
            for fn in zf.namelist():
                if 'xlsx' in fn and 'dicion' in fn and '~' not in fn:
                    df_dict_tmp = pd.read_excel(zf.open(fn), header=None)
        df_dict_tmp = (df_dict_tmp[df_dict_tmp[0].notna() &
                                  (df_dict_tmp.iloc[:, self.year % 2000 - 1] != 'n')]
                             .reset_index(drop=True))
        df_dict = df_dict_tmp[df_dict_tmp[0].astype(str).str.isdecimal()]
        header_index = df_dict.index[0] - 1
        df_dict = df_dict.set_axis(df_dict_tmp.iloc[header_index, :], axis=1)
        
        df_dict['dtype'] = df_dict.apply(obter_dtype, axis=1, df=self.df)

        dtype_dict = {nome: dtype for nome, dtype
                                  in df_dict[['Nome da Variável', 'dtype']]
                                             .itertuples(index=False, name=None)
                                  if dtype != 'datetime'}

        self.database_dict = dtype_dict
        return self.database_dict

    def otimize_df(self):
        self.make_database_dict()
        for col, dtype in self.database_dict.items():
            try:
                self.df[col] = self.df[col].astype(dtype)
            except TypeError:
                print_error(f"TypeError: {col}")
                self.df[col] = self.df[col].astype('string')
            except ValueError:
                print_error(f"ValueError: {col}")
                self.df[col] = self.df[col].astype('string')

        match self.year:
            case 2022:
                format_ = '%d%b%Y:%X'
            case 2023:
                format_ = '%d%b%y:%X'

        for col in self.df.select_dtypes('O').columns:
           self.df[col] = pd.to_datetime(self.df[col], format=format_)

        self.otimize_df = True
        return self.df


with requests.Session() as s:
    for year in range(2022, 2023):
        censo_escolar = handleCensoEscolar(s, year)
        censo_escolar.get_df('feather')
