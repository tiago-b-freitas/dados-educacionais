import requests
from bs4 import BeautifulSoup

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
                           ' if "tr(year) in file_url["href"]]')

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

class handleDatabase(object):
    def __init__(self, year):
        self.year = year
        with open('root.txt', 'r', encoding='utf-8') as f:
            self.root = f.read().strip()

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
            print(f'[INFO] Endereço já existente.\n{self.basic_names()}'
                   '\nEndereço={self.file_url}')
            return self.file_url
        print('[INFO] Obtendo endereço para extração da Base de dados.\n'
             f'{self.basic_names()}')
        r = medium.get(self.url)
        soup = BeautifulSoup(r.text, 'html.parser')
        year = self.year
        file_urls = eval(criterion)
        self.assert_url(file_urls)
        self.file_url = file_urls[0]
        print(f'Endereço {self.file_url} obtido com sucesso!')
        return self.file_url

    def get_save_raw_database(self, medium, url, cert=True):
        filename = os.path.split(url)
        if os.path.isfile(filename):
            print('[INFO] {filename} já existente.')
            return

        print(f'[INFO] {filename} não existente. Fazendo download.')
        r = medium.get(url, verify=cert)
        print(f'[INFO]: Download concluído.'
              f'Gravando arquivo em {self.path}/raw-files.')
        base = os.path.join(self.path, 'raw-files')
        if not os.path.isdir(base):
            os.mkdir(base)
        self.filepath = os.path.join(base, filename)
        with open(self.filepath, 'wb') as f:
            f.write(r.content)
        print(f'[INFO]: Arquivo gravado com sucesso.')

    def get_df(self, columns=None):
        pass

    def assert_url(self, file_urls):
        assert len(file_urls) == 1,
                          (f'Mais de um link para extração da base de dados.\n'
                           f'{self.basic_names}\nFile_urls={self.file_url}')

    def basic_names(self):
        return f'Base de dados={self.name}\nAno={self.year}'


class handleCensoEscolar(handleDatabase):
    def __init__(self, year):
        super().__init__(year)
        self.name = 'Censo Escolar'
        self.path = os.path.join(self.root 'censo-escolar')
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        self.url = CENSO_ESCOLAR_URL

    def get_url(self, medium):
        criterion = CENSO_ESCOLAR_CRITERION
        file_url = super().get_url(medium, criterion)
        return file_url

    def get_save_raw_database(self, medium):
        cert = os.path.join(self.root, CERT_PATH, CENSO_ESCOLAR_CERT)
        if not os.path.isfile(cert):
            cert = False
        super().get_save_raw_database(medium,
                                      get_url(medium),
                                      cert)
    def unzip(self):
        if not hasattr(self, 'filepath'):
            get_save_raw_database(medium)
        match self.year:
            case 2022, 2023:
                selections = ['microdados', '.csv', '~suplemento']
        with zipfile.ZipFile(self.filepath, 'r') as zf:
            for filename in z.namelist():
                fn = 
                correct_file = True
                for sel in selections:
                    if sel[0] = '~'
                        correct_file &= not sel in filename.lower()
                    else:
                        correct_file &= sel in filename.lower()
                    if not correct_file:
                        break
                if correct_file:
                    with z.open(filename) as f:
                        df = pd.read_csv(f,
                                         sep=';',
                                         decimal='.',
                                         encoding='windows-1252',
                                         low_memory=False)
                        self.df = df
                        return df
                    
    def make_database_dict(self):
        with zipfile.ZipFile(f'{PATH_OUT}microdados_censo_escolar_{ano}.zip') as zf:
            for fn in zf.namelist():
                if 'xlsx' in fn and 'dicion' in fn and '~' not in fn:
                    df_dict_tmp = pd.read_excel(zf.open(fn), header=None)
        df_dict_tmp = df_dict_tmp[df_dict_tmp[0].notna() &
                                  (df_dict_tmp.iloc[:, ano % 2000 - 1] != 'n')].reset_index(drop=True)
        df_dict = df_dict_tmp[df_dict_tmp[0].astype(str).str.isdecimal()]
        header_index = df_dict.index[0] - 1
        df_dict = df_dict.set_axis(df_dict_tmp.iloc[header_index, :], axis=1)
        
        df_dict['dtype'] = df_dict.apply(obter_dtype, axis=1, df=df)

        dtype_dict = {nome: dtype for nome, dtype
                                  in df_dict[['Nome da Variável', 'dtype']]
                                             .itertuples(index=False, name=None)
                                  if dtype != 'datetime'}

        self.database_dict = dtype_dict
        return self.database_dict

    def otimize_df(self):
        make_database_dict()
        for col, dtype in self.database_dict.items():
            try:
                self.df[col] = self.df[col].astype(dtype)
            except TypeError:
                print(f"TypeError: {col}")
                self.df[col] = self.df[col].astype('string')
            except ValueError:
                print(f"ValueError: {col}")
                self.df[col] = self.df[col].astype('string')

        match self.year:
            case 2023:
                format_ = '%d%b%y:%X'
            case 2022:
                format_ = '%d%b%Y:%X'

        for col in self.df.select_dtypes('O').columns:
           self.df[col] = pd.to_datetime(self.df[col], format=format_)

        return self.df

    def get_df(self):
        if not hasattr(self, 'df'):
            unzip()
        if not self.otimized:
            otimize_df()
        return self.df
                
    def save2feather(self):
        self.df.to_feather()



with requests.Session() as s:
    for year in range(2023, 2024):
        censo_escolar = handleCensoEscolar(year)
        censo_escolar.get_url(s)
        censo_escolar.get_url(s)
