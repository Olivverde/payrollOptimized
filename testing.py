import os
import pandas as pd

class TABLE(object):    
    def __init__(self, name=None, data=None) -> None:    
        self.name = name
        self._data = data
        
    def get_data(self):
        return self._data

    def set_data(self, updated_dt):
        self._data = updated_dt
    
    def get_column_s(self, column, multiple=0):
            if multiple == 0:
                return self._data[column]
            elif multiple == 1:
                return self._data[[column]]
        
class CHANNEL:
    def __init__(self, name) -> None:
        
        self.name = name

class PAYROLL(CHANNEL):

    def __init__(self,name) -> None:
        super().__init__(name)
        self._tables = []
        self.year = 0
        self.month = 0
        self.day = 0
        self.xtension = ''
    
    def add_table(self, nw_table, multiple=0):
        if multiple == 0:
            self._tables.append(nw_table)
        elif multiple == 1:
            for table in nw_table:
                self.add_table(table)
        
    def get_all_tables(self):
        return self._tables
    
    def get_table(self, name):
        for table in self._tables:
            if table.name == name:
                return table

class LOADER(object):
    
    def __init__(self):
        
        self._tables = [] 
        
        ignores = self.get_ignores()
        self.get_file_paths(ignores)
        self.extract_columns()
    
    def get_tables(self):
        return self._tables
    
    def set_table(self, table):
        self._tables.append(table)
    
    def get_ignores(self):
        
        ignore_tables = {}
        ign_path = "LIBS/IGNORES.txt"
        with open(ign_path, 'r') as ignores:
            for line in ignores:
                
                try:
                    filename, sheet = line.strip().split('\\')
                except Exception as e:
                    print('WRONG NOMENCLATURE FOUND IN', ign_path,'- ',line)
                    raise SystemExit from e
                
                if filename not in ignore_tables:
                    ignore_tables[filename] = [sheet]
                elif sheet not in ignore_tables[filename]:
                    ignore_tables[filename].append(sheet)
                else:
                    print('DUPLICATED IGNORES FOUND IN', ign_path,'- ',line)
                    raise SystemExit
        return ignore_tables
    
    def get_file_paths(self, ignores):
        # Ruta final
        final_route = "DATA/"
        file_list = os.listdir(final_route)
        self.gen_structure(final_route, file_list, ignores)
        
    def ignoring(self, file, tables, ignores):
        if file in ignores:
                return [table for table in tables if table not in ignores[file]]
        return tables
    
    def add_tables(self, tables, file):
        for table in tables:
                df = pd.read_excel(file,sheet_name=table)
                self.set_table(TABLE(table,df))
                    
    def gen_structure(self, final_route, file_list, ignores):
        
        for file in file_list:
            tables = pd.ExcelFile(final_route + file).sheet_names
            
            info = file.split('.')[0].split('_')
            file_type, file_subtype = info[3], info[4]
            temp_file = file_type+'_'+file_subtype
                        
            tables = self.ignoring(temp_file, tables, ignores)
            self.add_tables(tables,final_route+file)

    def extract_columns(self):
        for table in self.get_tables():
            columns = table.get_data().columns
            normalized_cols = self.normalize_columns(columns)
            table.get_data().columns = normalized_cols

    def normalize_columns(self, columns):
        
        normalized_columns = []
        replacements = {
            'á':'a',
            'é':'e',
            'í':'i',
            'ó':'o',
            'ú':'u'
        }
        
        for column in columns:
            column = column.lower()
            for char in replacements:
                if char in column:
                    column = column.replace(char,replacements[char])
                column = column.replace(' ','_')
            normalized_columns.append(column)
                                
        return normalized_columns
            
class DATA_HANDLER(object):

    def __init__(self):
        
        self.channels = []
        self.L = LOADER()
        self.loaders = self.L.get_tables()
        
        # master data
        self._TC = self.shortcuts('TC')
        self._cc_new_score = self.shortcuts('PUNTOS_NUEVOS')
        
        
        # # master funcs

        self.cc_per_channel() # extract credit cards per channels, creates the channels
        self.calc_cc_per_channel()
        
        # self.channels = {
        #     'empresarial':{},
        #     'pricesmart':{},
        #     'sid':{},
        #     'venta_directa':{},
        #     'walmart':{}
        # }    
    def set_channels(self, channel):
        self.channels.append(channel)
    
    def get_TC(self):
        return self._TC
    
    def shortcuts(self, tableName):
        for t in self.loaders:
            if tableName == t.name:
                return t.get_data()
 
    def cc_per_channel(self):
        TC = self.get_TC()
        temp_channels = TC['canal_especifico'].unique()      
        for channel in temp_channels:
            normalized_channel = self.L.normalize_columns([channel])[0]
            TC_channel = TC[TC['canal_especifico'] == channel]
            pr = PAYROLL(normalized_channel)
            pr.add_table(TABLE('TC', TC_channel))
            self.set_channels(pr)
    
    def calc_cc_per_channel(self):
        c = self.channels
        for channel in c:
            print(channel.name)
            TC = channel.get_table('TC').get_data()
            df = self.cc_amount(TC)
            df = self.cc_type(TC, df)
            
            
    def cc_amount(self, TC):
        crossover_var = 'numero_de_colaborador'
        df = TC.groupby([crossover_var]).size().reset_index(name='cantidad_tc')
        
        return df  
    
    def cc_type(self, TC, df):
        crossover_var = 'numero_de_colaborador'
        cc_type = TC.pivot_table(index=crossover_var, columns='primera/segunda/multicuenta',aggfunc='size',fill_value=0)
        df = pd.merge(df,cc_type, on=crossover_var,how='inner') # merge temporal
        return df
    
    
        
class COMISIONES(object):
    
    def __init__(self, channel_structure) -> None:

        goals = {}
        
        commission_template = self.create_commission_per_channel(channel_structure)
        # self.calculate_commission(commission_template)
     
    def create_commission_per_channel(self, channel_structure):
        
        crossover_var = 'numero_de_colaborador'
        
        for channel in channel_structure:
            # print(channel)
            TC = channel_structure[channel]['TC'] # df de tarjeta de credito      
            temp_df = TC.groupby([crossover_var]).size().reset_index(name='cantidad_tc') # Calcula el numero de tarjetas colocadas por colaborador
            # Calcula el tipo de tarjeta colocada (primera, segunda o multicuenta)
            cc_type = TC.pivot_table(index=crossover_var, columns='primera/segunda/multicuenta',aggfunc='size',fill_value=0)
            temp_df = pd.merge(temp_df,cc_type, on=crossover_var,how='inner') # merge temporal
            # Calcula el numero de colores colocados por colaborador
            cc_color = TC.pivot_table(index=crossover_var, columns='color',aggfunc='size',fill_value=0)
            temp_df = pd.merge(temp_df,cc_color, on=crossover_var,how='inner') # merge temporal
            temp_df['%primeras'] = temp_df['Primera']/temp_df['cantidad_tc']
            # pnts_first = TC[TC['primera/segunda/multicuenta']=='Primera'][[crossover_var, 'primera/segunda/multicuenta']]
            # pnts_first = pnts_first.groupby([crossover_var]).sum().reset_index(name='pts_1ras_cuentas')
            
            print(temp_df)
    
        
    
    
        
        
 
"""
Clases
"""
data = DATA_HANDLER()
