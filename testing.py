import os
import pandas as pd
import numpy as np

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
    
    def get_available_table_names(self):
        return [[self.name,i.name] for i in self._tables]
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
        self.tc_goals = self.shortcuts('METAS_TC')
        self.class_tc = self.shortcuts('CLASIFICACION_TC')
        
        
        # # master funcs
        self.cc_per_channel() # extract credit cards per channels, creates the channels
        self.calc_cc_per_channel()
        self.calc_cc_class()
        
        # for i in self.channels:
        #     print(i.name,'\n',i.get_table('payroll').get_data())
        
        
        
    def set_channels(self, channel):
        self.channels.append(channel)
    
    def get_TC(self):
        return self._TC
    
    def set_TC(self, TC):
        self._TC = TC
        
    def get_scores(self):
        return self._cc_new_score
    
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
        self.crossover_var = 'numero_de_colaborador'
        self.create_cc_scores()
        for channel in c:
            TC = channel.get_table('TC').get_data() # Credit Card per Channel
            df = self.cc_amount(TC, channel.name)
            df = self.cc_type(TC, df, channel.name)
            df = self.sum_firsts_cc(df,channel.name)
            channel.add_table(TABLE('payroll',df))

    def calc_cc_class(self):
        CTC = self.class_tc
        for channel in self.channels:
            PR = channel.get_table('payroll').get_data()
            if channel.name not in  ('sid', 'empresarial'):
                PR['categoria_tc'] = PR.apply(self.set_class,args=(CTC[CTC['canal'].apply(lambda x: channel.name in x)],), axis=1)
            print(PR)

    def set_class(self, PR, CTC):

        return CTC[CTC['minimo'] <= PR['puntos_tc']][CTC['maximo'] > PR['puntos_tc']]['categoria'].values[0]
        # print(CTC.loc[CTC['minimo'] >= PR['puntos_tc']][
        #     CTC['maximo'] <= PR['puntos_tc'], 'categoria'
        # ].values[0])
        

    def sum_firsts_cc(self, df, channel):
        TC = self.get_TC()
        TC = TC[TC['canal_especifico'].str.lower().str.replace(' ', '_')==channel]
        temp = TC.groupby(['numero_de_colaborador', 'primera/segunda/multicuenta'])['puntos_nuevos'].sum().reset_index()
        temp = temp.pivot(index='numero_de_colaborador', columns='primera/segunda/multicuenta', values='puntos_nuevos').reset_index()
        temp.columns.name = None
        temp = temp.rename(columns={'Primera': 'pnts_1ras_cuentas', 'Segunda': 'pnts_omas'})[['numero_de_colaborador','pnts_1ras_cuentas','pnts_omas']]
        df = pd.merge(df,temp,how='left',on='numero_de_colaborador').replace({None: np.nan}).fillna(0)
        df['puntos_tc'] = df['pnts_1ras_cuentas'] + df['pnts_omas']
        return df
    
    def create_cc_scores(self):
        TC = self.get_TC()
        TC['puntos_nuevos'] = TC.apply(lambda row: row['puntos'] if row['primera/segunda/multicuenta'] == 'Primera' else np.nan, axis=1)
        self.set_TC(TC)
            
    def cc_amount(self, TC, channel):
        df = TC.groupby([self.crossover_var]).size().reset_index(name='cantidad_tc')
        df = self.set_goals(df, channel)
        return df  
    
    def set_goals(self, df, channel):
        channel_verification = self.tc_goals['canal']==channel
        cc_goal = self.tc_goals.loc[channel_verification,'meta_tc'].values[0]
        first_goal = self.tc_goals.loc[channel_verification,'meta_primera'].values[0]
        if cc_goal != 9999 and first_goal != 9999:
            df['meta_tc'] = cc_goal
            df['meta_primera'] = first_goal
        else:
            df['meta_tc'] = 1
            df['meta_primera'] = 1
        
        return df
    
    def cc_type(self, TC, df, channel):
        cc_type = TC.pivot_table(index=self.crossover_var, columns='primera/segunda/multicuenta',aggfunc='size',fill_value=0)
        df = pd.merge(df,cc_type, on=self.crossover_var,how='inner')
        self.check_firsts_goal(df, channel)
        
        return self.first_cc(df)
    
        
    def check_firsts_goal(self, df, channel):
        checker = df.copy()
        checker['goal_fg'] = (df['Primera']>=df['meta_primera']).astype(int)
        checker = checker[['numero_de_colaborador','goal_fg']]
        self.cc_sale_score(checker, channel)
    
    def cc_sale_score(self, checker, channel):
        TC = self.get_TC()
        scores = self.get_scores()
        TC['puntos_nuevos'] = TC.apply(self.set_new_score, args=(checker,channel), axis=1)
        self.set_TC(TC)

        ########################## DEBUGGING ############################################################################
        # print(TC[TC['primera/segunda/multicuenta']=='Segunda'][TC['canal_especifico']=='Walmart']['puntos_nuevos'])
        # print(TC[['canal_especifico','primera/segunda/multicuenta','puntos_nuevos']].sample(5))
        # print(TC)
        # print(TC[TC['canal_especifico']=='Empresarial'][TC['puntos_nuevos'].isna()][['numero_solicitud','canal_especifico','puntos_nuevos','primera/segunda/multicuenta']].shape)
 
    def set_new_score(self, TC, checker, channel):
        if TC['primera/segunda/multicuenta'] == 'Primera':
            return TC['puntos_nuevos']
        elif TC['primera/segunda/multicuenta'] == 'Segunda':
            return self.if_second(TC, channel, checker)
        else:
            return 0


    def if_second(self, TC, channel, checker):
        if self.L.normalize_columns([TC['canal_especifico']])[0] != channel:
            return TC['puntos_nuevos']
        if channel in ('sid', 'empresarial'):
            return TC['puntos']
        return self.extract_score(self.concat_goal_class(TC, self.extract_goal_compliance(checker, TC)))

    def concat_goal_class(self, TC, checker_bool):
        return TC['canal_especifico']+TC['color']+TC['primera/segunda/multicuenta']+str(checker_bool.values[0])
        
    def extract_goal_compliance(self, checker, TC):
        return checker.loc[checker['numero_de_colaborador']==TC['numero_de_colaborador'],'goal_fg']
        
    def extract_score(self, concat):
        cc_2nd_score = self.get_scores()
        return cc_2nd_score.loc[cc_2nd_score['concat']==concat,'puntos'].values[0] 
        
    def first_cc(self, df):
        df['%primeras'] = df['Primera']/df['cantidad_tc']
        return df
    
    def cc_colors(self, TC, df):
        cc_color = TC.pivot_table(index=self.crossover_var, columns='color',aggfunc='size',fill_value=0)
        df = pd.merge(df,cc_color, on=self.crossover_var,how='inner')
        
        return df
    
    
data = DATA_HANDLER()
