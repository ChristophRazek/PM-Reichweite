import pandas as pd
import pyodbc
import SQL as s
import warnings

warnings.simplefilter("ignore")

conn_string = r'DRIVER={SQL Server}; server=172.19.128.2\emeadb; database=emea_enventa_live; UID=usr_razek; PWD=wB382^%H3INJ'
conx = pyodbc.connect(conn_string)

#SQL Varibalen
sql_variables = [[70002,'Ancorotti Cosmetics S.p.A.',201],[70044,'NUCO E.i.G. Kosyl s.j.',208] ]

def Bedarfsdeckung(x):
    # Bestand
    df_anc_best = pd.read_sql(s.bestand_ancorotti, conx)
    df_anc_best = df_anc_best.query(f'ARTIKELNR == "{x}"')
    df_anc_best['BUCHBESTAND'] = df_anc_best['BUCHBESTAND'].astype('int64')
    best_list = df_anc_best.values.tolist()

    # OOR
    df_anc_fw = pd.read_sql(s.fw_ancorotti, conx)
    df_anc_fw[['BELEGNR', 'FIXPOSNR', 'BELEGART']] = df_anc_fw[['BELEGNR', 'FIXPOSNR', 'BELEGART']].astype('int64')
    df_anc_fw.dropna(subset='PM Nr', inplace=True)
    df_anc_fw = df_anc_fw[['BELEGNR', 'FIXPOSNR', 'BELEGART', 'MENGE_BESTELLT', 'PM Nr']]
    df_anc_fw.rename(columns={'PM Nr': 'ARTIKELNR'}, inplace=True)
    df_anc_fw = df_anc_fw.query(f'ARTIKELNR == "{x}"')
    df_anc_fw.reset_index(inplace = True)

    df_anc_fw['BUCHBESTAND'] = 0
    if len(best_list) == 0:
        df_anc_fw['BUCHBESTAND'] = 0
    else:
        df_anc_fw['BUCHBESTAND'].loc[0] = best_list[0][1]


    # PM Zugänge
    df_anc_pm = pd.read_sql(s.pm_ancorotti, conx)
    df_anc_pm = df_anc_pm[['ARTIKELNR', 'PREADVISE MENGE', 'EXP DISPATCH']]
    df_anc_pm['PREADVISE MENGE'] = df_anc_pm['PREADVISE MENGE'].astype('int64')
    df_anc_pm = df_anc_pm.query(f'ARTIKELNR == "{x}"')
    pm_list = df_anc_pm.values.tolist()
    #print(pm_list)


    #Zusatzspalten
    df_anc_fw['UNTERDECKUNG1'] = 0
    df_anc_fw['PM_ZUGANG'] = 0
    df_anc_fw['UNTERDECKUNG2'] = 0
    df_anc_fw['PM_DATUM'] = ''
    df_anc_fw['ANMERKUNG'] = ''

    #Berechnung Unterdeckung
    for index, row in df_anc_fw.iterrows():
        df_anc_fw['UNTERDECKUNG1'].loc[index] = df_anc_fw['BUCHBESTAND'].loc[index] - df_anc_fw['MENGE_BESTELLT'].loc[index]
        if df_anc_fw['UNTERDECKUNG1'].loc[index] >= 0:
            df_anc_fw['BUCHBESTAND'].loc[index+1] = df_anc_fw['UNTERDECKUNG1'].loc[index]

        else:
            #Hilfscode um die WHILE Schleife zu Starten da der Default Wert 0 ist!
            df_anc_fw['UNTERDECKUNG2'].loc[index] = -1
            while df_anc_fw['UNTERDECKUNG2'].loc[index] <0:
                #print(len(pm_list))
                if len(pm_list)== 0:
                    break
                else:
                    df_anc_fw['PM_ZUGANG'].loc[index] += pm_list[0][1]
                    df_anc_fw['UNTERDECKUNG2'].loc[index] = df_anc_fw['UNTERDECKUNG1'].loc[index] + df_anc_fw['PM_ZUGANG'].loc[index]
                    df_anc_fw['BUCHBESTAND'].loc[index + 1] = df_anc_fw['UNTERDECKUNG2'].loc[index]
                    df_anc_fw['PM_DATUM'].loc[index] = pm_list[0][2]
                    #print(df_anc_fw['UNTERDECKUNG2'].loc[index])

                    pm_list.pop(0)

    #Ausfüllen der Spalte ob PM On Hand ist oder Ankunft des PM
    datum = ''
    for index, row in df_anc_fw.iterrows():
        if df_anc_fw['PM_DATUM'].loc[index] == '':
            df_anc_fw['ANMERKUNG'].loc[index] = datum
        else:
            datum = df_anc_fw['PM_DATUM'].loc[index]
            df_anc_fw['ANMERKUNG'].loc[index] = datum
    df_anc_fw['ANMERKUNG'].replace(to_replace='', value='PM ON HAND', inplace=True)

    list_concat.append(df_anc_fw)
    #print(df_anc_fw.to_markdown())



list_concat = []
#Auflisten aller PM des OOR
df_article= pd.read_sql(s.liste, conx)
articles = df_article.values.tolist()

# Starten des Loop/Analyse für alle PM im OOR
for a in articles:
    x = a[0]
    Bedarfsdeckung(x)

#Verknüpfen aller PM Analysen
df_all = pd.concat(list_concat)
df_all = df_all[['FIXPOSNR', 'BELEGART', 'BELEGNR', 'BUCHBESTAND', 'UNTERDECKUNG1', 'PM_ZUGANG', 'UNTERDECKUNG2', 'PM_DATUM', 'ANMERKUNG']]
df_all['UNTERDECKUNG2'].replace(to_replace=-1, value=0, inplace=True)

#Fehlen PM?
df_all['ANMERKUNG'] = df_all.apply(lambda x : 'MISSING' if x['UNTERDECKUNG1'] < 0 and x['PM_DATUM'] == '' else x['ANMERKUNG'], axis=1)

#Ersetze Missing durch Datum
df_all['KW']= df_all['ANMERKUNG'].replace(to_replace='MISSING', value='1900-01-01 00:00:00').replace(to_replace='PM ON HAND', value='1900-01-01 00:00:00')
#Konvertiere Datum zu Kalenderwoche
df_all['KW'] = pd.to_datetime(df_all['KW']).dt.strftime('%Y-%V')
df_all['KW'] = df_all.apply(lambda x : 'TBD' if x['ANMERKUNG'] == 'MISSING' else x['KW'], axis=1)
df_all['KW'] = df_all.apply(lambda x : 'PM ON HAND' if x['ANMERKUNG'] == 'PM ON HAND' else x['KW'], axis=1)

#Erstellen des OOR
df_oor =pd.read_sql(s.fw_ancorotti, conx)
df_oor[['BELEGNR', 'FIXPOSNR', 'BELEGART']] = df_oor[['BELEGNR', 'FIXPOSNR', 'BELEGART']].astype('int64')


#Verknüpfen PM Analysen mit dem OOR
df_export = pd.merge(df_oor, df_all, how='left', on=['BELEGNR', 'FIXPOSNR', 'BELEGART'])
df_export['UNTERDECKUNG2'].replace(to_replace=-1, value=0, inplace=True)
df_export=df_export.fillna(0)
df_export = df_export[['FIXPOSNR','BELEGART','BELEGNR','ARTIKELNR','BEZEICHNUNG','DELIVERY DATE','PM Nr',
                      'PM Description','PE14_CommentEMEA','BUCHBESTAND','MENGE_BESTELLT','UNTERDECKUNG1','PM_ZUGANG','UNTERDECKUNG2',
                      'PM_DATUM','ANMERKUNG','KW']]

#Export
df_export.to_excel(r'S:\ANC.xlsx', index=False)
print(df_export.to_markdown())

#Remove the test