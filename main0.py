# Importação de Libs
import geopandas as gpd
import pandas as pd
from flask import Flask, render_template, request, send_file
from flask_jsonpify import jsonpify

# Paths de arquivos para leitura
# PATH TRAMPO
path_bacia = "C:\\Users\\paulo.smaia\\Documents\\LOCAL_DB\\BACIAS_GO\\BACIA_JOAO_LEITE\\BACIA_JOAOLEITE.gpkg"
path_cnarh = "C:\\Users\\paulo.smaia\\Documents\\LOCAL_DB\\BACIAS_GO\\BACIA_JOAO_LEITE\\cnarh40_joaoleite.gpkg"
path_durhs = "C:\\Users\\paulo.smaia\\Documents\\LOCAL_DB\\BACIAS_GO\\BACIA_JOAO_LEITE\\durhs_111121_joaoleite_points.gpkg"
path_subtrecho = "C:\\Users\\paulo.smaia\\Documents\\LOCAL_DB\\BACIAS_GO\\BACIA_JOAO_LEITE\\subtrechos.gpkg"
logo = "C:\\Users\\paulo.smaia\\Documents\\LOCAL_DB\\BACIAS_GO\\BACIA_JOAO_LEITE\\gota_icon_conv.ico"

# PATH HOME
#path_bacia = "D:\\SEMAD\\arq_disph\\BACIA_JOAOLEITE.gpkg"
#path_cnarh = "D:\\SEMAD\\arq_disph\\cnarh40_joaoleite.gpkg"
#path_durhs = "D:\\SEMAD\\arq_disph\\durhs_111121_joaoleite_points.gpkg"
#path_subtrecho = "D:\\SEMAD\\arq_disph\\subtrechos.gpkg"
#logo = "D:\\SEMAD\\arq_disph\\gota_icon_conv.ico"


bacia_joaoleite = gpd.read_file(path_bacia)
cnarh4_joaoleite = gpd.read_file(path_cnarh)
durhs_joaoleite = gpd.read_file(path_durhs)
subtrechos_joaoleite = gpd.read_file(path_subtrecho)


# Reprojeção
subtrechos_joaoleite = subtrechos_joaoleite.to_crs(4674)
durhs_joaoleite = durhs_joaoleite.to_crs(4674)

# Tranformação de afluentes em 0 e mudança de tipo de dado
subtrechos_joaoleite['trecho_princ'] = (subtrechos_joaoleite['trecho_princ'].fillna(0)).astype(int)
subtrechos_joaoleite['esp_cd'] = subtrechos_joaoleite['esp_cd'].fillna(0)
subtrechos_joaoleite['Q95ESPAno'] = subtrechos_joaoleite['Q95ESPAno'].fillna(0)

# Cálculo da área à montante
def CalcAreaMont(location,durhs_joaoleite,subtrechos_joaoleite):
  dic = {"cocursodag":(location.iloc[0]['cocursodag']), "cobacia":(location.iloc[0]['cobacia'])}
  cobacia = dic.get("cobacia")
  sel_loc = location[location['cobacia'] == cobacia]
  area_mont = sel_loc['Q_nuareamont']
  return area_mont

# Cálculo das Vazões sazonais com base na cobacia do subtrecho
# UNIDADE SAI EM m³/s
def ConVazoesSazonais(location,durhs_joaoleite,subtrechos_joaoleite):
  DQ95ESPMES = [location.iloc[0]['Q95ESPJan'],location.iloc[0]['Q95ESPFev'],
             location.iloc[0]['Q95ESPMar'],location.iloc[0]['Q95ESPAbr'],
             location.iloc[0]['Q95ESPMai'],location.iloc[0]['Q95ESPJun'],
             location.iloc[0]['Q95ESPJul'],location.iloc[0]['Q95ESPAgo'],
             location.iloc[0]['Q95ESPSet'],location.iloc[0]['Q95ESPOut'],
             location.iloc[0]['Q95ESPNov'],location.iloc[0]['Q95ESPDez']]

  Q95Local = [location.iloc[0]['Q_DQ95Jan']*1000,location.iloc[0]['Q_DQ95Fev']*1000,
              location.iloc[0]['Q_DQ95Mar']*1000,location.iloc[0]['Q_DQ95Abr']*1000,
              location.iloc[0]['Q_DQ95Mai']*1000,location.iloc[0]['Q_DQ95Jun']*1000,
              location.iloc[0]['Q_DQ95Jul']*1000,location.iloc[0]['Q_DQ95Ago']*1000,
              location.iloc[0]['Q_DQ95Set']*1000,location.iloc[0]['Q_DQ95Out']*1000,
              location.iloc[0]['Q_DQ95Nov']*1000,location.iloc[0]['Q_DQ95Dez']*1000]
  Qoutorgavel = [i * 0.5 for i in Q95Local]
  return DQ95ESPMES, Q95Local, Qoutorgavel

# CONSULTA DE DADOS DAS OUTORGAS À MONTANTE
def ConOutorgasAMontante(location,durhs_joaoleite,cnarh4_joaoleite,subtrechos_joaoleite):
  dic = {"cocursodag":(location.iloc[0]['cocursodag']), "cobacia":(location.iloc[0]['cobacia']), "area_km2":(location.iloc[0]['area_km2'])}
  cobacia = dic.get("cobacia")
  cocursodag = dic.get("cocursodag")
  area_km2 = dic.get("area_km2")
  filter_otto = ((cnarh4_joaoleite['cocursodag'].str.contains(cocursodag)) & (cnarh4_joaoleite['cobacia'] > (cobacia)) & (cnarh4_joaoleite['INT_TSU_DS'] != 'Subterrânea'))
  sel_cnarh_externo = cnarh4_joaoleite[filter_otto] #seleção cnarh externa utilizando cod. otto
  filter_trec_princ = ((cnarh4_joaoleite['cobacia']==cobacia) & (cnarh4_joaoleite['cocursodag'] == cocursodag)& (cnarh4_joaoleite['INT_TSU_DS'] != 'Subterrânea')) #Análise em subtrecho????
  filter_trec_princ = cnarh4_joaoleite[filter_trec_princ]
  filter_trec_princ = gpd.sjoin_nearest(filter_trec_princ, subtrechos_joaoleite, how='inner')
  sel_trec_princ = (filter_teste.loc[filter_teste['trecho_princ'] == 1]) #seleção cnarh interna para trecho principal
  merge_all_cnarh = pd.concat([sel_trec_princ,sel_cnarh_externo])
  dados_cnarh = merge_all_cnarh.loc[:,('INT_CD_CNARH40','EMP_NM_EMPREENDIMENTO','EMP_NM_USUARIO',
                                       'EMP_NU_CPFCNPJ','EMP_DS_EMAILRESPONSAVEL','EMP_NU_CEPENDERECO',
                                       'EMP_CD_IBGEMUNCORRESPONDENCIA','EMP_DS_LOGRADOURO','EMP_DS_COMPLEMENTOENDERECO',
                                       'EMP_NU_LOGRADOURO','EMP_NU_CAIXAPOSTAL','EMP_DS_BAIRRO','EMP_NU_DDD','EMP_NU_TELEFONE',
                                       'EMP_SG_UF','EMP_NM_MUNICIPIO')]
  return dados_cnarh

# CONSULTA DE VAZOES DAS OUTORGAS À MONTANTE
def ConOutorgasTotaisAMontante(location,cnarh4_joaoleite,subtrechos_joaoleite):
  dic = {"cocursodag":(location.iloc[0]['cocursodag']),
         "cobacia":(location.iloc[0]['cobacia']),
         "area_km2":(location.iloc[0]['area_km2'])}
  cobacia = dic.get("cobacia")
  cocursodag = dic.get("cocursodag")
  area_km2 = dic.get("area_km2")
  filter_otto = ((cnarh4_joaoleite['cocursodag'].str.contains(cocursodag)) & (cnarh4_joaoleite['cobacia'] > (cobacia))
                 & (cnarh4_joaoleite['INT_TSU_DS'] != 'Subterrânea'))
  sel_cnarh_externo = cnarh4_joaoleite[filter_otto] #seleção cnarh externa utilizando cod. otto
  filter_trec_princ = ((cnarh4_joaoleite['cobacia']==cobacia) & (cnarh4_joaoleite['cocursodag'] == cocursodag)
                       & (cnarh4_joaoleite['INT_TSU_DS'] != 'Subterrânea')) #Análise em subtrecho????
  filter_trec_princ = cnarh4_joaoleite[filter_trec_princ]
  filter_trec_princ = gpd.sjoin_nearest(filter_trec_princ, subtrechos_joaoleite, how='inner')
  sel_trec_princ = (filter_trec_princ.loc[filter_trec_princ['trecho_princ'] == 1]) #seleção cnarh interna para trecho principal
  merge_all_cnarh = pd.concat([sel_trec_princ,sel_cnarh_externo])
  merge_all_cnarh.loc[:,'DAD_QT_VAZAODIAJAN':'DAD_QT_VAZAODIADEZ'] = merge_all_cnarh.loc[:,'DAD_QT_VAZAODIAJAN':'DAD_QT_VAZAODIADEZ'].stack().str.replace('.','', regex=True).unstack()
  merge_all_cnarh.loc[:,'DAD_QT_VAZAODIAJAN':'DAD_QT_VAZAODIADEZ'] = merge_all_cnarh.loc[:,'DAD_QT_VAZAODIAJAN':'DAD_QT_VAZAODIADEZ'].stack().str.replace(',','.', regex=True).unstack()
  merge_all_cnarh = merge_all_cnarh.fillna(value=0)
  merge_all_cnarh.loc[:,'DAD_QT_VAZAODIAJAN':'DAD_QT_VAZAODIADEZ'] = merge_all_cnarh.loc[:,'DAD_QT_VAZAODIAJAN':'DAD_QT_VAZAODIADEZ'].astype(float)
  tot_cnarh_jan = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAJAN'] != 0]
  count_cnarh_jan = tot_cnarh_jan[tot_cnarh_jan.columns[0]].count()
  tot_cnarh_fev = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAFEV'] != 0]
  count_cnarh_fev = tot_cnarh_fev[tot_cnarh_fev.columns[0]].count()
  tot_cnarh_mar = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAMAR'] != 0]
  count_cnarh_mar = tot_cnarh_mar[tot_cnarh_mar.columns[0]].count()
  tot_cnarh_abr = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAABR'] != 0]
  count_cnarh_abr = tot_cnarh_abr[tot_cnarh_abr.columns[0]].count()
  tot_cnarh_mai = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAMAI'] != 0]
  count_cnarh_mai = tot_cnarh_mai[tot_cnarh_mai.columns[0]].count()
  tot_cnarh_jun = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAJUN'] != 0]
  count_cnarh_jun = tot_cnarh_jun[tot_cnarh_jun.columns[0]].count()
  tot_cnarh_jul = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAJUL'] != 0]
  count_cnarh_jul = tot_cnarh_jul[tot_cnarh_jul.columns[0]].count()
  tot_cnarh_ago = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAAGO'] != 0]
  count_cnarh_ago = tot_cnarh_ago[tot_cnarh_ago.columns[0]].count()
  tot_cnarh_set = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIASET'] != 0]
  count_cnarh_set = tot_cnarh_set[tot_cnarh_set.columns[0]].count()
  tot_cnarh_out = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIAOUT'] != 0]
  count_cnarh_out = tot_cnarh_out[tot_cnarh_out.columns[0]].count()
  tot_cnarh_nov = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIANOV'] != 0]
  count_cnarh_nov = tot_cnarh_nov[tot_cnarh_nov.columns[0]].count()
  tot_cnarh_dez = merge_all_cnarh[merge_all_cnarh['DAD_QT_VAZAODIADEZ'] != 0]
  count_cnarh_dez = tot_cnarh_dez[tot_cnarh_dez.columns[0]].count()
  total_outorgas = [count_cnarh_jan,count_cnarh_fev,count_cnarh_mar,count_cnarh_abr,
                  count_cnarh_mai,count_cnarh_jun,count_cnarh_jul,count_cnarh_ago,
                  count_cnarh_set,count_cnarh_out,count_cnarh_nov,count_cnarh_dez]
  # Soma da DAD_QT_VAZAODIAMES e converter p L/s (*1000)/3600
  vazao_tot_cnarh = [sum(merge_all_cnarh['DAD_QT_VAZAODIAJAN']/3.6),sum(merge_all_cnarh['DAD_QT_VAZAODIAFEV']/3.6),
               sum(merge_all_cnarh['DAD_QT_VAZAODIAMAR']/3.6),sum(merge_all_cnarh['DAD_QT_VAZAODIAABR']/3.6),
               sum(merge_all_cnarh['DAD_QT_VAZAODIAMAI']/3.6),sum(merge_all_cnarh['DAD_QT_VAZAODIAJUN']/3.6),
               sum(merge_all_cnarh['DAD_QT_VAZAODIAJUL']/3.6),sum(merge_all_cnarh['DAD_QT_VAZAODIAAGO']/3.6),
               sum(merge_all_cnarh['DAD_QT_VAZAODIASET']/3.6),sum(merge_all_cnarh['DAD_QT_VAZAODIAOUT']/3.6),
               sum(merge_all_cnarh['DAD_QT_VAZAODIANOV']/3.6),sum(merge_all_cnarh['DAD_QT_VAZAODIADEZ']/3.6)]
  return total_outorgas,vazao_tot_cnarh

# CONSULTA DE INFORMAÇÕES DA DURH ANALISADA
def getinfodurh(location):
  # VAZÃO POR DIA
  Qls = [location.iloc[0]['dad_qt_vazaodiajan'],location.iloc[0]['dad_qt_vazaodiafev'],
         location.iloc[0]['dad_qt_vazaodiamar'],location.iloc[0]['dad_qt_vazaodiaabr'],
         location.iloc[0]['dad_qt_vazaodiamai'],location.iloc[0]['dad_qt_vazaodiajun'],
         location.iloc[0]['dad_qt_vazaodiajul'],location.iloc[0]['dad_qt_vazaodiaago'],
         location.iloc[0]['dad_qt_vazaodiaset'],location.iloc[0]['dad_qt_vazaodiaout'],
         location.iloc[0]['dad_qt_vazaodianov'],location.iloc[0]['dad_qt_vazaodiadez']]
# HORAS POR DIA
  HD = [location.iloc[0]['dad_qt_horasdiajan'],location.iloc[0]['dad_qt_horasdiafev'],
        location.iloc[0]['dad_qt_horasdiamar'],location.iloc[0]['dad_qt_horasdiaabr'],
        location.iloc[0]['dad_qt_horasdiamai'],location.iloc[0]['dad_qt_horasdiajun'],
        location.iloc[0]['dad_qt_horasdiajul'],location.iloc[0]['dad_qt_horasdiaago'],
        location.iloc[0]['dad_qt_horasdiaset'],location.iloc[0]['dad_qt_horasdiaout'],
        location.iloc[0]['dad_qt_horasdianov'],location.iloc[0]['dad_qt_horasdiadez']]
# DIA POR MES
  DM = [location.iloc[0]['dad_qt_diasjan'],location.iloc[0]['dad_qt_diasfev'],
        location.iloc[0]['dad_qt_diasmar'],location.iloc[0]['dad_qt_diasabr'],
        location.iloc[0]['dad_qt_diasmai'],location.iloc[0]['dad_qt_diasjun'],
        location.iloc[0]['dad_qt_diasjul'],location.iloc[0]['dad_qt_diasago'],
        location.iloc[0]['dad_qt_diasset'],location.iloc[0]['dad_qt_diasout'],
        location.iloc[0]['dad_qt_diasnov'],location.iloc[0]['dad_qt_diasdez']]
# HORAS POR MES
  HM = [(location.iloc[0]['dad_qt_horasdiajan'])*(location.iloc[0]['dad_qt_diasjan']),
        (location.iloc[0]['dad_qt_horasdiafev'])*(location.iloc[0]['dad_qt_diasfev']),
        (location.iloc[0]['dad_qt_horasdiamar'])*(location.iloc[0]['dad_qt_diasmar']),
        (location.iloc[0]['dad_qt_horasdiaabr'])*(location.iloc[0]['dad_qt_diasabr']),
        (location.iloc[0]['dad_qt_horasdiamai'])*(location.iloc[0]['dad_qt_diasmai']),
        (location.iloc[0]['dad_qt_horasdiajun'])*(location.iloc[0]['dad_qt_diasjun']),
        (location.iloc[0]['dad_qt_horasdiajul'])*(location.iloc[0]['dad_qt_diasjul']),
        (location.iloc[0]['dad_qt_horasdiaago'])*(location.iloc[0]['dad_qt_diasago']),
        (location.iloc[0]['dad_qt_horasdiaset'])*(location.iloc[0]['dad_qt_diasset']),
        (location.iloc[0]['dad_qt_horasdiaout'])*(location.iloc[0]['dad_qt_diasout']),
        (location.iloc[0]['dad_qt_horasdianov'])*(location.iloc[0]['dad_qt_diasnov']),
        (location.iloc[0]['dad_qt_horasdiadez'])*(location.iloc[0]['dad_qt_diasdez'])]
# M³ POR MÊS
  M3 = [((x*y)*3.6) for x,y in zip(HM,Qls)]
# DIC DE INFORMAÇÕES
  teste = {"Vazão/Dia":Qls,
         "Horas/Mês":list(map(int, HM)),
         "Horas/Dia":list(map(int, HD)),
          "Dia/Mês":list(map(int, DM)),
          "M³/Mês":M3}
  # CRIAR DATAFRAME
  dfinfos = pd.DataFrame(teste,index=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])

  return dfinfos

#FUNÇÃO DE VAZAO DAS DURHS VALIDADAS
def ConVazoesDurhsValid(location,durhs_joaoleite,subtrechos_joaoleite):
  dic = {"cocursodag":(location.iloc[0]['cocursodag']), "cobacia":(location.iloc[0]['cobacia']), "area_km2":(location.iloc[0]['area_km2'])}
  cobacia = dic.get("cobacia")
  cocursodag = dic.get("cocursodag")
  sel_bacia = ((bacia_joaoleite['cocursodag'].str.contains(cocursodag)) & (bacia_joaoleite['cobacia'] > (cobacia)))
  sel_bacia = bacia_joaoleite[sel_bacia]
  sel_durhs_vald = durhs_joaoleite.loc[durhs_joaoleite['situacaodurh']== 'Validada'] # situacaodurh
  clip_durhs = sel_durhs_vald.clip(sel_bacia)
  tot_durh_jan = clip_durhs[clip_durhs['dad_qt_vazaodiajan'] != 0]
  count_durhs_jan = tot_durh_jan[tot_durh_jan.columns[0]].count()
  tot_durh_fev = clip_durhs[clip_durhs['dad_qt_vazaodiafev'] != 0]
  count_durhs_fev = tot_durh_fev[tot_durh_fev.columns[0]].count()
  tot_durh_mar = clip_durhs[clip_durhs['dad_qt_vazaodiamar'] != 0]
  count_durhs_mar = tot_durh_mar[tot_durh_mar.columns[0]].count()
  tot_durh_abr = clip_durhs[clip_durhs['dad_qt_vazaodiaabr'] != 0]
  count_durhs_abr = tot_durh_abr[tot_durh_abr.columns[0]].count()
  tot_durh_mai = clip_durhs[clip_durhs['dad_qt_vazaodiamai'] != 0]
  count_durhs_mai = tot_durh_mai[tot_durh_mai.columns[0]].count()
  tot_durh_jun = clip_durhs[clip_durhs['dad_qt_vazaodiajun'] != 0]
  count_durhs_jun = tot_durh_jun[tot_durh_jun.columns[0]].count()
  tot_durh_jul = clip_durhs[clip_durhs['dad_qt_vazaodiajul'] != 0]
  count_durhs_jul = tot_durh_jul[tot_durh_jul.columns[0]].count()
  tot_durh_ago = clip_durhs[clip_durhs['dad_qt_vazaodiaago'] != 0]
  count_durhs_ago = tot_durh_ago[tot_durh_ago.columns[0]].count()
  tot_durh_set = clip_durhs[clip_durhs['dad_qt_vazaodiaset'] != 0]
  count_durhs_set = tot_durh_set[tot_durh_set.columns[0]].count()
  tot_durh_out = clip_durhs[clip_durhs['dad_qt_vazaodiaout'] != 0]
  count_durhs_out = tot_durh_out[tot_durh_out.columns[0]].count()
  tot_durh_nov = clip_durhs[clip_durhs['dad_qt_vazaodianov'] != 0]
  count_durhs_nov = tot_durh_nov[tot_durh_nov.columns[0]].count()
  tot_durh_dez = clip_durhs[clip_durhs['dad_qt_vazaodiadez'] != 0]
  count_durhs_dez = tot_durh_dez[tot_durh_dez.columns[0]].count()
  total_durhs_mont = [count_durhs_jan,count_durhs_fev,count_durhs_mar,count_durhs_abr,
                      count_durhs_mai,count_durhs_jun,count_durhs_jul,count_durhs_ago,
                      count_durhs_set,count_durhs_out,count_durhs_nov,count_durhs_dez]
  vaz_durhs_mont = [sum(clip_durhs.dad_qt_vazaodiajan), sum(clip_durhs.dad_qt_vazaodiafev),
                  sum(clip_durhs.dad_qt_vazaodiamar), sum(clip_durhs.dad_qt_vazaodiaabr),
                  sum(clip_durhs.dad_qt_vazaodiamai), sum(clip_durhs.dad_qt_vazaodiajun),
                  sum(clip_durhs.dad_qt_vazaodiajul), sum(clip_durhs.dad_qt_vazaodiaago),
                  sum(clip_durhs.dad_qt_vazaodiaset), sum(clip_durhs.dad_qt_vazaodiaout),
                  sum(clip_durhs.dad_qt_vazaodianov), sum(clip_durhs.dad_qt_vazaodiadez)]
  return total_durhs_mont,vaz_durhs_mont

#Durhs diferente de validadas
def VazDurhsDif(location, subtrechos_joaoleite, durhs_joaoleite):
    dic = {"cocursodag": (location.iloc[0]['cocursodag']), "cobacia": (location.iloc[0]['cobacia']),
           "area_km2": (location.iloc[0]['area_km2'])}
    cobacia = dic.get("cobacia")
    cocursodag = dic.get("cocursodag")
    sel_bacia = ((bacia_joaoleite['cocursodag'].str.contains(cocursodag)) & (bacia_joaoleite['cobacia'] > (cobacia)))
    sel_bacia = bacia_joaoleite[sel_bacia]
    sel_durhs = durhs_joaoleite.loc[(durhs_joaoleite['situacaodurh'] == 'Sujeita a outorga') |
                                    (durhs_joaoleite['situacaodurh'] == 'Em Retificação') |
                                    (durhs_joaoleite['situacaodurh'] == 'Enviada') |
                                    (durhs_joaoleite['situacaodurh'] == 'Paralisada') |
                                    (durhs_joaoleite['situacaodurh'] == 'Pendente')]
    durhs_dif_mont = sel_durhs.clip(sel_bacia)

    durh_dif_jan = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiajan'] != 0]
    count_durhs_jan = durh_dif_jan[durh_dif_jan.columns[0]].count()
    durh_dif_fev = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiafev'] != 0]
    count_durhs_fev = durh_dif_fev[durh_dif_fev.columns[0]].count()
    durh_dif_mar = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiamar'] != 0]
    count_durhs_mar = durh_dif_mar[durh_dif_mar.columns[0]].count()
    durh_dif_abr = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiaabr'] != 0]
    count_durhs_abr = durh_dif_abr[durh_dif_abr.columns[0]].count()
    durh_dif_mai = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiamai'] != 0]
    count_durhs_mai = durh_dif_mai[durh_dif_mai.columns[0]].count()
    durh_dif_jun = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiajun'] != 0]
    count_durhs_jun = durh_dif_jun[durh_dif_jun.columns[0]].count()
    durh_dif_jul = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiajul'] != 0]
    count_durhs_jul = durh_dif_jul[durh_dif_jul.columns[0]].count()
    durh_dif_ago = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiaago'] != 0]
    count_durhs_ago = durh_dif_ago[durh_dif_ago.columns[0]].count()
    durh_dif_set = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiaset'] != 0]
    count_durhs_set = durh_dif_set[durh_dif_set.columns[0]].count()
    durh_dif_out = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiaout'] != 0]
    count_durhs_out = durh_dif_out[durh_dif_out.columns[0]].count()
    durh_dif_nov = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodianov'] != 0]
    count_durhs_nov = durh_dif_nov[durh_dif_nov.columns[0]].count()
    durh_dif_dez = durhs_dif_mont[durhs_dif_mont['dad_qt_vazaodiadez'] != 0]
    count_durhs_dez = durh_dif_dez[durh_dif_dez.columns[0]].count()

    total_durhsdif_mont = [count_durhs_jan, count_durhs_fev, count_durhs_mar, count_durhs_abr,
                           count_durhs_mai, count_durhs_jun, count_durhs_jul, count_durhs_ago,
                           count_durhs_set, count_durhs_out, count_durhs_nov, count_durhs_dez]
    vaz_durhs_dif = [sum(durhs_dif_mont.dad_qt_vazaodiajan), sum(durhs_dif_mont.dad_qt_vazaodiafev),
                     sum(durhs_dif_mont.dad_qt_vazaodiamar), sum(durhs_dif_mont.dad_qt_vazaodiaabr),
                     sum(durhs_dif_mont.dad_qt_vazaodiamai), sum(durhs_dif_mont.dad_qt_vazaodiajun),
                     sum(durhs_dif_mont.dad_qt_vazaodiajul), sum(durhs_dif_mont.dad_qt_vazaodiaago),
                     sum(durhs_dif_mont.dad_qt_vazaodiaset), sum(durhs_dif_mont.dad_qt_vazaodiaout),
                     sum(durhs_dif_mont.dad_qt_vazaodianov), sum(durhs_dif_mont.dad_qt_vazaodiadez)]
    dicdif = {"Vazão durhs": vaz_durhs_dif,
              "Qnt. usuarios": total_durhsdif_mont}
    dfdurhs = pd.DataFrame(dicdif,
                           index=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])
    return dfdurhs

# Após passar no critério de localização, a função de análise é executada
def analise(location):
    total_outorgas, vazao_tot_cnarh = ConOutorgasTotaisAMontante(location,cnarh4_joaoleite,subtrechos_joaoleite)
    DQ95ESPMES, Q95Local, Qoutorgavel = ConVazoesSazonais(location, durhs_joaoleite, subtrechos_joaoleite)
    total_durhs_mont, vaz_durhs_mont = ConVazoesDurhsValid(location, durhs_joaoleite, subtrechos_joaoleite)
    dfinfos = getinfodurh(location)
    dfdurhs = VazDurhsDif(location, subtrechos_joaoleite, durhs_joaoleite)  # durhs diferentes de validaadas
    dfinfos['Q95 local l/s'] = Q95Local
    dfinfos['Q95 Esp l/s/km²'] = DQ95ESPMES
    dfinfos['Durhs val à mont'] = total_durhs_mont
    dfinfos['vazao total Durhs Montante'] = vaz_durhs_mont
    dfinfos["Qnt de outorgas à mont "] = total_outorgas
    dfinfos["Vazao Total cnarh Montante L/s"] = vazao_tot_cnarh
    dfinfos['Vazão Total à Montante'] = [(x + y) for x, y in zip(vazao_tot_cnarh, vaz_durhs_mont)]
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / (dfinfos['Q95 local l/s'] * 0.5)) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / (dfinfos['Q95 local l/s'] * 0.5)) * 100
    dfinfos["Q outorgável"] = Qoutorgavel
    dfinfos["Q disponível"] = [(x - y - z) for x, y, z in zip(Q95Local, Qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    # print(dfinfos)
    return dfinfos


# Função inicial para pegar a localização da Durh
def getlocation(numero_durh, durhs_joaoleite, subtrechos_joaoleite):
  point = durhs_joaoleite.loc[durhs_joaoleite['numerodurh']== numero_durh] # AQUI ENTRA NUMERO DA DURH
  location = gpd.sjoin_nearest(point,subtrechos_joaoleite, how='inner')
  if (location.iloc[0]['Q95ESPAno'] == 0):
    print("Subtrecho em barragem/massa d'agua", 'Alerta') # FUTURO POP-UP DE NOTIFICAÇÃO
  else:
    print("Subtrecho fora de barragem/massa d'agua", 'Alerta')
    analise(location)
  return point, location

# função inicial para rodar a localização
#def run(numero_durh):
#  location, point = getlocation(numero_durh,durhs_joaoleite,subtrechos_joaoleite)
#  return numero_durh,location

app = Flask(__name__)

@app.route("/")
# Função -> O que vc quer exibir naquela pagina
def homepage():
    return render_template('homepage.html')

@app.route("/Resultados", methods=["POST", "GET"])
def run():
    numero_durh = request.form['numero_durh']
    point, location = getlocation(numero_durh,durhs_joaoleite,subtrechos_joaoleite)
    dfinfos = analise(location)
    return render_template('resultados.html', tables=[dfinfos.to_html(classes='data', header="true")])


@app.route("/")
def return_to():
    return redirect(url_for("/"))


#@app.route("/Resultados", methods=["GET"])
#def resultados():
#    dfinfos = run()
#    return render_template('resultados.html', tables=[dfinfos.to_html(classes='data', header="true")])
# DURH023031

#Colocar o site no ar
if __name__ == "__main__":
    app.run(debug=True)


