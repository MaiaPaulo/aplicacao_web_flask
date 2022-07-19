import asyncio
import asyncpg
import pandas as pd
import geopandas as gpd
import numpy as np
from flask import Flask, render_template, request
from django.shortcuts import redirect
from flask import url_for

loop = asyncio.get_event_loop()

async def main(numero_durh):
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    numerodurh = numero_durh
    data = await conn.fetch(f"""
SELECT *
FROM 
   (SELECT *
     FROM durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' and (d.situacaodurh = 'Validada' OR d.situacaodurh = 'Sujeita a outorga')
    ) AS dunica,  


  (SELECT 
     sub.feco, sub.fid, sub.cobacia, sub.cocursodag, sub.dn, sub.q_q95espjan, sub.q_q95espfev,
     sub.area_km2, sub.q_q95espmar, sub.q_q95espabr, sub.q_q95espmai, sub.q_q95espjun, sub.q_q95espjul,
     sub.q_q95espago, sub.q_q95espset, sub.q_q95espout, sub.q_q95espnov, sub.q_q95espdez,
     sub.q_dq95jan, sub.q_dq95fev, sub.q_dq95mar, sub.q_dq95abr, sub.q_dq95mai, sub.q_dq95jun,
     sub.q_dq95jul, sub.q_dq95ago, sub.q_dq95set, sub.q_dq95out, sub.q_dq95nov, sub.q_dq95dez, sub.q_q95espano,
     ST_Distance(sub.geom, ST_Transform (d.geometry, 3857)) As act_dist, sub.q_noriocomp
     FROM subtrechos AS sub, otto_minibacias_pol_100k AS mini, durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' AND
         mini.cobacia = (SELECT mini.cobacia
                           FROM
                           durhs_filtradas_completas AS d,
                           otto_minibacias_pol_100k AS mini
                           WHERE
                          d.numerodurh = '{numerodurh}'
                           AND ST_INTERSECTS(ST_Transform(d.geometry, 3857),mini.geom)
                           GROUP BY mini.cobacia
                        )    
           AND ST_INTERSECTS(sub.geom, mini.geom)
     ORDER BY act_dist
     LIMIT 1
     ) As sel 
        """)
    await conn.close()
    colnames = [key for key in data[0].keys()]
    data = pd.DataFrame(data, columns=colnames)
    data.fillna(np.nan, inplace=True)
    dic_infos = {'corpodagua': data.iloc[0]['corpodagua'], 'subbacia': data.iloc[0]['subbacia'],
                 'municipio': data.iloc[0]['municipio'], 'area_km2': data.iloc[0]['area_km2'],
                 'numeroprocesso': data.iloc[0]['numeroprocesso'], 'finalidadeuso': data.iloc[0]['finalidadeuso'],
                 'longitude': data.iloc[0]['longitude'], 'latitude': data.iloc[0]['latitude'],
                 'q_noriocomp': data.iloc[0]['q_noriocomp']}
    return data, dic_infos


# SELECIONAR MINI BACIAS
async def get_minibacia(data):
    cobacia = data.loc[0]['cobacia']
    cocursodag = data.loc[0]['cocursodag']
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    bacia_select = await conn.fetch(f"""
SELECT o.cobacia, o.cocursodag, o.cotrecho, o.wkb_geometry as geometry 
FROM otto_minibacias_pol_100k AS o
WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) AND ((o.cobacia) >= ('{cobacia}'))
    """)
    await conn.close()
    colnames = [key for key in bacia_select[0].keys()]
    df = pd.DataFrame(bacia_select, columns=colnames)
    gdf = gpd.GeoDataFrame(df)
    gdf['geometry'] = gpd.GeoSeries.from_wkb(gdf['geometry'])
    gdf.iloc[:, 0:3] = gdf.iloc[:, 0:3].astype(str)
    gdf.set_crs(epsg='3857', inplace=True)
    return gdf, df


# TESTE PARA SABER SE POSSUI DURHS E OUTORGAS À MONTANTE
async def get_tests(data):
    cobacia = data.loc[0]['cobacia']
    cocursodag = data.loc[0]['cocursodag']
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    durhs_teste = await conn.fetch(f"""
SELECT *
FROM durhs_filtradas_otto AS d  
WHERE((d.cocursodag) LIKE ('{cocursodag}%') 
                         AND (d.cobacia) >= ('{cobacia}'))
AND (d.situacaodurh = 'Validada' 
AND d.pontointerferencia = 'Captação Superficial')
    """)
    cnarh_teste = await conn.fetch(f"""
SELECT *
FROM cnarh40_otto AS cn  
WHERE ((cn.cocursodag) LIKE ('{cocursodag}%') 
                         AND (cn.cobacia) >= ('{cobacia}'))
AND (cn.int_tin_ds = 'Captação' 
AND cn.int_tch_ds = 'Rio ou Curso D''Água')
        """)
    print(cobacia, cocursodag)
    return durhs_teste, cnarh_teste


# Cálculo das Vazões sazonais com base na cobacia do subtrecho
# UNIDADE SAI EM m³/s
def con_vazsazonais(data):
    dq95_espmes = [data.iloc[0]['q_q95espjan'], data.iloc[0]['q_q95espfev'],
                   data.iloc[0]['q_q95espmar'], data.iloc[0]['q_q95espabr'],
                   data.iloc[0]['q_q95espmai'], data.iloc[0]['q_q95espjun'],
                   data.iloc[0]['q_q95espjul'], data.iloc[0]['q_q95espago'],
                   data.iloc[0]['q_q95espset'], data.iloc[0]['q_q95espout'],
                   data.iloc[0]['q_q95espnov'], data.iloc[0]['q_q95espdez']]

    q95_local = [data.iloc[0]['q_dq95jan'] * 1000, data.iloc[0]['q_dq95fev'] * 1000,
                 data.iloc[0]['q_dq95mar'] * 1000, data.iloc[0]['q_dq95abr'] * 1000,
                 data.iloc[0]['q_dq95mai'] * 1000, data.iloc[0]['q_dq95jun'] * 1000,
                 data.iloc[0]['q_dq95jul'] * 1000, data.iloc[0]['q_dq95ago'] * 1000,
                 data.iloc[0]['q_dq95set'] * 1000, data.iloc[0]['q_dq95out'] * 1000,
                 data.iloc[0]['q_dq95nov'] * 1000, data.iloc[0]['q_dq95dez'] * 1000]
    feco = data.iloc[0]['feco'].astype(float)
    qoutorgavel = list(map(lambda x: x * (1 - feco), q95_local))
    return dq95_espmes, q95_local, qoutorgavel


# CONSULTA DE INFORMAÇÕES DA DURH ANALISADA
def getinfodurh(data):
    # VAZÃO POR DIA
    qls = [data.iloc[0]['dad_qt_vazaodiajan'], data.iloc[0]['dad_qt_vazaodiafev'],
           data.iloc[0]['dad_qt_vazaodiamar'], data.iloc[0]['dad_qt_vazaodiaabr'],
           data.iloc[0]['dad_qt_vazaodiamai'], data.iloc[0]['dad_qt_vazaodiajun'],
           data.iloc[0]['dad_qt_vazaodiajul'], data.iloc[0]['dad_qt_vazaodiaago'],
           data.iloc[0]['dad_qt_vazaodiaset'], data.iloc[0]['dad_qt_vazaodiaout'],
           data.iloc[0]['dad_qt_vazaodianov'], data.iloc[0]['dad_qt_vazaodiadez']]
    # HORAS POR DIA
    hd = [data.iloc[0]['dad_qt_horasdiajan'], data.iloc[0]['dad_qt_horasdiafev'],
          data.iloc[0]['dad_qt_horasdiamar'], data.iloc[0]['dad_qt_horasdiaabr'],
          data.iloc[0]['dad_qt_horasdiamai'], data.iloc[0]['dad_qt_horasdiajun'],
          data.iloc[0]['dad_qt_horasdiajul'], data.iloc[0]['dad_qt_horasdiaago'],
          data.iloc[0]['dad_qt_horasdiaset'], data.iloc[0]['dad_qt_horasdiaout'],
          data.iloc[0]['dad_qt_horasdianov'], data.iloc[0]['dad_qt_horasdiadez']]
    # DIA POR MES
    dm = [float(data.iloc[0]['dad_qt_diasjan']), float(data.iloc[0]['dad_qt_diasfev']),
          float(data.iloc[0]['dad_qt_diasmar']), float(data.iloc[0]['dad_qt_diasabr']),
          float(data.iloc[0]['dad_qt_diasmai']), float(data.iloc[0]['dad_qt_diasjun']),
          float(data.iloc[0]['dad_qt_diasjul']), float(data.iloc[0]['dad_qt_diasago']),
          float(data.iloc[0]['dad_qt_diasset']), float(data.iloc[0]['dad_qt_diasout']),
          float(data.iloc[0]['dad_qt_diasnov']), float(data.iloc[0]['dad_qt_diasdez'])]
    # HORAS POR MES
    hm = [float(data.iloc[0]['dad_qt_horasdiajan']) * float(data.iloc[0]['dad_qt_diasjan']),
          float(data.iloc[0]['dad_qt_horasdiafev']) * float(data.iloc[0]['dad_qt_diasfev']),
          float(data.iloc[0]['dad_qt_horasdiamar']) * float(data.iloc[0]['dad_qt_diasmar']),
          float(data.iloc[0]['dad_qt_horasdiaabr']) * float(data.iloc[0]['dad_qt_diasabr']),
          float(data.iloc[0]['dad_qt_horasdiamai']) * float(data.iloc[0]['dad_qt_diasmai']),
          float(data.iloc[0]['dad_qt_horasdiajun']) * float(data.iloc[0]['dad_qt_diasjun']),
          float(data.iloc[0]['dad_qt_horasdiajul']) * float(data.iloc[0]['dad_qt_diasjul']),
          float(data.iloc[0]['dad_qt_horasdiaago']) * float(data.iloc[0]['dad_qt_diasago']),
          float(data.iloc[0]['dad_qt_horasdiaset']) * float(data.iloc[0]['dad_qt_diasset']),
          float(data.iloc[0]['dad_qt_horasdiaout']) * float(data.iloc[0]['dad_qt_diasout']),
          float(data.iloc[0]['dad_qt_horasdianov']) * float(data.iloc[0]['dad_qt_diasnov']),
          float(data.iloc[0]['dad_qt_horasdiadez']) * float(data.iloc[0]['dad_qt_diasdez'])]
    # M³ POR MÊS
    m3 = [((x * y) * 3.6) for x, y in zip(hm, qls)]
    # DIC DE INFORMAÇÕES
    dic_durh = {"Vazão/Dia": qls,
                "Horas/Mês": hm,  # list(map(int, hm)),
                "Horas/Dia": hd,  # list(map(int, hd)),
                "Dia/Mês": dm,  # list(map(float, dm)),
                "M³/Mês": m3}
    # CRIAR DATAFRAME
    dfinfos = pd.DataFrame(dic_durh,
                           index=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])

    return dfinfos


# CONSULTA DE VAZAO DAS DURHS VALIDADAS
async def get_valid_durhs(data):
    cobacia = data.loc[0]['cobacia']
    cocursodag = data.loc[0]['cocursodag']
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    durhs_select = await conn.fetch(f"""
SELECT *
FROM durhs_filtradas_otto AS d  
WHERE((d.cocursodag) LIKE ('{cocursodag}%') 
                         AND (d.cobacia) >= ('{cobacia}'))
AND (d.situacaodurh = 'Validada' 
AND d.pontointerferencia = 'Captação Superficial')
    """)
    colnames = [key for key in durhs_select[0].keys()]
    df_durhs_select = pd.DataFrame(durhs_select, columns=colnames)
    gdf_durhs_select = gpd.GeoDataFrame(df_durhs_select)
    gdf_durhs_select['geometry'] = gpd.GeoSeries.from_wkb(gdf_durhs_select['geometry'])
    gdf_durhs_select.set_crs(epsg='3857', inplace=True)
    gdf_durhs_select.fillna(np.nan, inplace=True)
    gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(str)
    tot_durh_jan = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiajan'] != "nan"]
    tot_durh_fev = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiafev'] != "nan"]
    tot_durh_mar = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiamar'] != "nan"]
    tot_durh_abr = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaabr'] != "nan"]
    tot_durh_mai = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiamai'] != "nan"]
    tot_durh_jun = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiajun'] != "nan"]
    tot_durh_jul = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiajul'] != "nan"]
    tot_durh_ago = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaago'] != "nan"]
    tot_durh_set = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaset'] != "nan"]
    tot_durh_out = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaout'] != "nan"]
    tot_durh_nov = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodianov'] != "nan"]
    tot_durh_dez = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiadez'] != "nan"]
    gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(float)
    # Produtos finais adquiridos no return
    total_durhs_mont = [tot_durh_jan.gml_id.count(), tot_durh_fev.gml_id.count(), tot_durh_mar.gml_id.count(),
                        tot_durh_abr.gml_id.count(), tot_durh_mai.gml_id.count(), tot_durh_jun.gml_id.count(),
                        tot_durh_jul.gml_id.count(), tot_durh_ago.gml_id.count(), tot_durh_set.gml_id.count(),
                        tot_durh_out.gml_id.count(), tot_durh_nov.gml_id.count(), tot_durh_dez.gml_id.count()]
    vaz_durhs_mont = [np.nansum(gdf_durhs_select.dad_qt_vazaodiajan), np.nansum(gdf_durhs_select.dad_qt_vazaodiafev),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiamar), np.nansum(gdf_durhs_select.dad_qt_vazaodiaabr),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiamai), np.nansum(gdf_durhs_select.dad_qt_vazaodiajun),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiajul), np.nansum(gdf_durhs_select.dad_qt_vazaodiaago),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiaset), np.nansum(gdf_durhs_select.dad_qt_vazaodiaout),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodianov), np.nansum(gdf_durhs_select.dad_qt_vazaodiadez)]
    return total_durhs_mont, vaz_durhs_mont


# CONSULTA DE VAZOES DAS OUTORGAS À MONTANTE
async def get_cnarh40_mont(data):
    cobacia = data.loc[0]['cobacia']
    cocursodag = data.loc[0]['cocursodag']
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    cnarh_select = await conn.fetch(f"""
SELECT *
FROM cnarh40_otto AS cn  
WHERE ((cn.cocursodag) LIKE ('{cocursodag}%') 
                         AND (cn.cobacia) >= ('{cobacia}'))
AND (cn.int_tin_ds = 'Captação' 
AND cn.int_tch_ds = 'Rio ou Curso D''Água')
    """)
    colnames = [key for key in cnarh_select[0].keys()]
    df_cnarh_select = pd.DataFrame(cnarh_select, columns=colnames)
    gdf_cnarh_select = gpd.GeoDataFrame(df_cnarh_select)
    gdf_cnarh_select['geom'] = gpd.GeoSeries.from_wkb(gdf_cnarh_select['geom'])
    gdf_cnarh_select['geom'].set_crs(epsg='3857', inplace=True)
    gdf_cnarh_select.fillna(np.nan, inplace=True)
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(
        str).stack().str.replace('.', '', regex=True).unstack()
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(
        str).stack().str.replace(',', '.', regex=True).unstack()
    tot_cnarh_jan = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajan'] != "nan"]
    tot_cnarh_fev = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiafev'] != "nan"]
    tot_cnarh_mar = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiamar'] != "nan"]
    tot_cnarh_abr = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaabr'] != "nan"]
    tot_cnarh_mai = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiamai'] != "nan"]
    tot_cnarh_jun = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajun'] != "nan"]
    tot_cnarh_jul = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajul'] != "nan"]
    tot_cnarh_ago = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaago'] != "nan"]
    tot_cnarh_set = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaset'] != "nan"]
    tot_cnarh_out = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaout'] != "nan"]
    tot_cnarh_nov = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodianov'] != "nan"]
    tot_cnarh_dez = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiadez'] != "nan"]
    # Conteagem de outorgar à montante != nan
    tot_out = [tot_cnarh_jan.id.count(), tot_cnarh_fev.id.count(), tot_cnarh_mar.id.count(),
               tot_cnarh_abr.id.count(), tot_cnarh_mai.id.count(), tot_cnarh_jun.id.count(),
               tot_cnarh_jul.id.count(), tot_cnarh_ago.id.count(), tot_cnarh_set.id.count(),
               tot_cnarh_out.id.count(), tot_cnarh_nov.id.count(), tot_cnarh_dez.id.count()]
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(
        float)
    # Soma da DAD_QT_VAZAODIAMES e converter p L/s (*1000)/3600
    vaz_tot_cnarh = [round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiajan'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiafev'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiamar'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaabr'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiamai'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiajun'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiajul'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaago'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaset'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaout'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodianov'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiadez'] / 3.6), 2)]
    return tot_out, vaz_tot_cnarh


def anals_without_durh(data):
    tot_out, vaz_tot_cnarh = loop.run_until_complete(get_cnarh40_mont(data))
    dq95_espmes, q95_local, qoutorgavel = con_vazsazonais(data)
    dfinfos = getinfodurh(data)
    analise = "Sem Durhs à Montante"
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos["Qnt de outorgas à mont "] = tot_out
    dfinfos["Vazao Total cnarh Montante L/s"] = vaz_tot_cnarh
    dfinfos['Vazão Total à Montante'] = vaz_tot_cnarh
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / qoutorgavel) * 100
    dfinfos["Q outorgável"] = qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


def anals_complete(data):
    tot_out, vaz_tot_cnarh = loop.run_until_complete(get_cnarh40_mont(data))
    dq95_espmes, q95_local, qoutorgavel = con_vazsazonais(data)
    total_durhs_mont, vaz_durhs_mont = loop.run_until_complete(get_valid_durhs(data))
    analise = "Durhs e Outorgas à Montante"
    dfinfos = getinfodurh(data)
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos['Durhs val à mont'] = total_durhs_mont
    dfinfos['vazao total Durhs Montante'] = vaz_durhs_mont
    dfinfos["Qnt de outorgas à mont "] = tot_out
    dfinfos["Vazao Total cnarh Montante L/s"] = vaz_tot_cnarh
    dfinfos['Vazão Total à Montante'] = [(x + y) for x, y in zip(vaz_tot_cnarh, vaz_durhs_mont)]
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) /
                                   qoutorgavel) * 100
    dfinfos["Q outorgável"] = qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


def anals_without_cnarh(data):
    dq95_espmes, q95_local, qoutorgavel = con_vazsazonais(data)
    total_durhs_mont, vaz_durhs_mont = loop.run_until_complete(get_valid_durhs(data))
    analise = "Sem Outorgas à Montante"
    dfinfos = getinfodurh(data)
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos['Durhs val à mont'] = total_durhs_mont
    dfinfos['Vazão Total à Montante'] = vaz_durhs_mont
    dfinfos["Comprom individual(%)"] = round((dfinfos['Vazão/Dia'] / qoutorgavel) * 100, 2)
    dfinfos["Comprom bacia(%)"] = round(
        ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / qoutorgavel) * 100, 2)
    dfinfos["Q outorgável"] = qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


def anals_no_mont(data):
    dq95_espmes, q95_local, qoutorgavel = con_vazsazonais(data)
    dfinfos = getinfodurh(data)
    analise = "Sem Durhs e Outorgas à Montante"
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia']) / qoutorgavel) * 100
    dfinfos["Q disponível"] = qoutorgavel
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


# FUNÇÃO PARA ADQUIRIR DADOS DO SUBTRECHO MAIS PROX À DURH ANALISADA
async def main_c(coordenadas_lonlat):
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    coordenadas_lonlat = coordenadas_lonlat
    data_sub = await conn.fetch(f"""
    SELECT sub.feco, sub.fid, sub.cobacia, sub.cocursodag, sub.dn, sub.q_q95espjan, sub.q_q95espfev,
            sub.area_km2, sub.q_q95espmar, sub.q_q95espabr, sub.q_q95espmai, sub.q_q95espjun, sub.q_q95espjul,
            sub.q_q95espago, sub.q_q95espset, sub.q_q95espout, sub.q_q95espnov, sub.q_q95espdez,
            sub.q_dq95jan, sub.q_dq95fev, sub.q_dq95mar, sub.q_dq95abr, sub.q_dq95mai, sub.q_dq95jun,sub.q_dq95jul,
            sub.q_dq95ago, sub.q_dq95set, sub.q_dq95out, sub.q_dq95nov, sub.q_dq95dez, sub.q_q95espano, sub.q_noriocomp
    FROM subtrechos AS sub
    WHERE ST_Within
    (
    ST_GeomFromText('POINT({coordenadas_lonlat})', 3857), sub.geom) = TRUE;
        """)
    await conn.close()
    colnames = [key for key in data_sub[0].keys()]
    data_sub = pd.DataFrame(data_sub, columns=colnames)
    data_sub.fillna(np.nan, inplace=True)
    dic_sub = {'area_km2': data_sub.iloc[0]['area_km2'], 'q_noriocomp': data_sub.iloc[0]['q_noriocomp']}
    return data_sub, dic_sub


async def durh_c(numero_durh):
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    numero_durh = numero_durh
    data_durh = await conn.fetch(f"""
SELECT *
     FROM durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numero_durh}' and (d.situacaodurh = 'Validada' OR d.situacaodurh = 'Sujeita a outorga')

        """)
    await conn.close()
    colnames = [key for key in data_durh[0].keys()]
    data_durh = pd.DataFrame(data_durh, columns=colnames)
    data_durh.fillna(np.nan, inplace=True)
    dic_durh = {'corpodagua': data_durh.iloc[0]['corpodagua'], 'subbacia': data_durh.iloc[0]['subbacia'],
                'municipio': data_durh.iloc[0]['municipio'], 'numeroprocesso': data_durh.iloc[0]['numeroprocesso'],
                'finalidadeuso': data_durh.iloc[0]['finalidadeuso'], 'longitude': data_durh.iloc[0]['longitude'],
                'latitude': data_durh.iloc[0]['latitude']}
    return dic_durh, data_durh


# TESTE PARA SABER SE POSSUI DURHS E OUTORGAS À MONTANTE
async def get_tests_c(data_sub):
    cobacia = data_sub.loc[0]['cobacia']
    cocursodag = data_sub.loc[0]['cocursodag']
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    durhs_teste = await conn.fetch(f"""
SELECT *
FROM durhs_filtradas_otto AS d  
WHERE((d.cocursodag) LIKE ('{cocursodag}%') 
                         AND (d.cobacia) >= ('{cobacia}'))
AND (d.situacaodurh = 'Validada' 
AND d.pontointerferencia = 'Captação Superficial')
    """)
    cnarh_teste = await conn.fetch(f"""
SELECT *
FROM cnarh40_otto AS cn  
WHERE ((cn.cocursodag) LIKE ('{cocursodag}%') 
                         AND (cn.cobacia) >= ('{cobacia}'))
AND (cn.int_tin_ds = 'Captação' 
AND cn.int_tch_ds = 'Rio ou Curso D''Água')
        """)
    print(cobacia, cocursodag)
    return durhs_teste, cnarh_teste


# Cálculo das Vazões sazonais com base na cobacia do subtrecho
# UNIDADE SAI EM m³/s
def con_vazoes_sazonais_c(data_sub):
    dq95_espmes = [data_sub.iloc[0]['q_q95espjan'], data_sub.iloc[0]['q_q95espfev'],
                   data_sub.iloc[0]['q_q95espmar'], data_sub.iloc[0]['q_q95espabr'],
                   data_sub.iloc[0]['q_q95espmai'], data_sub.iloc[0]['q_q95espjun'],
                   data_sub.iloc[0]['q_q95espjul'], data_sub.iloc[0]['q_q95espago'],
                   data_sub.iloc[0]['q_q95espset'], data_sub.iloc[0]['q_q95espout'],
                   data_sub.iloc[0]['q_q95espnov'], data_sub.iloc[0]['q_q95espdez']]

    q95_local = [data_sub.iloc[0]['q_dq95jan'] * 1000, data_sub.iloc[0]['q_dq95fev'] * 1000,
                 data_sub.iloc[0]['q_dq95mar'] * 1000, data_sub.iloc[0]['q_dq95abr'] * 1000,
                 data_sub.iloc[0]['q_dq95mai'] * 1000, data_sub.iloc[0]['q_dq95jun'] * 1000,
                 data_sub.iloc[0]['q_dq95jul'] * 1000, data_sub.iloc[0]['q_dq95ago'] * 1000,
                 data_sub.iloc[0]['q_dq95set'] * 1000, data_sub.iloc[0]['q_dq95out'] * 1000,
                 data_sub.iloc[0]['q_dq95nov'] * 1000, data_sub.iloc[0]['q_dq95dez'] * 1000]
    feco = data_sub.iloc[0]['feco'].astype(float)
    qoutorgavel = list(map(lambda x: x * (1 - feco), q95_local))
    return dq95_espmes, q95_local, qoutorgavel


# CONSULTA DE INFORMAÇÕES DA DURH ANALISADA
def getinfodurh_c(data_durh):
    # VAZÃO POR DIA
    qls = [data_durh.iloc[0]['dad_qt_vazaodiajan'], data_durh.iloc[0]['dad_qt_vazaodiafev'],
           data_durh.iloc[0]['dad_qt_vazaodiamar'], data_durh.iloc[0]['dad_qt_vazaodiaabr'],
           data_durh.iloc[0]['dad_qt_vazaodiamai'], data_durh.iloc[0]['dad_qt_vazaodiajun'],
           data_durh.iloc[0]['dad_qt_vazaodiajul'], data_durh.iloc[0]['dad_qt_vazaodiaago'],
           data_durh.iloc[0]['dad_qt_vazaodiaset'], data_durh.iloc[0]['dad_qt_vazaodiaout'],
           data_durh.iloc[0]['dad_qt_vazaodianov'], data_durh.iloc[0]['dad_qt_vazaodiadez']]
    # HORAS POR DIA
    hd = [data_durh.iloc[0]['dad_qt_horasdiajan'], data_durh.iloc[0]['dad_qt_horasdiafev'],
          data_durh.iloc[0]['dad_qt_horasdiamar'], data_durh.iloc[0]['dad_qt_horasdiaabr'],
          data_durh.iloc[0]['dad_qt_horasdiamai'], data_durh.iloc[0]['dad_qt_horasdiajun'],
          data_durh.iloc[0]['dad_qt_horasdiajul'], data_durh.iloc[0]['dad_qt_horasdiaago'],
          data_durh.iloc[0]['dad_qt_horasdiaset'], data_durh.iloc[0]['dad_qt_horasdiaout'],
          data_durh.iloc[0]['dad_qt_horasdianov'], data_durh.iloc[0]['dad_qt_horasdiadez']]
    # DIA POR MES
    dm = [float(data_durh.iloc[0]['dad_qt_diasjan']), float(data_durh.iloc[0]['dad_qt_diasfev']),
          float(data_durh.iloc[0]['dad_qt_diasmar']), float(data_durh.iloc[0]['dad_qt_diasabr']),
          float(data_durh.iloc[0]['dad_qt_diasmai']), float(data_durh.iloc[0]['dad_qt_diasjun']),
          float(data_durh.iloc[0]['dad_qt_diasjul']), float(data_durh.iloc[0]['dad_qt_diasago']),
          float(data_durh.iloc[0]['dad_qt_diasset']), float(data_durh.iloc[0]['dad_qt_diasout']),
          float(data_durh.iloc[0]['dad_qt_diasnov']), float(data_durh.iloc[0]['dad_qt_diasdez'])]
    # HORAS POR MES
    hm = [float(data_durh.iloc[0]['dad_qt_horasdiajan']) * float(data_durh.iloc[0]['dad_qt_diasjan']),
          float(data_durh.iloc[0]['dad_qt_horasdiafev']) * float(data_durh.iloc[0]['dad_qt_diasfev']),
          float(data_durh.iloc[0]['dad_qt_horasdiamar']) * float(data_durh.iloc[0]['dad_qt_diasmar']),
          float(data_durh.iloc[0]['dad_qt_horasdiaabr']) * float(data_durh.iloc[0]['dad_qt_diasabr']),
          float(data_durh.iloc[0]['dad_qt_horasdiamai']) * float(data_durh.iloc[0]['dad_qt_diasmai']),
          float(data_durh.iloc[0]['dad_qt_horasdiajun']) * float(data_durh.iloc[0]['dad_qt_diasjun']),
          float(data_durh.iloc[0]['dad_qt_horasdiajul']) * float(data_durh.iloc[0]['dad_qt_diasjul']),
          float(data_durh.iloc[0]['dad_qt_horasdiaago']) * float(data_durh.iloc[0]['dad_qt_diasago']),
          float(data_durh.iloc[0]['dad_qt_horasdiaset']) * float(data_durh.iloc[0]['dad_qt_diasset']),
          float(data_durh.iloc[0]['dad_qt_horasdiaout']) * float(data_durh.iloc[0]['dad_qt_diasout']),
          float(data_durh.iloc[0]['dad_qt_horasdianov']) * float(data_durh.iloc[0]['dad_qt_diasnov']),
          float(data_durh.iloc[0]['dad_qt_horasdiadez']) * float(data_durh.iloc[0]['dad_qt_diasdez'])]
    # M³ POR MÊS
    m3 = [((x * y) * 3.6) for x, y in zip(hm, qls)]
    # DIC DE INFORMAÇÕES
    dic_durh = {"Vazão/Dia": qls,
                "Horas/Mês": hm,  # list(map(int, hm)),
                "Horas/Dia": hd,  # list(map(int, hd)),
                "Dia/Mês": dm,  # list(map(float, dm)),
                "M³/Mês": m3}
    # CRIAR DATAFRAME
    dfinfos = pd.DataFrame(dic_durh,
                           index=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])

    return dfinfos


# CONSULTA DE VAZAO DAS DURHS VALIDADAS
async def get_valid_durhs_c(data_sub):
    cobacia = data_sub.loc[0]['cobacia']
    cocursodag = data_sub.loc[0]['cocursodag']
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    durhs_select = await conn.fetch(f"""
SELECT *
FROM durhs_filtradas_otto AS d  
WHERE((d.cocursodag) LIKE ('{cocursodag}%') 
                         AND (d.cobacia) >= ('{cobacia}'))
AND (d.situacaodurh = 'Validada' 
AND d.pontointerferencia = 'Captação Superficial')
    """)
    colnames = [key for key in durhs_select[0].keys()]
    df_durhs_select = pd.DataFrame(durhs_select, columns=colnames)
    gdf_durhs_select = gpd.GeoDataFrame(df_durhs_select)
    gdf_durhs_select['geometry'] = gpd.GeoSeries.from_wkb(gdf_durhs_select['geometry'])
    gdf_durhs_select.set_crs(epsg='3857', inplace=True)
    gdf_durhs_select.fillna(np.nan, inplace=True)
    gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(str)
    tot_durh_jan = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiajan'] != "nan"]
    tot_durh_fev = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiafev'] != "nan"]
    tot_durh_mar = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiamar'] != "nan"]
    tot_durh_abr = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaabr'] != "nan"]
    tot_durh_mai = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiamai'] != "nan"]
    tot_durh_jun = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiajun'] != "nan"]
    tot_durh_jul = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiajul'] != "nan"]
    tot_durh_ago = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaago'] != "nan"]
    tot_durh_set = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaset'] != "nan"]
    tot_durh_out = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiaout'] != "nan"]
    tot_durh_nov = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodianov'] != "nan"]
    tot_durh_dez = gdf_durhs_select[gdf_durhs_select['dad_qt_vazaodiadez'] != "nan"]
    gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_durhs_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(float)
    # Produtos finais adquiridos no return
    total_durhs_mont = [tot_durh_jan.gml_id.count(), tot_durh_fev.gml_id.count(), tot_durh_mar.gml_id.count(),
                        tot_durh_abr.gml_id.count(), tot_durh_mai.gml_id.count(), tot_durh_jun.gml_id.count(),
                        tot_durh_jul.gml_id.count(), tot_durh_ago.gml_id.count(), tot_durh_set.gml_id.count(),
                        tot_durh_out.gml_id.count(), tot_durh_nov.gml_id.count(), tot_durh_dez.gml_id.count()]
    vaz_durhs_mont = [np.nansum(gdf_durhs_select.dad_qt_vazaodiajan), np.nansum(gdf_durhs_select.dad_qt_vazaodiafev),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiamar), np.nansum(gdf_durhs_select.dad_qt_vazaodiaabr),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiamai), np.nansum(gdf_durhs_select.dad_qt_vazaodiajun),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiajul), np.nansum(gdf_durhs_select.dad_qt_vazaodiaago),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodiaset), np.nansum(gdf_durhs_select.dad_qt_vazaodiaout),
                      np.nansum(gdf_durhs_select.dad_qt_vazaodianov), np.nansum(gdf_durhs_select.dad_qt_vazaodiadez)]
    return total_durhs_mont, vaz_durhs_mont


# CONSULTA DE VAZOES DAS OUTORGAS À MONTANTE
async def get_cnarh40_mont_c(data_sub):
    cobacia = data_sub.loc[0]['cobacia']
    cocursodag = data_sub.loc[0]['cocursodag']
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    cnarh_select = await conn.fetch(f"""
SELECT *
FROM cnarh40_otto AS cn  
WHERE ((cn.cocursodag) LIKE ('{cocursodag}%') 
                         AND (cn.cobacia) >= ('{cobacia}'))
AND (cn.int_tin_ds = 'Captação' 
AND cn.int_tch_ds = 'Rio ou Curso D''Água')
    """)
    colnames = [key for key in cnarh_select[0].keys()]
    df_cnarh_select = pd.DataFrame(cnarh_select, columns=colnames)
    gdf_cnarh_select = gpd.GeoDataFrame(df_cnarh_select)
    gdf_cnarh_select['geom'] = gpd.GeoSeries.from_wkb(gdf_cnarh_select['geom'])
    gdf_cnarh_select['geom'].set_crs(epsg='3857', inplace=True)
    gdf_cnarh_select.fillna(np.nan, inplace=True)
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(str).stack().str.replace(
            '.', '', regex=True).unstack()
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(str).stack().str.replace(
            ',', '.', regex=True).unstack()
    tot_cnarh_jan = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajan'] != "nan"]
    tot_cnarh_fev = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiafev'] != "nan"]
    tot_cnarh_mar = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiamar'] != "nan"]
    tot_cnarh_abr = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaabr'] != "nan"]
    tot_cnarh_mai = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiamai'] != "nan"]
    tot_cnarh_jun = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajun'] != "nan"]
    tot_cnarh_jul = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajul'] != "nan"]
    tot_cnarh_ago = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaago'] != "nan"]
    tot_cnarh_set = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaset'] != "nan"]
    tot_cnarh_out = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaout'] != "nan"]
    tot_cnarh_nov = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodianov'] != "nan"]
    tot_cnarh_dez = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiadez'] != "nan"]
    # Conteagem de outorgar à montante != nan
    tot_out = [tot_cnarh_jan.id.count(), tot_cnarh_fev.id.count(), tot_cnarh_mar.id.count(),
               tot_cnarh_abr.id.count(), tot_cnarh_mai.id.count(), tot_cnarh_jun.id.count(),
               tot_cnarh_jul.id.count(), tot_cnarh_ago.id.count(), tot_cnarh_set.id.count(),
               tot_cnarh_out.id.count(), tot_cnarh_nov.id.count(), tot_cnarh_dez.id.count()]
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = \
        gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(
            float)
    # Soma da DAD_QT_VAZAODIAMES e converter p L/s (*1000)/3600
    vaz_tot_cnarh = [round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiajan'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiafev'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiamar'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaabr'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiamai'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiajun'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiajul'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaago'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaset'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiaout'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodianov'] / 3.6), 2),
                     round(np.nansum(gdf_cnarh_select['dad_qt_vazaodiadez'] / 3.6), 2)]
    return tot_out, vaz_tot_cnarh


def anals_complete_c(data_sub, data_durh):
    tot_out, vaz_tot_cnarh = loop.run_until_complete(get_cnarh40_mont_c(data_sub))
    dq95_espmes, q95_local, qoutorgavel = con_vazoes_sazonais_c(data_sub)
    total_durhs_mont, vaz_durhs_mont = loop.run_until_complete(get_valid_durhs_c(data_sub))
    analise = "Durhs e Outorgas à Montante"
    dfinfos = getinfodurh_c(data_durh)
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos['Durhs val à mont'] = total_durhs_mont
    dfinfos['vazao total Durhs Montante'] = vaz_durhs_mont
    dfinfos["Qnt de outorgas à mont "] = tot_out
    dfinfos["Vazao Total cnarh Montante L/s"] = vaz_tot_cnarh
    dfinfos['Vazão Total à Montante'] = [(x + y) for x, y in zip(vaz_tot_cnarh, vaz_durhs_mont)]
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) /
                                   qoutorgavel) * 100
    dfinfos["Q outorgável"] = qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


def anals_without_durh_c(data_sub, data_durh):
    tot_out, vaz_tot_cnarh = loop.run_until_complete(get_cnarh40_mont_c(data_sub))
    dq95_espmes, q95_local, qoutorgavel = con_vazoes_sazonais_c(data_sub)
    dfinfos = getinfodurh_c(data_durh)
    analise = "Sem Durhs à Montante"
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos["Qnt de outorgas à mont "] = tot_out
    dfinfos["Vazao Total cnarh Montante L/s"] = vaz_tot_cnarh
    dfinfos['Vazão Total à Montante'] = vaz_tot_cnarh
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / qoutorgavel) * 100
    dfinfos["Q outorgável"] = qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


def anals_without_cnarh_c(data_sub, data_durh):
    dq95_espmes, q95_local, qoutorgavel = con_vazoes_sazonais_c(data_sub)
    total_durhs_mont, vaz_durhs_mont = loop.run_until_complete(get_valid_durhs_c(data_sub))
    analise = "Sem Outorgas à Montante"
    dfinfos = getinfodurh_c(data_durh)
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos['Durhs val à mont'] = total_durhs_mont
    dfinfos['Vazão Total à Montante'] = vaz_durhs_mont
    dfinfos["Comprom individual(%)"] = round((dfinfos['Vazão/Dia'] / qoutorgavel) * 100, 2)
    dfinfos["Comprom bacia(%)"] = round(
        ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / qoutorgavel) * 100, 2)
    dfinfos["Q outorgável"] = qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


def anals_no_mont_c(data_sub, data_durh):
    dq95_espmes, q95_local, qoutorgavel = con_vazoes_sazonais_c(data_sub)
    dfinfos = getinfodurh_c(data_durh)
    analise = "Sem Durhs e Outorgas à Montante"
    dfinfos['Q95 local l/s'] = q95_local
    dfinfos['Q95 Esp l/s/km²'] = dq95_espmes
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia']) / qoutorgavel) * 100
    dfinfos["Q disponível"] = qoutorgavel
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


app = Flask(__name__)


@app.route("/")
# Função -> O que vc quer exibir naquela pagina
def homepage():
    return render_template('homepage.html')


@app.route("/Análise corrigida")
def correcao():
    return render_template('hp_latlon.html')


@app.route("/Resultados corrigidos", methods=["POST", "GET"])
def run_c():
    coordenadas_lonlat = request.form['coordenadas_lonlat']
    numero_durh = request.form['numero_durh']
    print(coordenadas_lonlat, numero_durh)
    data_sub, dic_sub = loop.run_until_complete(main_c(coordenadas_lonlat))
    dic_durh, data_durh = loop.run_until_complete(durh_c(numero_durh))
    durhs_teste, cnarh_teste = loop.run_until_complete(get_tests_c(data_sub))
    corpodagua = dic_durh.get('corpodagua')
    q_noriocomp = dic_sub.get('q_noriocomp')
    subbacia = dic_durh.get('subbacia')
    mun_durh = dic_durh.get('municipio')
    area = round(dic_sub.get('area_km2'), 2)
    numeroproc = dic_durh.get('numeroprocesso')
    uso = dic_durh.get('finalidadeuso')
    lat = dic_durh.get('latitude')
    lon = dic_durh.get('longitude')
    if q_noriocomp == corpodagua:
        rio_compare = 'Rio condiz com a base'
    else:
        rio_compare = 'Inconsistência no nome do rio em relação à base'
    if (len(durhs_teste) != 0) & (len(cnarh_teste) != 0):
        dfinfos, analise = anals_complete_c(data_sub, data_durh)
    elif (len(durhs_teste) == 0) & (len(cnarh_teste) == 0):
        dfinfos, analise = anals_no_mont_c(data_sub, data_durh)
    elif (len(durhs_teste) == 0) & (len(cnarh_teste) != 0):
        dfinfos, analise = anals_without_durh_c(data_sub, data_durh)
    else:
        dfinfos, analise = anals_without_cnarh_c(data_sub, data_durh)
    return render_template('results_latlon.html',
                           coordenadas_lonlat=coordenadas_lonlat, numero_durh=numero_durh,
                           analise=analise, mun_durh=mun_durh, corpodagua=corpodagua,
                           subbacia=subbacia, area=area, numeroproc=numeroproc,
                           lat=lat, lon=lon, uso=uso, q_noriocomp=q_noriocomp,
                           rio_compare=rio_compare,
                           tables=[dfinfos.to_html(classes='data', header="true")])


@app.route("/Resultados", methods=["POST", "GET"])
def run():
    numero_durh = request.form['numero_durh']
    data, dic_infos = loop.run_until_complete(main(numero_durh))
    corpodagua = dic_infos.get('corpodagua')
    q_noriocomp = dic_infos.get('q_noriocomp')
    subbacia = dic_infos.get('subbacia')
    mun_durh = dic_infos.get('municipio')
    area = round(dic_infos.get('area_km2'), 2)
    numeroproc = dic_infos.get('numeroprocesso')
    uso = dic_infos.get('finalidadeuso')
    lat = dic_infos.get('latitude')
    lon = dic_infos.get('longitude')
    if q_noriocomp == corpodagua:
        rio_compare = 'Rio condiz com a base'
    else:
        rio_compare = 'Inconsistência no nome do rio em relação à base'
    durhs_teste, cnarh_teste = loop.run_until_complete(get_tests(data))
    if (len(durhs_teste) != 0) & (len(cnarh_teste) != 0):
        dfinfos, analise = anals_complete(data)
    elif (len(durhs_teste) == 0) & (len(cnarh_teste) == 0):
        dfinfos, analise = anals_no_mont(data)
    elif (len(durhs_teste) == 0) & (len(cnarh_teste) != 0):
        dfinfos, analise = anals_without_durh(data)
    else:
        dfinfos, analise = anals_without_cnarh(data)
    return render_template('resultados.html',
                           numero_durh=numero_durh, dfinfos=dfinfos,
                           mun_durh=mun_durh, corpodagua=corpodagua,
                           subbacia=subbacia, analise=analise,
                           area=area, numeroproc=numeroproc,
                           lat=lat, lon=lon, uso=uso, q_noriocomp=q_noriocomp,
                           rio_compare=rio_compare,
                           tables=[dfinfos.to_html(classes='data', header="true")])


@app.route("/")
def return_to():
    return redirect(url_for("/"))


# Colocar o site no ar
if __name__ == "__main__":
    app.run(debug=True)
