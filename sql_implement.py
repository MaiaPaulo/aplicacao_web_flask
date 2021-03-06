import asyncio
import asyncpg
import pandas as pd
import geopandas as gpd
import numpy as np
from flask import Flask, render_template, request, send_file, make_response


loop = asyncio.get_event_loop()


# FUNÇÃO PARA ADQUIRIR DADOS DO SUBTRECHO MAIS PROX À DURH ANALISADA
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
     ST_Distance(sub.geom, ST_Transform (d.geometry, 3857)) As act_dist
     FROM subtrechos AS sub, otto_minibacias_pol_100k AS mini, durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' AND
         mini.cobacia = (SELECT mini.cobacia
                           FROM
                        
                           durhs_filtradas_completas AS d,
                           otto_minibacias_pol_100k AS mini
                           WHERE
                          d.numerodurh = '{numerodurh}'
                           AND ST_INTERSECTS(ST_Transform (d.geometry, 3857), mini.wkb_geometry)
                           GROUP BY mini.cobacia
                        )    
           AND ST_INTERSECTS(sub.geom, mini.wkb_geometry)
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
                 'longitude': data.iloc[0]['longitude'], 'latitude': data.iloc[0]['latitude']}
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
FROM durhs_filtradas_completas AS d  
WHERE ST_within(ST_Transform(d.geometry, 3857), (SELECT ST_UNION(ST_GeomFromEWKB(o.wkb_geometry)) as geom 
                         FROM otto_minibacias_pol_100k AS o 
                         WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) 
                         AND ((o.cobacia) >= ('{cobacia}')))
         )
AND (d.situacaodurh = 'Validada' 
AND d.pontointerferencia = 'Captação Superficial')
    """)
    cnarh_teste = await conn.fetch(f"""
    SELECT *
    FROM cnarh40 AS cn  
    WHERE ST_within(ST_Transform(cn.geom, 3857), (SELECT ST_UNION(ST_GeomFromEWKB(o.wkb_geometry)) as geom 
                             FROM otto_minibacias_pol_100k AS o 
                             WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) 
                             AND ((o.cobacia) >= ('{cobacia}')))
             )
    AND (cn.int_tin_ds = 'Captação' 
    AND cn.int_tch_ds = 'Rio ou Curso D''Água')
        """)
    print(cobacia, cocursodag)
    return durhs_teste, cnarh_teste


# Cálculo das Vazões sazonais com base na cobacia do subtrecho
# UNIDADE SAI EM m³/s
def ConVazoesSazonais(data):
    DQ95ESPMES = [data.iloc[0]['q_q95espjan'], data.iloc[0]['q_q95espfev'],
                  data.iloc[0]['q_q95espmar'], data.iloc[0]['q_q95espabr'],
                  data.iloc[0]['q_q95espmai'], data.iloc[0]['q_q95espjun'],
                  data.iloc[0]['q_q95espjul'], data.iloc[0]['q_q95espago'],
                  data.iloc[0]['q_q95espset'], data.iloc[0]['q_q95espout'],
                  data.iloc[0]['q_q95espnov'], data.iloc[0]['q_q95espdez']]

    Q95Local = [data.iloc[0]['q_dq95jan'] * 1000, data.iloc[0]['q_dq95fev'] * 1000,
                data.iloc[0]['q_dq95mar'] * 1000, data.iloc[0]['q_dq95abr'] * 1000,
                data.iloc[0]['q_dq95mai'] * 1000, data.iloc[0]['q_dq95jun'] * 1000,
                data.iloc[0]['q_dq95jul'] * 1000, data.iloc[0]['q_dq95ago'] * 1000,
                data.iloc[0]['q_dq95set'] * 1000, data.iloc[0]['q_dq95out'] * 1000,
                data.iloc[0]['q_dq95nov'] * 1000, data.iloc[0]['q_dq95dez'] * 1000]
    feco = data.iloc[0]['feco'].astype(float)
    Qoutorgavel = list(map(lambda x: x * (1-feco), Q95Local))
    return DQ95ESPMES, Q95Local, Qoutorgavel


# CONSULTA DE INFORMAÇÕES DA DURH ANALISADA
def getinfodurh(data):
    # VAZÃO POR DIA
    Qls = [data.iloc[0]['dad_qt_vazaodiajan'], data.iloc[0]['dad_qt_vazaodiafev'],
           data.iloc[0]['dad_qt_vazaodiamar'], data.iloc[0]['dad_qt_vazaodiaabr'],
           data.iloc[0]['dad_qt_vazaodiamai'], data.iloc[0]['dad_qt_vazaodiajun'],
           data.iloc[0]['dad_qt_vazaodiajul'], data.iloc[0]['dad_qt_vazaodiaago'],
           data.iloc[0]['dad_qt_vazaodiaset'], data.iloc[0]['dad_qt_vazaodiaout'],
           data.iloc[0]['dad_qt_vazaodianov'], data.iloc[0]['dad_qt_vazaodiadez']]
    # HORAS POR DIA
    HD = [data.iloc[0]['dad_qt_horasdiajan'], data.iloc[0]['dad_qt_horasdiafev'],
          data.iloc[0]['dad_qt_horasdiamar'], data.iloc[0]['dad_qt_horasdiaabr'],
          data.iloc[0]['dad_qt_horasdiamai'], data.iloc[0]['dad_qt_horasdiajun'],
          data.iloc[0]['dad_qt_horasdiajul'], data.iloc[0]['dad_qt_horasdiaago'],
          data.iloc[0]['dad_qt_horasdiaset'], data.iloc[0]['dad_qt_horasdiaout'],
          data.iloc[0]['dad_qt_horasdianov'], data.iloc[0]['dad_qt_horasdiadez']]
    # DIA POR MES
    DM = [float(data.iloc[0]['dad_qt_diasjan']), float(data.iloc[0]['dad_qt_diasfev']),
          float(data.iloc[0]['dad_qt_diasmar']), float(data.iloc[0]['dad_qt_diasabr']),
          float(data.iloc[0]['dad_qt_diasmai']), float(data.iloc[0]['dad_qt_diasjun']),
          float(data.iloc[0]['dad_qt_diasjul']), float(data.iloc[0]['dad_qt_diasago']),
          float(data.iloc[0]['dad_qt_diasset']), float(data.iloc[0]['dad_qt_diasout']),
          float(data.iloc[0]['dad_qt_diasnov']), float(data.iloc[0]['dad_qt_diasdez'])]
    # HORAS POR MES
    HM = [float(data.iloc[0]['dad_qt_horasdiajan']) * float(data.iloc[0]['dad_qt_diasjan']),
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
    M3 = [((x * y) * 3.6) for x, y in zip(HM, Qls)]
    # DIC DE INFORMAÇÕES
    dic_durh = {"Vazão/Dia": Qls,
                "Horas/Mês": HM,  #list(map(int, HM)),
                "Horas/Dia": HD,  #list(map(int, HD)),
                "Dia/Mês": DM,  #list(map(float, DM)),
                "M³/Mês": M3}
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
FROM durhs_filtradas_completas AS d  
WHERE ST_within(ST_Transform(d.geometry, 3857), (SELECT ST_UNION(ST_GeomFromEWKB(o.wkb_geometry)) as geom 
                         FROM otto_minibacias_pol_100k AS o 
                         WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) 
                         AND ((o.cobacia) >= ('{cobacia}')))
         )
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
FROM cnarh40 AS cn  
WHERE ST_within(ST_Transform(cn.geom, 3857), (SELECT ST_UNION(ST_GeomFromEWKB(o.wkb_geometry)) as geom 
                         FROM otto_minibacias_pol_100k AS o 
                         WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) 
                         AND ((o.cobacia) >= ('{cobacia}')))
         )
AND (cn.int_tin_ds = 'Captação' 
AND cn.int_tch_ds = 'Rio ou Curso D''Água')
    """)
    colnames = [key for key in cnarh_select[0].keys()]
    df_cnarh_select = pd.DataFrame(cnarh_select, columns=colnames)
    gdf_cnarh_select = gpd.GeoDataFrame(df_cnarh_select)
    gdf_cnarh_select['geom'] = gpd.GeoSeries.from_wkb(gdf_cnarh_select['geom'])
    gdf_cnarh_select['geom'].set_crs(epsg='3857', inplace=True)
    gdf_cnarh_select.fillna(np.nan, inplace=True)
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = gdf_cnarh_select.loc[:,
                                                                         'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(
        str).stack().str.replace('.', '', regex=True).unstack()
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = gdf_cnarh_select.loc[:,
                                                                         'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(
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
    gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = gdf_cnarh_select.loc[:,
                                                                         'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(
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
    DQ95ESPMES, Q95Local, Qoutorgavel = ConVazoesSazonais(data)
    dfinfos = getinfodurh(data)
    analise = "Sem Durhs à Montante"
    dfinfos['Q95 local l/s'] = Q95Local
    dfinfos['Q95 Esp l/s/km²'] = DQ95ESPMES
    dfinfos["Qnt de outorgas à mont "] = tot_out
    dfinfos["Vazao Total cnarh Montante L/s"] = vaz_tot_cnarh
    dfinfos['Vazão Total à Montante'] = vaz_tot_cnarh
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / Qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / Qoutorgavel) * 100
    dfinfos["Q outorgável"] = Qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(Qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise


def anals_complete(data):
    tot_out, vaz_tot_cnarh = loop.run_until_complete(get_cnarh40_mont(data))
    DQ95ESPMES, Q95Local, Qoutorgavel = ConVazoesSazonais(data)
    total_durhs_mont, vaz_durhs_mont = loop.run_until_complete(get_valid_durhs(data))
    analise = "Durhs e Outorgas à Montante"
    dfinfos = getinfodurh(data)
    dfinfos['Q95 local l/s'] = Q95Local
    dfinfos['Q95 Esp l/s/km²'] = DQ95ESPMES
    dfinfos['Durhs val à mont'] = total_durhs_mont
    dfinfos['vazao total Durhs Montante'] = vaz_durhs_mont
    dfinfos["Qnt de outorgas à mont "] = tot_out
    dfinfos["Vazao Total cnarh Montante L/s"] = vaz_tot_cnarh
    dfinfos['Vazão Total à Montante'] = [(x + y) for x, y in zip(vaz_tot_cnarh, vaz_durhs_mont)]
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / Qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) /
                                   Qoutorgavel) * 100
    dfinfos["Q outorgável"] = Qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(Qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise

def anals_without_cnarh(data):
    DQ95ESPMES, Q95Local, Qoutorgavel = ConVazoesSazonais(data)
    total_durhs_mont, vaz_durhs_mont = loop.run_until_complete(get_valid_durhs(data))
    analise = "Sem Outorgas à Montante"
    dfinfos = getinfodurh(data)
    dfinfos['Q95 local l/s'] = Q95Local
    dfinfos['Q95 Esp l/s/km²'] = DQ95ESPMES
    dfinfos['Durhs val à mont'] = total_durhs_mont
    dfinfos['Vazão Total à Montante'] = vaz_durhs_mont
    dfinfos["Comprom individual(%)"] = round((dfinfos['Vazão/Dia'] / Qoutorgavel) * 100, 2)
    dfinfos["Comprom bacia(%)"] = round(((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / Qoutorgavel) * 100, 2)
    dfinfos["Q outorgável"] = Qoutorgavel
    dfinfos["Q disponível"] = [(x - y) for x, y in zip(Qoutorgavel, (dfinfos['Vazão Total à Montante']))]
    dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
    dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
    dfinfos = dfinfos.round(decimals=2)
    return dfinfos, analise

def anals_no_mont(data):
    DQ95ESPMES, Q95Local, Qoutorgavel = ConVazoesSazonais(data)
    dfinfos = getinfodurh(data)
    analise = "Sem Durhs e Outorgas à Montante"
    dfinfos['Q95 local l/s'] = Q95Local
    dfinfos['Q95 Esp l/s/km²'] = DQ95ESPMES
    dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / Qoutorgavel) * 100
    dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia']) / Qoutorgavel) * 100
    dfinfos["Q disponível"] = Qoutorgavel
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


@app.route("/Resultados", methods=["POST", "GET"])
def run():
    numero_durh = request.form['numero_durh']
    data, dic_infos = loop.run_until_complete(main(numero_durh))
    corpodagua = dic_infos.get('corpodagua')
    subbacia = dic_infos.get('subbacia')
    mun_durh = dic_infos.get('municipio')
    area = round(dic_infos.get('area_km2'), 2)
    numeroproc = dic_infos.get('numeroprocesso')
    uso = dic_infos.get('finalidadeuso')
    lat = dic_infos.get('latitude')
    lon = dic_infos.get('longitude')
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
                           lat=lat, lon=lon, uso=uso,
                           tables=[dfinfos.to_html(classes='data', header="true")])

@app.route("/")
def return_to():
    return redirect(url_for("/"))

# Colocar o site no ar
if __name__ == "__main__":
    app.run(debug=True)

## TESTES 'DURH019261'


## DURH    12712020 - DURH008200
# 12702020  DURH008201
# 12722020  DURH008218
# 60872021  DURH016463