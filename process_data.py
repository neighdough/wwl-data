'''
Created on Aug 10, 2015

@author: nate

'''


from collections import defaultdict
import os
from ld import postgres as pg
from caeser import utils
from config import cnx_params

cred = cnx_params.wwl_2017
host = cred['host']
db = cred['db']
engine = pg.connect(host, 'postgres', db)
cursor = engine.connect()
schema = pg.table_schema()
update_year = db.split('_')[-1]
parcel_year = '2017'
parcels = 'sca_shelby_parcels_{}'.format(parcel_year)
zip_codes = ['38109','38107','38127','38002','38125','38112','38104','38106',
             '38120', '38115','38053','38138','38114','38108','38122','38018',
             '38132','38126', '38119','38054','38139','38118','38135','38103',
             '38141','38028','38017', '38133','38134','38111','38131','38036',
             '38152','38117','38016','38128', '38116','38029','38105'
             ]
#geography = 'cen_tract_2010'

def tot_pop(geography):
    t_schema = schema['b01003']
    col = 'totpop'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
        set {2} = subquery.{2} \
        from (select c.geoid, b01003001 as {2} \
        from {1}.b01003 as c) as subquery \
        where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def hhinc(geography):
    t_schema = schema['b19013']
    col = 'hhinc'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
        set {2} = subquery.{2} \
        from (select c.geoid, b19013001 as {2} \
        from {1}.b19013 as c) as subquery \
        where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def pct_race(geography):
    t_schema = schema['b02001']
    q = """alter table summary_{0} add column pct_wh numeric(20,2), \
            add column pct_bl numeric(20,2), \
            add column pct_ai numeric(20,2), \
            add column pct_as numeric(20,2), \
            add column pct_nh numeric(20,2), \
            add column pct_ot numeric(20,2), \
            add column pct_2m numeric(20,2); \
            update summary_{0} \
            set pct_wh = subquery.pct_wh, \
            pct_bl = subquery.pct_bl, \
            pct_ai = subquery.pct_ai, \
            pct_as = subquery.pct_as, \
            pct_nh = subquery.pct_nh, \
            pct_ot = subquery.pct_ot, \
            pct_2m = subquery.pct_2m \
            from \
            (select t.geoid, \
            t.b02001002 / \
            nullif(b02001001,0)*100 as pct_wh, \
            t.b02001003 / \
            nullif(b02001001,0)*100 as pct_bl, \
            t.b02001004 / \
            nullif(b02001001,0)*100 as pct_ai, \
            t.b02001005 / \
            nullif(b02001001,0)*100 as pct_as, \
            t.b02001006 / \
            nullif(b02001001,0)*100 as pct_nh, \
            t.b02001007 / \
            nullif(b02001001,0)*100 as pct_ot, \
            t.b02001008 / \
            nullif(b02001001,0)*100 as pct_2m \
            from {1}.b02001 as t, summary_{0}) as subquery \
            where summary_{0}.geoid10 = subquery.geoid
        """
    cursor.execute(q.format(geography, t_schema))

#     t_schema = schema['b02001']
#     fields = pg.db_fields('b02001')
#     set = pg.subquery_fields(fields)
#     q = """alter table summary_{0} add column {1}; update summary_{0} \
#     set {2} \
#     from (select c.geoid, {3} \
#     from {4}.b02001 as c) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid;"""
#     cursor.execute(q.format(geography, ', add column '.join(col + ' ' + 'numeric(20,2)' for col in fields.split(',')),
#                    set,','.join(col for col in fields.split(',')),t_schema))

def pct_poverty(geography):
    t_schema = schema['b17001']
    q = """alter table summary_{0} \
            add column pct_pov_to14 numeric(20,2), \
            add column pct_pov_15to17 numeric(20,2), \
            add column pct_pov_18to24 numeric(20,2), \
            add column pct_pov_25to34 numeric(20,2), \
            add column pct_pov_35to44 numeric(20,2), \
            add column pct_pov_45to54 numeric(20,2), \
            add column pct_pov_55to64 numeric(20,2), \
            add column pct_pov_65up numeric(20,2); \
            update summary_{0} \
            set pct_pov_to14 = subquery.pct_pov_to14, \
            pct_pov_15to17 = subquery.pct_pov_15to17,\
            pct_pov_18to24 = subquery.pct_pov_18to24, \
            pct_pov_25to34 = subquery.pct_pov_25to34, \
            pct_pov_35to44 = subquery.pct_pov_35to44, \
            pct_pov_45to54 = subquery.pct_pov_45to54, \
            pct_pov_55to64 = subquery.pct_pov_55to64, \
            pct_pov_65up = subquery.pct_pov_65up \
            from \
            (select t.geoid, \
            (t.b17001004 + t.b17001005 + t.b17001006 + \
            t.b17001007 + t.b17001018 + t.b17001019 + \
            t.b17001020 + t.b17001021) / \
            nullif(t.b17001001,0)*100 as pct_pov_to14, \
            (t.b17001008 + t.b17001009+ t.b17001022 + \
            t.b17001023) / \
            nullif(t.b17001001,0)*100 as pct_pov_15to17, \
            (t.b17001010 + t.b17001024) / \
            nullif(t.b17001001,0)*100 as pct_pov_18to24, \
            (t.b17001011 + t.b17001025) / \
            nullif(t.b17001001,0)*100 as pct_pov_25to34, \
            (t.b17001012 + t.b17001026) / \
            nullif(t.b17001001,0)*100 as pct_pov_35to44, \
            (t.b17001013 + t.b17001027) / \
            nullif(t.b17001001,0)*100 as pct_pov_45to54, \
            (t.b17001014 + t.b17001028) / \
            nullif(t.b17001001,0)*100 as pct_pov_55to64, \
            (t.b17001015 + t.b17001016 + t.b17001029 + \
            t.b17001030) / \
            nullif(t.b17001001,0)*100 as pct_pov_65up \
            from {1}.b17001 as t, summary_{0}) as subquery \
            where summary_{0}.geoid10 = subquery.geoid
        """
    cursor.execute(q.format(geography, t_schema))

#     fields = pg.db_fields('b17001')
#     set = pg.subquery_fields(fields)
#     q = """alter table summary_{0} add column {1}; update summary_{0} \
#     set {2} \
#     from (select c.geoid, {3} \
#     from {4}.b17001 as c) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid;"""
#     cursor.execute(q.format(geography, ', add column '.join(col + ' ' + 'numeric(20,2)' for col in fields.split(',')),
#                    set,','.join(col for col in fields.split(',')),t_schema))

def pct_poverty_race(geography):


    poverty_race_tables = {'': 'tot','a': 'wh', 'b': 'bl', 'c': 'ai',
                           'd': 'as', 'e':'nh', 'f':'ot',
                           'g':'2m', 'i': 'hp'}

    q = """alter table summary_{0} add column {1} numeric(20,2); update summary_{0} \
    set {1} = subquery.{1} \
    from (select c.geoid, {2}/nullif({4},0)*100 as {1} \
    from demographics.{3} as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    for k, val in poverty_race_tables.iteritems():
        cursor.execute(q.format(geography, 'pct_pov_' + val, 'b17001'+k + '002',
                       'b17001'+k, 'b17001'+k+'001'))

def pct_by_sex(geography):
    t_schema = schema['b01001']
    cols = ['pct_male', 'pct_female']
    q = """alter table summary_{0} add column {2} numeric(20,2), \
            add column {3} numeric(20,2); \
            update summary_{0}
            set {2} = subquery.{2}, {3} = subquery.{3} \
            from (select t.geoid, case t.b01001001 when 0 then 0 \
            else (t.b01001002/t.b01001001)*100 end as {2}, \
            case t.b01001001 when 0 then 0 \
            else (t.b01001026/t.b01001001)*100 end as {3} \
            from {1}.b01001 as t, summary_{0}) as subquery \
            where summary_{0}.geoid10 = subquery.geoid
        """
    cursor.execute(q.format(geography, t_schema, cols[0], cols[1]))

def pct_age(geography):
    t_schema = schema['b01001']
    q = """alter table summary_{0}
            add column pct_to14 numeric(20,2), \
            add column pct_15to19 numeric(20,2), \
            add column pct_20to24 numeric(20,2), \
            add column pct_25to34 numeric(20,2), \
            add column pct_35to49 numeric(20,2), \
            add column pct_50to66 numeric(20,2), \
            add column pct_67up numeric(20,2); \
            update summary_{0} \
            set pct_to14 = subquery.pct_to14, \
            pct_15to19 = subquery.pct_15to19,\
            pct_20to24 = subquery.pct_20to24, \
            pct_25to34 = subquery.pct_25to34, \
            pct_35to49 = subquery.pct_35to49, \
            pct_50to66 = subquery.pct_50to66, \
            pct_67up = subquery.pct_67up \
            from \
            (select t.geoid, \
            (t.b01001003 + t.b01001004 + t.b01001005 + \
            t.b01001027 + t.b01001028 + t.b01001029) / \
            nullif(t.b01001001,0)*100 as pct_to14, \
            (t.b01001006 + t.b01001007+ t.b01001030 + \
            t.b01001031) / \
            nullif(t.b01001001,0)*100 as pct_15to19, \
            (t.b01001008 + t.b01001009 + t.b01001010 + \
            t.b01001032 + t.b01001033 + t.b01001034) / \
            nullif(t.b01001001,0)*100 as pct_20to24, \
            (t.b01001011 + t.b01001012 + t.b01001035 + \
            t.b01001036) / \
            nullif(t.b01001001,0)*100 as pct_25to34, \
            (t.b01001013 + t.b01001014 + t.b01001015 + \
            t.b01001037 + t.b01001038 + t.b01001039) / \
            nullif(t.b01001001,0)*100 as pct_35to49, \
            (t.b01001016 + t.b01001017 + t.b01001018 + \
            t.b01001019 + t.b01001020 + t.b01001040 + \
            t.b01001041 + t.b01001042 + t.b01001043 + \
            t.b01001044) / \
            nullif(t.b01001001,0)*100 as pct_50to66, \
            (t.b01001021 + t.b01001022 + t.b01001023 + \
            t.b01001024 + t.b01001025 + t.b01001045 + \
            t.b01001046 + t.b01001047 + t.b01001048 + \
            t.b01001049) / \
            nullif(t.b01001001,0)*100 as pct_67up \
            from {1}.b01001 as t, summary_{0}) as subquery \
            where summary_{0}.geoid10 = subquery.geoid
        """
    cursor.execute(q.format(geography, t_schema))

def housing_density(geography):
    t_schema = schema['b25001']
    col = 'hsng_density'
    q = """alter table summary_{0} drop column if exists {2},
    add column {2} numeric(20,2); update summary_{0} \
    set {2} = subquery.{2}/summary_{0}.sqmiland \
    from (select c.geoid, b25001001 as {2} \
    from {1}.b25001 as c, summary_{0}) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography,t_schema, col))

def sfcomm(geography):

    t_schema = schema[parcels]
    col = 'pct_comm'
    geog_key ={'cen_county_2010':"subquery.geoid10 = '47157'",
              'cen_place_2010': """subquery.geoid10 in ('4701740','4748000',\
              '4716420','4749060','4740350','4728960','4703440')""",
              'cen_tract_2010': "substring(subquery.geoid10, 1, 5) = '47157'",
              'cen_zip_2010': "subquery.geoid10 in ('{}')".format(
                  "','".join(z for z in zip_codes)),
                        'cen_msa_2013': "subquery.geoid10 = '32820'"}

    params = {'geography': geography,
              't_schema': t_schema,
              'col': col,
              'query': geog_key[geography],
              'parcels':parcels}

    q = """drop table if exists tract_sfla;\
    create temp table tract_sfla as \
    select t.geoid10, cast(sum(d.sfla) as float) as sfla \
    from (select distinct on (parcelid)  sfla, wkb_geometry from {t_schema}.{parcels} \
    join {t_schema}.sca_dweldat on parcelid = parid) as d \
    join geography.{geography} as t on \
    st_within(st_centroid(d.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10;\

    drop table if exists tract_sf;\
    create temp table tract_sf as \
    select t.geoid10, cast(sum(d.sf) as float) as sf \
    from (select distinct on (parcelid) sf, wkb_geometry from {t_schema}.{parcels} \
    join {t_schema}.sca_comintext on parcelid = parid) as d \
    join geography.{geography} as t on\
    st_intersects(d.wkb_geometry, t.wkb_geometry) \
    group by t.geoid10 \
    order by geoid10; \

    alter table summary_{geography} drop column if exists {col}, \
    add column {col} numeric(20,2); \
    update summary_{geography} \
    set {col} = subquery.{col}  \
    from (\
    select c.geoid10, sum(sf)/nullif(sum(sf) + sum(sfla), 0) * 100 as {col} \
    from tract_sf as c join tract_sfla on c.geoid10 = tract_sfla.geoid10 \
    group by c.geoid10) as subquery \
    where summary_{geography}.geoid10 = subquery.geoid10 \
    and {query};\
    """
#     q = """alter table summary_{0} add column sfcommacre numeric(20,2); \
#     update summary_{0} \
#     set sfcommacre = subquery.sfcommacre \
#     from (select t.geoid10, sum(sf)/acreland as sfcommacre \
#     from ({1}.sca_shelby_parcels_2016 \
#     join {1}.sca_comintext on parid = parcelid) as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10, t.acreland \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(**params))

def age_comm(geography):
    t_schema = schema['sca_comdat']
    col = 'age_comm'
    geog_key ={'cen_county_2010':"subquery.geoid10 = '47157'",
              'cen_place_2010': "subquery.geoid10 in ('4701740','4748000','4716420','4749060','4740350','4728960','4703440')",
              'cen_tract_2010': "substring(subquery.geoid10, 1, 5) = '47157'",
              'cen_zip_2010': "subquery.geoid10 in ('{}')".format("','".join(z for z in zip_codes)),
              'cen_msa_2013': "subquery.geoid10 = '32820'"}

    params = {'geography': geography,
              't_schema': schema['sca_shelby_parcels_{}'.format(parcel_year)],
              'col': col,
              'query': geog_key[geography],
              'parcels':parcels}

    q = """alter table summary_{geography} add column {col} numeric(20,2);
    update summary_{geography} \
    set {col} = subquery.{col} \
    from \
    (select t.geoid10, 2014 - avg(yrblt) as {col} \
    from ({t_schema}.{parcels} \
    join {t_schema}.sca_comdat on parid = parcelid) as b \
    join geography.{geography} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{geography}.geoid10 = subquery.geoid10
    and {query};"""
    cursor.execute(q.format(**params))

def pct_developed(geography):
    t_schema = schema['sca_pardat']
    col = 'pct_dev'
    geog_key ={'cen_county_2010':"subquery.geoid10 = '47157'",
              'cen_place_2010': "subquery.geoid10 in ('4701740','4748000','4716420','4749060','4740350','4728960','4703440')",
              'cen_tract_2010': "substring(subquery.geoid10, 1, 5) = '47157'",
              'cen_zip_2010': "subquery.geoid10 in ('{}')".format("','".join(z for z in zip_codes)),
              'cen_msa_2013': "subquery.geoid10 = '32820'"}

    params = {'geography': geography,
              't_schema': schema['sca_shelby_parcels_{}'.format(parcel_year)],
              'col': col,
              'query': geog_key[geography],
              'parcels':parcels}
    q = """alter table summary_{geography} add column {col} numeric(20,2); \
    update summary_{geography} \
    set {col} = subquery.{col} \
    from \
    (select t.geoid10, \
    100 * sum(case when luc <> '000' then 1 else 0 end)::numeric/ count(luc) as {col} \
    from ({t_schema}.{parcels} \
    join {t_schema}.sca_pardat on parid = parcelid) as b \
    join geography.{geography} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{geography}.geoid10 = subquery.geoid10
    and {query};"""
    cursor.execute(q.format(**params))

def pct_vacant(geography):
    t_schema = schema['sca_pardat']
    col = 'pct_vac'
    geog_key ={'cen_county_2010':"subquery.geoid10 = '47157'",
              'cen_place_2010': "subquery.geoid10 in ('4701740','4748000','4716420','4749060','4740350','4728960','4703440')",
              'cen_tract_2010': "substring(subquery.geoid10, 1, 5) = '47157'",
              'cen_zip_2010': "subquery.geoid10 in ('{}')".format("','".join(z for z in zip_codes)),
              'cen_msa_2013': "subquery.geoid10 = '32820'"}

    params = {'geography': geography,
              't_schema': schema['sca_shelby_parcels_{}'.format(parcel_year)],
              'col': col,
              'query': geog_key[geography],
              'parcels':parcels}
    q = """alter table summary_{geography} drop column if exists {col},
    add column {col} numeric(20,2); \
    update summary_{geography} \
    set {col} = subquery.{col} \
    from \
    (select t.geoid10, \
    100 * sum(case when luc = '000' then 1 else 0 end)::numeric/ count(luc) as {col} \
    from ({t_schema}.{parcels} \
    join {t_schema}.sca_pardat on parid = parcelid) as b \
    join geography.{geography} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{geography}.geoid10 = subquery.geoid10
    and subquery.geoid10 not in ('72364', '38023')
    and {query};"""
    cursor.execute(q.format(**params))

def park(geography):
    t_schema = schema['cpgis_parks']
    col = 'park_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} drop column if exists {2},
        add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.cpgis_parks as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} drop column if exists {2},
         add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.cpgis_parks as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """

    cursor.execute(q.format(geography, t_schema, col))

def park_pcap(geography):
    t_schema = schema['cpgis_parks']
    col = 'park_pcap'
    q = """alter table summary_{0} drop column if exists {2},
    add column {2} numeric(20,2); \
    update summary_{0} \
    set {2} = subquery.{2}/nullif (totpop,0) * 10000 \
    from (select t.geoid10, name, count(b.wkb_geometry) as {2} \
    from {1}.cpgis_parks as b \
    join geography.{0} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10, name \
    ) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10
    and name <> 'Wapanocca National Wildlife Refuge';"""
    cursor.execute(q.format(geography, t_schema, col))

def gwy_sqmi(geography):
    t_schema = schema['mpo_bicyclefacilities']
    col = 'gwy_sqmi'
    q = """alter table summary_{0} drop column if exists {2},
    add column {2} numeric(20,2); \
    update summary_{0} \
    set {2} = subquery.{2}/sqmiland \
    from \
    (select t.geoid10, sum(miles) as {2} \
    from (select * from {1}.mpo_bicyclefacilities \
    where facility_class in ('Shared Use Path', 'Walking Path')) as b \
    join \
    geography.{0} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def transit_corr_density(geography):
    pass

def age_bldg(geography):
    t_schema = schema['sca_dweldat']
    col = 'age_bldg'
    geog_key ={'cen_county_2010':"subquery.geoid10 = '47157'",
              'cen_place_2010': "subquery.geoid10 in ('4701740','4748000','4716420','4749060','4740350','4728960','4703440')",
              'cen_tract_2010': "substring(subquery.geoid10, 1, 5) = '47157'",
              'cen_zip_2010': "subquery.geoid10 in ('{}')".format("','".join(z for z in zip_codes)),
              'cen_msa_2013': "subquery.geoid10 = '32820'"}

    params = {'geography': geography,
              't_schema': schema['sca_shelby_parcels_{}'.format(parcel_year)],
              'col': col,
              'query': geog_key[geography],
              'parcels':parcels}
    q = """alter table summary_{geography} drop column if exists {col},
    add column {col} numeric(20,2);
    update summary_{geography} \
    set {col} = subquery.{col} \
    from \
    (select t.geoid10, 2014 - avg(yrblt) as {col} \
    from ({t_schema}.{parcels} \
    join (select parid, yrblt from {t_schema}.sca_comdat
    union select parid, yrblt from {t_schema}.sca_dweldat) as p on parid = parcelid) as b \
    join geography.{geography} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{geography}.geoid10 = subquery.geoid10
    and subquery.geoid10 not in ('72364', '38023')
    and {query};"""
    cursor.execute(q.format(**params))

def housing_units(geography):
    t_schema = schema['b25001']
    col = 'hu'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
    set {2} = subquery.{2} \
    from (select c.geoid, b25001001 as {2} \
    from {1}.b25001 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def mdn_house_price(geography):
    t_schema = schema['b25077']
    col = 'mdnhprice'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
    set {2} = subquery.{2} \
    from (select c.geoid, b25077001 as {2} \
    from {1}.b25077 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def mdn_gross_rent(geography):
    t_schema = schema['b25064']
    col = 'mdngrrent'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
    set {2} = subquery.{2} \
    from (select c.geoid, b25064001 as {2} \
    from {1}.b25064 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def hsng_trans_burden(geography):
    t_schema = schema['htaindex']
    col = 'ht_ami'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
    set {2} = subquery.{2} \
    from (select c.geoid, {2} \
    from {1}.htaindex as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def pct_afford_housing(geography):
    t_schema = schema['hud_affordable_housing']
    col = 'pct_afford'
    q = """alter table summary_{0} add column {2} numeric(20,2); \
    update summary_{0} \
    set {2} = 100*subquery.{2}/hu \
    from \
    (select t.geoid10, \
    sum(total_units)  as {2} \
    from {1}.hud_affordable_housing as b \
    join geography.{0} as t on \
    st_within(b.wkb_geometry, t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def hsng_vac_rate(geography):
    t_schema = schema['b25004']
    col = 'pct_hu_vcnt'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
    set {2} = \
    case hu when 0 then 0 \
    else 100*subquery.{2}/hu \
    end \
    from (select c.geoid, b25004001 as {2} \
    from {1}.b25004 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def afford_hsng_green(geography):
    t_schema = schema['hud_affordable_housing']
    p_schema = schema['cpgis_parks']
    col = 'affhsgreen'
    q = """alter table summary_{0} add column {3} numeric(20,2); \
    update summary_{0} \
    set {3} = subquery.{3} \
    from \
    (select t.geoid10, \
    count(h.wkb_geometry) as {3} \
    from {1}.hud_affordable_housing as h \
    left join {2}.cpgis_parks as p \
    on st_dwithin(h.wkb_geometry, p.wkb_geometry, 2640)
    join geography.{0} as t on \
    st_within(h.wkb_geometry, t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, p_schema, col))

def foreclosure(geography):
    t_schema = schema['regis_foreclosures']
    col = 'foreclose'
    q = """alter table summary_{0} add column {2} numeric(20,2);
    update summary_{0} \
    set {2} = subquery.{2} \
    from \
    (select t.geoid10, count(p.wkb_geometry) as {2} \
    from {1}.regis_foreclosures as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def senior_fac(geography):
    pass

def pct_own(geography):
    t_schema = schema['b25003']
    col = 'pct_own'
    q = """alter table summary_{0} add column {2} numeric(20,2); update summary_{0} \
        set {2} = subquery.{2} \
        from (select c.geoid, \
        case b25003001\
        when  0 then 0 \
        else 100*b25003002/b25003001 \
        end \
        as {2} \
        from {1}.b25003 as c) as subquery \
        where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def pct_rent(geography):
    t_schema = schema['b25003']
    col = 'pct_rent'
    q = """alter table summary_{0} add column {2} numeric(20,2); \
        update summary_{0} \
        set {2} = subquery.{2} \
        from (select c.geoid, b25003003/nullif(b25003001,0) * 100 as {2}\
        from {1}.b25003 as c) as subquery \
        where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def pct_own_race(geography):
    poverty_race_tables = {'a': 'wh', 'b': 'bl', 'c': 'ai',
                           'd': 'as', 'e':'nh', 'f':'ot',
                           'g':'2m', 'i': 'hp'}
    t_schema = schema['b25003']
    q = """alter table summary_{0} add column {1} numeric(20,2); \
        update summary_{0} set {1} = subquery.{1} \
        from \
        (select c.geoid, \
        case b.b25003001 \
        when 0 then 0 \
        else 100*{2}/b.b25003001 \
        end \
        as {1} from {3}.{4} as c \
        join (select geoid, b25003001 from {3}.b25003) as b \
        on b.geoid = c.geoid) as subquery \
        where summary_{0}.geoid10 = subquery.geoid;"""
    for k, val in poverty_race_tables.iteritems():
        cursor.execute(q.format(geography, 'pct_own_' + val, 'b25003'+ k + '002',
                       t_schema, 'b25003'+ k))

def pct_mf(geography):
    t_schema = schema['b25032']
    col = 'pct_mf'
    fields_sum =  ' + '.join('b250320' + str(i) for i in range(16, 22))
    q = """alter table summary_{0} add column {3} numeric(20,2); update summary_{0} \
    set {3} = subquery.{3} \
    from (select c.geoid, \
    case b25032001 \
    when 0 then 0 \
    else 100*({1})/b25032001 \
    end \
    as {3} \
    from {2}.b25032 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, fields_sum, t_schema, col))

def age_sf(geography):
    t_schema = schema['sca_dweldat']
    col = 'age_sf'
    geog_key ={'cen_county_2010':"subquery.geoid10 = '47157'",
              'cen_place_2010': "subquery.geoid10 in ('4701740','4748000','4716420','4749060','4740350','4728960','4703440')",
              'cen_tract_2010': "substring(subquery.geoid10, 1, 5) = '47157'",
              'cen_zip_2010': "subquery.geoid10 in ('{}')".format("','".join(z for z in zip_codes)),
              'cen_msa_2013': "subquery.geoid10 = '32820'"}

    params = {'geography': geography,
              't_schema': schema['sca_shelby_parcels_{}'.format(parcel_year)],
              'col': col,
              'query': geog_key[geography],
              'parcels':parcels}
    q = """alter table summary_{geography} drop column if exists {col},
    add column {col} numeric(20,2); \
    update summary_{geography} \
    set {col} = subquery.{col} \
    from \
    (select t.geoid10, \
    2014 - avg(yrblt) as {col} \
    from ({t_schema}.{parcels} \
    join {t_schema}.sca_dweldat on parid = parcelid) as b \
    join geography.{geography} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{geography}.geoid10 = subquery.geoid10
    and subquery.geoid10 not in ('72364', '38023')
    and {query};"""
    cursor.execute(q.format(**params))

def mdn_yr_lived(geography):
    t_schema = schema['b25039']
    col = 'mdn_yr_lived'
    q = """alter table summary_{0} drop column if exists {2},
    add column {2} numeric; update summary_{0} \
    set {2} = subquery.{2} \
    from (select c.geoid, 2012 - b25039001 as {2} \
    from {1}.b25039 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema, col))

def pct_streets_sdw(geography):
    t_schema = schema['teleatlas_streets']
    q = """alter table summary_{0} add column strtsdw_pct numeric(20,2); \
            update summary_{0} \
            set strtsdw_pct = subquery.strtsdw_pct \
            from \
            (select s.geoid10, \
            case when (s.cnt_sdw + s.cnt_no_sdw) > 0 then (s.cnt_sdw::float / (s.cnt_sdw + s.cnt_no_sdw)) * 100 \
            else 0 \
            end as strtsdw_pct \
            from (select g.geoid10, \
            sum(case when r.sidewalk='YES' then 1 else 0 end) as cnt_sdw, \
            sum(case when r.sidewalk='NO' then 1 else 0 end) as cnt_no_sdw \
                 from geography.{0} as g \
            left join transportation.streets_with_sdw as r \
            on ST_DWithin(r.wkb_geometry, g.wkb_geometry, 2) group by g.geoid10) as s) as subquery \
            where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def bikeability(geography):
    t_schema = schema['bikeability']
    q = """alter table summary_{0} add column bic_index numeric(20,2);
    update summary_{0} \
    set bic_index = subquery.bic_index / sqmiland \
    from \
    (select t.geoid10, 100 * (val - min) / max - min as bic_index \
    from (select ogc_fid, bikescore_norm, wkb_geometry, \
        (commutebikepercnorm + intersectionnorm + bikenet_norm + \
        bikepathlengthdensitynorm) as val, \
        min(commutebikepercnorm + intersectionnorm + \
        bikenet_norm + bikepathlengthdensitynorm) over () as min, \
        max(commutebikepercnorm + intersectionnorm + \
        bikenet_norm + bikepathlengthdensitynorm) over () as max \
        from {1}.bikeability) as b
    left join geography.{0} as t
    on st_intersects(st_centroid(b.wkb_geometry),t.wkb_geometry) \
    group by t.geoid10, bic_index \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def travel_time(geography):
    t_schema = schema['b08303']
    fields = ['b08303'+str(i).zfill(3) for i in range(2,14)]
    q = """alter table summary_{0} add column {1} numeric(20,2); update summary_{0} \
        set {1} = subquery.{1} \
        from (select c.geoid, {1}/nullif(b08303001,0)*100 as {1} \
        from {2}.b08303 as c) as subquery \
        where summary_{0}.geoid10 = subquery.geoid;"""
    for field in fields:
        cursor.execute(q.format(geography, field, t_schema))

def travel_mode(geography):
    t_schema = schema['b08301']
    fields = ['b08301002', 'b08301010', 'b08301016', 'b08301018', 'b08301019', 'b08301020']
    q = """alter table summary_{0} add column {1} numeric(20,2); update summary_{0} \
    set {1} = subquery.{1}
    from (
    select  geoid, {1} / nullif(b08301001, 0)*100 as {1} \
    from {2}.b08301 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    for field in fields:
        cursor.execute(q.format(geography, field, t_schema))

def mmode_connection(geography):
    t_schema = schema['multimodalconnection']
    q = """alter table summary_{0} add column mmcnxpsmi numeric(20,2);
    update summary_{0} \
    set mmcnxpsmi = subquery.mmcnxpsmi / sqmiland \
    from \
    (select t.geoid10, count(p.wkb_geometry) as mmcnxpsmi \
    from {1}.multimodalconnection as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def transit_access(geography):
    t_schema = schema['transit_access']
    q = """alter table summary_{0} add column transit_access numeric(20,2);
    update summary_{0} \
    set transit_access = subquery.transit_access \
    from \
    (select t.geoid10, ((((access1 + bus_thirdm + closest_bu) - 0) / 13.3553 - 0) * 100) as transit_access \
    from {1}.transit_access as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10, transit_access \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def mata_routes(geography):
    t_schema = schema['mata_routes']
    col = 'mata_route_sqmi'
    q = """alter table summary_{0} drop column if exists {2},
    add column {2} numeric(20,2);
    update summary_{0} \
    set {2} = subquery.{2} / sqmiland \
    from \
    (select clipped.geoid10, sum(st_length(mata_geom)/5280) as {2} \
        from (select {0}.geoid10, \
        (st_intersection(mata_routes.wkb_geometry, \
            st_buffer({0}.wkb_geometry, .5))) as mata_geom \
    from {1}.mata_routes \
    join geography.{0} \
    on st_intersects(mata_routes.wkb_geometry, {0}.wkb_geometry))  as clipped \
    where st_dimension(clipped.mata_geom) = 1 \
    group by geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def mata_stops(geography):
    t_schema = schema['mata_stops']
    q = """alter table summary_{0} drop column if exists mata_stop_sqmi,
    add column mata_stop_sqmi numeric(20,2);
    update summary_{0} \
    set mata_stop_sqmi = subquery.mata_stop_sqmi \
    from \
    (select t.geoid10, count(p.wkb_geometry) as mata_stop_sqmi \
    from {1}.mata_stops as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def bicyle_facilities(geography):
    t_schema = schema['mpo_bicyclefacilities']
    col = 'bic_sqmi'
    q = """alter table summary_{0} drop column if exists {2},\
    add column {2} numeric(20,2);\
    update summary_{0} \
    set {2} = subquery.{2} / sqmiland \
    from \
    (select clipped.geoid10, sum(st_length(bic_geom)/5280) as {2} \
        from (select {0}.geoid10, \
        (st_intersection(mpo_bicyclefacilities.wkb_geometry, \
            st_buffer({0}.wkb_geometry, .5))) as bic_geom \
    from {1}.mpo_bicyclefacilities \
    join geography.{0} \
    on st_intersects(mpo_bicyclefacilities.wkb_geometry, {0}.wkb_geometry))  as clipped \
    where st_dimension(clipped.bic_geom) = 1 \
    group by geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def bus_rider(geography):
    t_schema = schema['mata_stops']
    q = ("alter table summary_{0} add column rider_sqmi numeric(20,2);"
         "update summary_{0} "
         "set rider_sqmi = subquery.rider_sqmi / sqmiland " 
         "from "
	 "(select t.geoid10, sum(tot) as rider_sqmi "
            "from (select wkb_geometry, sum(sum_passengers_on) tot "
                "from {1}.mata_stops s, "
                    "{1}.mata_ridership r "
                "where r.stop_id = s.stop_id "
                "group by s.stop_id, wkb_geometry) b "
                "join geography.{0} t "
                    "on st_within(b.wkb_geometry, t.wkb_geometry) "
                "group by geoid10) "
            "as subquery "
         "where summary_{0}.geoid10 = subquery.geoid10;")

    # q = """alter table summary_{0} add column rider_sqmi numeric(20,2);
    # update summary_{0} \
    # set rider_sqmi = subquery.rider_sqmi / sqmiland \
    # from \
    # (select t.geoid10, sum(sum_passengers_on) as rider_sqmi \
    # from {1}.mata_stops as p \
    # join geography.{0} as t on \
    # st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    # group by t.geoid10 \
    # ) as subquery \
     # where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def vmt(geography):
    t_schema = schema['htaindex']
    q = """alter table summary_{0} add column vmt_per_hh_ami numeric(20,2); update summary_{0} \
    set vmt_per_hh_ami = subquery.vmt_per_hh_ami \
    from (select c.geoid, vmt_per_hh_ami \
    from {1}.htaindex as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def walkability(geography):
    t_schema = schema['walkability']
    q = """alter table summary_{0} add column walkscore numeric(20,2);
    update summary_{0} \
    set walkscore = subquery.walkscore \
    from \
    (select t.geoid10, \
    (hh_density_norm + intersectiondensity_norm + retaildensity_norm) as walkscore \
    from {1}.walkability as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10, walkscore, hh_density_norm, intersectiondensity_norm, retaildensity_norm \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def car_own(geography):
    t_schema = schema['htaindex']
    q = """alter table summary_{0} add column autos_per_hh_ami numeric(20,2); update summary_{0} \
    set autos_per_hh_ami = subquery.autos_per_hh_ami \
    from (select c.geoid, autos_per_hh_ami \
    from {1}.htaindex as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def tree_canopy(geography):
    t_schema = schema['tree_canopy']
    if geography == 'cen_place_2010':
        q = """alter table summary_{0} add column pct_canopy numeric(20,2);
        update summary_{0} \
        set pct_canopy = subquery.pct_canopy \
        from \
        (select t.geoid10, percent_tree_canopy as pct_canopy \
        from {1}.tree_canopy_muni as p \
        join geography.{0} as t on \
        lower(t.name10) = lower(p.municipali) \
        group by t.geoid10, pct_canopy \
        ) as subquery \
         where summary_{0}.geoid10 = subquery.geoid10;"""
    else:
        q = """alter table summary_{0} add column pct_canopy numeric(20,2);
        update summary_{0} \
        set pct_canopy = subquery.pct_canopy \
        from \
        (select t.geoid10, sum(p.aland10)*0.000247105, (sum(tree_canopy_acres)/
        (sum(p.aland10)*0.000247105)) *100 as pct_canopy \
        from {1}.tree_canopy as p \
        join geography.{0} as t on \
        st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
        group by t.geoid10 \
        ) as subquery \
         where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def elec_station(geography):
    t_schema = schema['alternative_fuel_stations']
    q = """alter table summary_{0} add column elec_chg_station numeric(20,2); \
    update summary_{0} \
    set elec_chg_station = subquery.elec_chg_station \
    from \
    (select t.geoid10, \
    sum(case when fuel = 'Electric' then 1 else 0 end) as elec_chg_station \
    from {1}.alternative_fuel_stations as b \
    join geography.{0} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def green_bldgs(geography):
    t_schema = schema['leed_buildings']
    q = """alter table summary_{0} add column green_bldgs_sqmi numeric(20,2); \
    update summary_{0} \
    set green_bldgs_sqmi = subquery.green_bldgs_sqmi/sqmiland \
    from \
    (select t.geoid10, \
    count(b.wkb_geometry) as green_bldgs_sqmi \
    from {1}.leed_buildings as b \
    join geography.{0} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def air_qual(geography):
    t_schema = schema['epa_airquality']
    if geography == 'cen_county_2010':
        spatial_q = "t.geoid10 = concat(lpad(state_code::text, 2, '0'), \
                lpad(county_code::text, 3, '0'))"
    else:
        spatial_q = 'st_within(st_closestpoint(t.wkb_geometry,b.wkb_geometry), t.wkb_geometry)'
    q = """alter table summary_{0} drop column if exists days_ovr_aq_stndrd,\
            add column days_ovr_aq_stndrd numeric(20,2); \
    update summary_{0} \
    set days_ovr_aq_stndrd = subquery.days_ovr_aq_stndrd \
    from \
    (select t.geoid10, \
    sum(primary_exceedance_count) as days_ovr_aq_stndrd \
    from {1}.epa_airquality  as b \
    join geography.{0} as t on {2} \
    where parameter_name in ('Nitrogen dioxide (NO2)', \
    'Ozone', 'Sulfur dioxide', 'Carbon monoxide') \
    or parameter_name like '%%PM%%' \
    group by t.geoid10 \
    ) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, spatial_q))

def protected_land(geography):
    t_schema = schema['esri_fedland']
    col = 'fed_acre_sqmi'
    q = """alter table summary_{0} drop column if exists {2},
    add column {2} numeric(20,2);
    update summary_{0} \
    set {2} = subquery.{2} / sqmiland \
    from \
    (select t.geoid10, sqmi * 640 as {2} \
    from {1}.esri_fedland as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10, sqmi \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def chg_prop_val(geography):
    """
    TODO:
    adjust query to differentiate between zip codes and other geographies
    the substring where clause doesn't work for zip codes
    """
    from caeser import utils
    from_year_cpi, to_year_cpi = utils.get_cpi(2004, int(parcel_year))
    t_schema = schema['sca_shelby_parcels_{}'.format(parcel_year)]
    col = 'pct_chgprop'
    geog_key ={'cen_county_2010':"subquery.geoid10 = '47157'",
              'cen_place_2010': ("subquery.geoid10 in "
                                 "('4701740','4748000','4716420','4749060',"
                                 "'4740350','4728960','4703440')"),
              'cen_tract_2010': "substring(subquery.geoid10, 1, 5) = '47157'",
              'cen_zip_2010': "subquery.geoid10 in ('{}')".format(
                                        ("','".join(z for z in zip_codes))),
              'cen_msa_2013': "subquery.geoid10 = '32820'"}

    params = {'geography': geography,
              't_schema': schema['sca_shelby_parcels_{}'.format(parcel_year)],
              'col': col,
              'query': geog_key[geography],
              'year': parcel_year,
              'from_cpi': from_year_cpi,
              'to_cpi': to_year_cpi}
    if geography == 'cen_zip_2010':
        apr14 = """alter table summary_{geography} drop column if exists rtotapr14,
        add column rtotapr14 numeric(20,2);
        update summary_{geography}
        set rtotapr14 = subquery.rtotapr14 \
        from \
        (select t.geoid10, sum(rtotapr) as rtotapr14 \
        from ({t_schema}.sca_shelby_parcels_{year} \
        join (select parid, rtotapr from {t_schema}.sca_asmt_{year}) \
        as a on parid = parcelid) as p \
        join geography.{geography} as t on \
        st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
        group by t.geoid10) as subquery \
        where summary_{geography}.geoid10 = subquery.geoid10
        and subquery.geoid10 not in ('72364', '38023')
        and {query};
        """

        apr04 = """alter table summary_{geography} drop column if exists rtotapr04,
        add column rtotapr04 numeric(20,2);
        update summary_{geography}
        set rtotapr04 = subquery.rtotapr04 * ({to_cpi}/{from_cpi}) \
        from \
        (select t.geoid10, sum(rtotapr) as rtotapr04 \
        from ({t_schema}.sca_shelby_parcels_2004 \
        join (select parid, rtotapr from {t_schema}.sca_asmt_2004) \
        as a on parid = parcelid) as p \
        join geography.{geography} as t on \
        st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
        group by t.geoid10) as subquery \
        where summary_{geography}.geoid10 = subquery.geoid10
        and subquery.geoid10 not in ('72364', '38023')
        and {query};
        """
    else:
        apr14 = """alter table summary_{geography} drop column if exists rtotapr14,
        add column rtotapr14 numeric(20,2);
        update summary_{geography}
        set rtotapr14 = subquery.rtotapr14 \
        from \
        (select t.geoid10, sum(rtotapr) as rtotapr14 \
        from ({t_schema}.sca_shelby_parcels_{year} \
        join (select parid, rtotapr from {t_schema}.sca_asmt_{year}) \
        as a on parid = parcelid) as p \
        join geography.{geography} as t on \
        st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
        group by t.geoid10) as subquery \
        where summary_{geography}.geoid10 = subquery.geoid10 \
        and substring(summary_{geography}.geoid10,1,5) = '47157';
        """

        apr04 = """alter table summary_{geography} drop column if exists rtotapr04,
        add column rtotapr04 numeric(20,2);
        update summary_{geography}
        set rtotapr04 = subquery.rtotapr04 * (236.736/188.9) \
        from \
        (select t.geoid10, sum(rtotapr) as rtotapr04 \
        from ({t_schema}.sca_shelby_parcels_2004 \
        join (select parid, rtotapr from {t_schema}.sca_asmt_2004) \
        as a on parid = parcelid) as p \
        join geography.{geography} as t on \
        st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
        group by t.geoid10) as subquery \
        where summary_{geography}.geoid10 = subquery.geoid10 \
        and substring(summary_{geography}.geoid10,1,5) = '47157';
        """


    cursor.execute(apr14.format(**params))
    cursor.execute(apr04.format(**params))
    pct_change = """alter table summary_{geography} drop column if exists {col},
    add column {col} numeric(20,2); \
    update summary_{geography} \
    set {col} = 100 * ((rtotapr14 - rtotapr04) / rtotapr04);
    """
    cursor.execute(pct_change.format(**params))

    rem = """alter table summary_{geography} drop column rtotapr04, drop column rtotapr14;"""
    cursor.execute(rem.format(**params))

def snap_receipt(geography):
    t_schema = schema['b19058']
    q = """alter table summary_{0} add column pct_snap numeric(20,2); update summary_{0} \
    set pct_snap = subquery.pct_snap \
    from (select c.geoid, b19058002/nullif(b19058001,0)*100 as pct_snap \
    from {1}.b19058 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def avg_hours_worked(geography):
    t_schema = schema['b23020']
    q = """alter table summary_{0} add column avg_hours numeric(20,2); update summary_{0} \
    set avg_hours = subquery.avg_hours \
    from (select c.geoid, b23020001 as avg_hours \
    from {1}.b23020 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def job_access(geography):
    t_schema = schema['htaindex']
    q = """alter table summary_{0} add column emp_ovrll_ndx numeric(20,2); update summary_{0} \
    set emp_ovrll_ndx = subquery.emp_ovrll_ndx \
    from (select c.geoid, emp_ovrll_ndx \
    from {1}.htaindex as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def pct_labor_force(geography):
    t_schema = schema['b23025']
    q = """alter table summary_{0} add column pct_labor_force numeric(20,2); update summary_{0} \
    set pct_labor_force = subquery.pct_labor_force \
    from (select c.geoid, \
    case b23025001 \
    when 0 then 0 \
    else 100*b23025002/b23025001 \
    end \
    as pct_labor_force \
    from {1}.b23025 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def emp_mix(geography):
    t_schema = schema['htaindex']
    q = """alter table summary_{0} add column emp_ndx numeric(20,2); update summary_{0} \
    set emp_ndx = subquery.emp_ndx \
    from (select c.geoid, emp_ndx \
    from {1}.htaindex as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def pct_unemp(geography):
    t_schema = schema['b23025']
    q = """alter table summary_{0} add column pct_unemp numeric(20,2); update summary_{0} \
    set pct_unemp = subquery.pct_unemp \
    from (select c.geoid, \
    case b23025002 \
    when 0 then 0 \
    else 100 * b23025005/b23025002 \
    end \
    as pct_unemp \
    from {1}.b23025 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def pct_commercial(geography):
    t_schema = schema['emp_centers']
    col = 'pct_commercial'
    q = """alter table summary_{0} add column {2} numeric(20,2);
    update summary_{0} \
    set {2} = subquery.{2}  \
    from \
    (select t.geoid10, 100 * (cns07 + cns09 + cns10 + cns12 +
    cns13 + cns17 + cns18 + cns19) / c000 as {2} \
    from {1}.emp_centers as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10, {2} \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""

    cursor.execute(q.format(geography, t_schema, col))

# def bus_sector(geography):
#     t_schema = schema['emp_centers']
#     jobs = {'cns01': 'pct_ag', 'cns02': 'pct_mining', 'cns03': 'pct_util',
#             'cns04': 'pct_construction', 'cns05': 'pct_manuf', 'cns06': 'pct_wholesale',
#             'cns07': 'pct_retail','cns08': 'pct_transport', 'cns09': 'pct_info',
#             'cns10': 'pct_finance', 'cns11': 'pct_realestate','cns12': 'pct_prof_services',
#              'cns13': 'pct_mgmt', 'cns14': 'pct_waste_mgmt', 'cns15': 'pct_ed',
#             'cns16': 'pct_health', 'cns17': 'pct_arts', 'cns18': 'pct_food',
#             'cns19': 'pct_other', 'cns20': 'pct_pubadmin'}
#     q = """alter table summary_{0} add column {1} numeric(20,2); \
#     update summary_{0} \
#     set {1} = subquery.{1}  \
#     from \
#     (select t.geoid10, 100 * {2} / c000 as {1} \
#     from {3}.emp_centers as p \
#     join geography.{0} as t on \
#     st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10, {1} \
#     ) as subquery \
#      where summary_{0}.geoid10 = subquery.geoid10;"""
#     for k, val in jobs.iteritems():
#         cursor.execute(q.format(geography, val, k, t_schema))

def emp_sector(geography):
    t_schema = schema['emp_centers']
    jobs = {'cns01': 'pct_ag', 'cns02': 'pct_mining', 'cns03': 'pct_util',
            'cns04': 'pct_construction', 'cns05': 'pct_manuf', 'cns06': 'pct_wholesale',
            'cns07': 'pct_retail','cns08': 'pct_transport', 'cns09': 'pct_info',
            'cns10': 'pct_finance', 'cns11': 'pct_realestate','cns12': 'pct_prof_services',
            'cns13': 'pct_mgmt', 'cns14': 'pct_waste_mgmt', 'cns15': 'pct_ed',
            'cns16': 'pct_health', 'cns17': 'pct_arts', 'cns18': 'pct_food',
            'cns19': 'pct_other', 'cns20': 'pct_pubadmin'}
    q = """alter table summary_{0} drop column if exists {1},
    add column {1} numeric(20,2); \
    update summary_{0} \
    set {1} = subquery.{1}  \
    from \
    (select t.geoid10, 100 * sum({2})/sum(c000) as {1} \
    from {3}.emp_centers as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    for k, val in jobs.iteritems():
        cursor.execute(q.format(geography, val, k, t_schema))

def low_mod_jobs(geography):
    t_schema = schema['emp_centers']
    q = """alter table summary_{0} add column pct_lowinc_job numeric(20,2);
    update summary_{0} \
    set pct_lowinc_job = subquery.pct_lowinc_job  \
    from \
    (select t.geoid10, 100 *  ce01 / c000 as pct_lowinc_job \
    from {1}.emp_centers as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10, pct_lowinc_job \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def ed_attain(geography):
    table = 'b15003'
    t_schema = schema[table]
    q = """alter table summary_{0} add column pct_{1} numeric(20,2); update summary_{0} \
    set pct_{1} = subquery.pct_{1} \
    from (select c.geoid, \
    case b15003001 \
    when 0 then 0 \
    else 100 * {1}/b15003001 \
    end \
    as pct_{1} \
    from {2}.b15003 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    for i in range(16, 26):
        cursor.execute(q.format(geography, table + str(i).zfill(3),t_schema))

def elem_schools(geography):
    t_schema = schema['cpgis_schools']
    col = 'elem_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.cpgis_schools as h
        where fcode = 73003
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, fcode, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.cpgis_schools as h
        where fcode = 73003
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10, fcode) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
#     q = """alter table summary_{0} add column elem_pcap numeric(20,2); \
#     update summary_{0} \
#     set elem_pcap =
#     case totpop \
#     when 0 then 0 \
#     else subquery.elem_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, fcode, \
#     count(b.wkb_geometry) as elem_pcap \
#     from {1}.cpgis_schools as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10, fcode \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10
#     and fcode = 73003;"""
    cursor.execute(q.format(geography, t_schema, col))

def middle_schools(geography):
    t_schema = schema['cpgis_schools']
    col = 'middle_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.cpgis_schools as h
        where fcode = 73004
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, fcode, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.cpgis_schools as h
        where fcode = 73004
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10, fcode) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """

#     q = """alter table summary_{0} add column middle_pcap numeric(20,2); \
#     update summary_{0} \
#     set middle_pcap =
#     case totpop \
#     when 0 then 0 \
#     else subquery.middle_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, fcode, \
#     count(b.wkb_geometry) as middle_pcap \
#     from {1}.cpgis_schools as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10, fcode \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10
#     and fcode = 73004;"""
    cursor.execute(q.format(geography, t_schema, col))

def high_schools(geography):
    t_schema = schema['cpgis_schools']
    col = 'high_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.cpgis_schools as h
        where fcode = 73005
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, fcode, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.cpgis_schools as h
        where fcode = 73005
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10, fcode) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """

#     q = """alter table summary_{0} add column high_pcap numeric(20,2); \
#     update summary_{0} \
#     set high_pcap =
#     case totpop \
#     when 0 then 0 \
#     else subquery.high_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, fcode, \
#     count(b.wkb_geometry) as high_pcap \
#     from {1}.cpgis_schools as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10, fcode \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10
#     and fcode = 73005;"""
    cursor.execute(q.format(geography, t_schema, col))

def pvt_schools(geography):
    t_schema = schema['cpgis_schools']
    col = 'pvt_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.cpgis_schools as h
        where fcode = 73006
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, fcode, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry, fcode,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.cpgis_schools as h
        where fcode = 73006
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10, fcode) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """

#     q = """alter table summary_{0} add column pvt_pcap numeric(20,2); \
#     update summary_{0} \
#     set pvt_pcap =
#     case totpop \
#     when 0 then 0 \
#     else subquery.pvt_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, fcode, pub_pri, \
#     count(b.wkb_geometry) as pvt_pcap \
#     from {1}.cpgis_schools as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10, fcode, pub_pri \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10 \
#     and pub_pri = 'Private'
#     and fcode != 73006;"""
    cursor.execute(q.format(geography, t_schema, col))

def daycare_dist(geography):
    t_schema = schema['cpgis_childcare_cntrs']
    col = 'chldcntr_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.cpgis_childcare_cntrs as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.cpgis_childcare_cntrs as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
#     q = """alter table summary_{0} add column chldcntr_pcap numeric(20,2); \
#     update summary_{0} \
#     set chldcntr_pcap = \
#     case totpop \
#     when 0 then 0 \
#     else subquery.chldcntr_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as chldcntr_pcap \
#     from {1}.cpgis_childcare_cntrs as b \
#     join geography.{0} as t on \
#     st_within(b.wkb_geometry, t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def mdnhhinc_race(geography):
    hh_race_tables = {'a': 'wh', 'b': 'b', 'c': 'na',
                           'd': 'as', 'e':'nh', 'f':'ot',
                           'g':'2r', 'i': 'hp'}
    t_schema = schema['b19013']
    q = """alter table summary_{0} add column {1} numeric(20,2); update summary_{0} \
    set {1} = subquery.{1} \
    from (select c.geoid, {2} as {1} \
    from {3}.{4} as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    for k, val in hh_race_tables.iteritems():
        cursor.execute(q.format(geography, 'mdnhhinc_' + val, 'b19013'+ k + '001',
                       t_schema, 'b19013'+ k))

def community_gardens(geography):
    t_schema = schema['community_gardens_130426']
    col = 'cmgrdn_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.community_gardens_130426 as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.community_gardens_130426 as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """

#     q = """alter table summary_{0} add column cmgrdn_pcap numeric(20,2); \
#     update summary_{0} \
#     set cmgrdn_pcap =
#     case totpop \
#     when 0 then 0 \
#     else subquery.cmgrdn_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as cmgrdn_pcap \
#     from {1}.community_gardens_130426 as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def farmers_markets(geography):
    t_schema = schema['farmers_markets_130426']
    col = 'frmrmkt_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.farmers_markets_130426 as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.farmers_markets_130426 as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
#     q = """alter table summary_{0} add column frmrmkt_pcap numeric(20,2); \
#     update summary_{0} \
#     set frmrmkt_pcap =
#     case totpop \
#     when 0 then 0 \
#     else subquery.frmrmkt_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as frmrmkt_pcap \
#     from {1}.farmers_markets_130426 as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def libraries(geography):
    t_schema = schema['libraries_130610']
    col = 'library_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.libraries_130610 as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.libraries_130610 as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """

#     q = """alter table summary_{0} add column library_pcap numeric(20,2); \
#     update summary_{0} \
#     set library_pcap =
#     case totpop \
#     when 0 then 0 \
#     else subquery.library_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as library_pcap \
#     from {1}.libraries_130610 as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def commcenters_dist(geography):
    t_schema = schema['community_centers_130508']
    col = 'commcenter_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.community_centers_130508 as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.community_centers_130508 as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
#    alter table summary_{0} add column commcenter_pcap numeric(20,2); \
#     update summary_{0} \
#     set commcenter_pcap = \
#     case totpop \
#     when 0 then 0 \
#     else subquery.commcenter_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as commcenter_pcap \
#     from {1}.community_centers_130508 as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def medicaid_pop(geography):
    t_schema = schema['c27007']
    q = """alter table summary_{0} add column pct_medicaid numeric(20,2); update summary_{0} \
    set pct_medicaid = subquery.pct_medicaid \
    from (select c.geoid, \
    case c27007001 \
    when 0 then 0 \
    else 100*(c27007004+c27007007+c27007010+c27007014+c27007017+c27007020)/c27007001 \
    end \
    as pct_medicaid \
    from {1}.c27007 as c) as subquery \
    where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def bikepedinc_pcap(geography):
    t_schema = schema['nhtsa_ped_bike_fatality']
    col = 'bpinc_pcap'
    q = """alter table summary_{0} drop column if exists {2},
    add column {2} numeric(20,2); \
    update summary_{0} \
    set  {2} = subquery.{2}/nullif (totpop, 0) *10000\
    from \
    (select t.geoid10, \
    sum(b.persons) as {2} \
    from {1}.nhtsa_ped_bike_fatality as b \
    join geography.{0} as t on \
    st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
    where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def hospitals(geography):
    t_schema = schema['hospitals']
    col = 'hosp_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.hospitals as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.hospitals as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
#     q = """alter table summary_{0} add column hosp_pcap numeric(20,2); \
#     update summary_{0} \
#     set hosp_pcap = \
#     case totpop \
#     when 0 then 0 \
#     else subquery.hosp_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as hosp_pcap \
#     from {1}.hospitals as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def police_stations(geography):
    t_schema = schema['police_stations']
    col = 'pol_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.police_stations as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.police_stations as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
#     q = """alter table summary_{0} add column polstn_pcap numeric(20,2); \
#     update summary_{0} \
#     set polstn_pcap = \
#     case totpop \
#     when 0 then 0 \
#     else subquery.polstn_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as polstn_pcap \
#     from {1}.police_stations as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def fire_stations(geography):
    t_schema = schema['fire_stations']
    col = 'fire_dist'
    if geography in ['cen_tract_2010', 'cen_zip_2010']:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select distinct on(geoid10) geoid10,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) / 5280 as {2}
        from geography.{0} as t, {1}.fire_stations as h
        order by geoid10, st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry)) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0}
        set {2} = subquery.{2}
        from
        (select p.geoid10, (sum({2})/count(sub.geoid10)) / 5280 as {2} from
        (select distinct on(geoid10) geoid10, t.wkb_geometry,
        st_distance(st_centroid(t.wkb_geometry), h.wkb_geometry) as {2}
        from geography.cen_tract_2010 as t, {1}.fire_stations as h
        order by geoid10) as sub
        join geography.{0} as p on st_within(st_centroid(sub.wkb_geometry),p.wkb_geometry)
        group by p.geoid10) as subquery
        where summary_{0}.geoid10 = subquery.geoid10
        """
#     q = """alter table summary_{0} add column firestn_pcap numeric(20,2); \
#     update summary_{0} \
#     set firestn_pcap = \
#     case totpop \
#     when 0 then 0 \
#     else subquery.firestn_pcap/totpop \
#     end \
#     from \
#     (select t.geoid10, \
#     count(b.wkb_geometry) as firestn_pcap \
#     from {1}.fire_stations as b \
#     join geography.{0} as t on \
#     st_within(st_centroid(b.wkb_geometry), t.wkb_geometry) \
#     group by t.geoid10 \
#     ) as subquery \
#     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def os_sqmi(geography):
    t_schema = schema['cpgis_openspace']
    q = """alter table summary_{0} add column os_sqmi numeric(20,2);
    update summary_{0} \
    set os_sqmi = subquery.os_sqmi / sqmiland \
    from \
    (select t.geoid10, sum(acreage) as os_sqmi \
    from {1}.cpgis_openspace as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def pct_impervious(geography):
    t_schema = schema['tree_canopy']
    col = 'pct_imp'
    if geography == 'cen_place_2010':
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0} \
        set {2} = subquery.{2} \
        from \
        (select t.geoid10, percent_impervious as {2} \
        from {1}.tree_canopy_muni as p \
        join geography.{0} as t on \
        lower(t.name10) = lower(p.municipali) \
        group by t.geoid10, percent_impervious \
        ) as subquery \
         where summary_{0}.geoid10 = subquery.geoid10;"""
    else:
        q = """alter table summary_{0} add column {2} numeric(20,2);
        update summary_{0} \
        set {2} = subquery.{2} \
        from \
        (select t.geoid10, sum(p.aland10)*0.000247105, (sum(impervious_acres)/ \
        (sum(p.aland10)*0.000247105)) *100 as {2} \
        from {1}.tree_canopy as p \
        join geography.{0} as t on \
        st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
        group by t.geoid10 \
        ) as subquery \
         where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema, col))

def wetlands_sqmi(geography):
    t_schema = schema['nwi_wetlands']
    q = """alter table summary_{0} add column wetland_sqmi numeric(20,2);
    update summary_{0} \
    set wetland_sqmi = subquery.wetland_sqmi / sqmiland \
    from \
    (select t.geoid10, wetland_type, sum(acres) as wetland_sqmi \
    from {1}.nwi_wetlands as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10, wetland_type \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10 \
     and wetland_type like '%%Wetland%%';"""
    cursor.execute(q.format(geography, t_schema))

def brownfld_sqmi(geography):
    t_schema = schema['tristate_brownfield']
    q = """alter table summary_{0} add column brnfld_sqmi numeric(20,2);
    update summary_{0} \
    set brnfld_sqmi = subquery.brnfld_sqmi / sqmiland \
    from \
    (select t.geoid10, count(p.wkb_geometry) as brnfld_sqmi \
    from {1}.tristate_brownfield as p \
    join geography.{0} as t on \
    st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) \
    group by t.geoid10 \
    ) as subquery \
     where summary_{0}.geoid10 = subquery.geoid10;"""
    cursor.execute(q.format(geography, t_schema))

def hisp_pop(geography):
    t_schema = schema['b03003']
    q = """alter table summary_{0} add column pct_hisp numeric(20,2);
            update summary_{0} \
                set pct_hisp =
                case when subquery.b03003003 = 0 then 0
                else subquery.b03003003/summary_{0}.totpop *100\
                end \
        from (select c.geoid, b03003003 \
        from {1}.b03003 as c) as subquery \
        where summary_{0}.geoid10 = subquery.geoid;"""
    cursor.execute(q.format(geography, t_schema))

def broadband(geography):
    t_schema = schema['fcc_broadband']
    q = """alter table summary_{0} add column pcat_10 numeric(20);\
            update summary_{0} \
            set pcat_10 = \
                case when pcat_10x1 = 0 then 0 \
                when pcat_10x1 = 1 then 200 \
                when pcat_10x1 = 2 then 400 \
                when pcat_10x1 = 3 then 600 \
                when pcat_10x1 = 4 then 800 \
                when pcat_10x1 = 5 then 1000 \
                end\
            from (select tractcode, pcat_10x1 from {1}.fcc_broadband) sub \
            where summary_{0}.geoid10 = sub.tractcode"""
    cursor.execute(q.format(geography, t_schema))

def life_exp(geography):
    t_schema = schema['schd_life_exp_0913']
    if geography == 'cen_zip_2010':
        params = {'geography': geography, 't_schema':t_schema, 
                'column': 'life_exp'} 
        q = """alter table summary_{geography} add column {column} integer;
                update summary_{geography} set {column} = subquery.{column} \
                from (select zipcode, {column} from {t_schema}.schd_life_exp_0913) subquery \
                    where summary_{geography}.geoid10 = subquery.zipcode;"""
        cursor.execute(q.format(**params))

def mortality(geography):
    t_schema = schema['schd_mortality_0913']
    if geography == 'cen_zip_2010':
        for col in ['heart_disease','neoplasm','cerebrovascular',
                    'lower_resp','diabetes','alzheimers']:
            params = {'geography':geography, 't_schema':t_schema,
                      'column':col}
            q = """alter table summary_{geography} add column {column} integer;
                update summary_{geography} set {column} = subquery.{column} \
                from (select zipcode, {column} from {t_schema}.schd_mortality_0913) subquery \
                where summary_{geography}.geoid10 = subquery.zipcode;"""
            cursor.execute(q.format(**params))

def tdh_health(geography):
    if geography == 'cen_tract_2010':
        t_schema = schema['tdh_indicators']
        for col in ['births11_15', 'deaths10_14', 'neoplasms', 'heartdis']:
            params = {'geography': geography, 'column': col, 
                      'schema': schema}
            q = """alter table summary_{geography} add column {column} numeric(20,2);
                update summary_{geography} set {column} = tdh_indicators.{column}\
                from health.tdh_indicators where geoid10 = geoid;"""
            cursor.execute(q.format(**params))

def mlgw_rates(geography):
    if geography == 'cen_zip_2010':
        params = {'schema' : schema['mlgw_rates'], 'geography': geography} 

        q = """alter table summary_{geography} 
                drop column if exists krate_res,
                add column krate_res numeric(20,2),
                drop column if exists krate_apt,
                add column krate_apt numeric(20,2),
                drop column if exists grate_apt,
                add column grate_apt numeric(20,2),
                drop column if exists grate_res,
                add column grate_res numeric(20,2),
                drop column if exists wrate_res,
                add column wrate_res numeric(20,2),
                drop column if exists wrate_apt,
                add column wrate_apt numeric(20,2);
                update summary_{geography} set 
                krate_res =
                (select avg_mo from {schema}.mlgw_rates where meter = 'RES'
                and scat = 'K' and geoid10 = zip),
                krate_apt = 
                (select avg_mo from {schema}.mlgw_rates where meter = 'APT'
                and scat = 'K' and geoid10 = zip),
                grate_res = 
                (select avg_mo from {schema}.mlgw_rates where meter = 'RES'
                and scat = 'G' and geoid10 = zip),
                grate_apt = 
                (select avg_mo from {schema}.mlgw_rates where meter = 'APT'
                and scat = 'G' and geoid10 = zip),
                wrate_res = 
                (select avg_mo from {schema}.mlgw_rates where meter = 'RES'
                and scat = 'W' and geoid10 = zip),
                wrate_apt = 
                (select avg_mo from {schema}.mlgw_rates where meter = 'APT'
                and scat = 'W' and geoid10 = zip)
               """
        cursor.execute(q.format(**params))

# def bcs_summary(geography):
    # """
    # BCS data were loaded from raw bcs_property table for properties that had
    # a rating greater than 0 to ignore properties that were not surveyed
    # or were missing that score

    # litter was converted to an ordinal scale with following guidelines:
	# Low -> 1
	# Medium -> 2
	# High -> 3
	# everything else -> 0
    # occupancy was converted to binary with following guidelines:
	# Unoccupied -> 1
	# everything else -> 0"""



    # for col in ['litter', 'occupancy', 'rating']:
        # q = """alter table summary_{geography} add column {col} numeric(20,2);\
		# update summary_{geography}\
		# set {col} = subquery.{col}\
		# from \
                # (select t.geoid10,
                # case when sum({col}) > 0 then 100 * sum({col}) / count(parcelid)\
                        # else 0
                # end {col}
                # from {t_schema}.{bcs} b
                # join geography.{geography} as t on \
                    # st_within(st_centroid(b.wkb_geometry), t.wkb_geometry)\
                    # group by t.geoid10) as subquery
                # where summary_{geography}.geoid10 = subquery.geoid10		
	# """


	   
    # params = {'geography': geography,
              # 't_schema': schema['sca_shelby_parcels_{}'.format(parcel_year)],
              # 'col': col,
              # 'query': geog_key[geography],
              # 'bcs':'mph_bcs_2015'}
    
