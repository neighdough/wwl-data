"""
This module contains methods to help with pushing annual updates for WWL up
to main database. Some of the methods will add new data to the project
database.

Usage:
    updates.py <year> acs <acs_year>
    updates.py <year> air_quality
    updates.py <year> brownfields
    updates.py <year> assessor
    updates.py <year> fars <fars_year>
    updates.py <year> usgbc

Options:
    -h, --help      :show help document
    <year>   :WWL database year to update (e.g. 2016 = livability_2016, 
                        2017 = livability_2017, etc.)
    acs             :add new data from American Community Survey
    air_quality     :EPA air quality data
    brownfields     :EPA brownfield data
    assessor        :Shelby County Assessor data
        <assessment_year>
    fars            :NHTSA Fatality Analysis Reporting System data
        <fars_year> :year that FARS data represents
    usgbc           :U.S. Green Building Council LEED data

Example:
    $ python updates.py 2018 acs 2016
    $ python updates.py 2018 air_quality
    $ python updates.py 2018 fars 2017

"""

import pandas as pd
import sys
sys.path.append('/home/nate/source/')
from caeser import utils
from config import cnx_params
import os
import jedi
from pyproj import Proj, transform
from sqlalchemy import text
from sqlalchemy.engine import reflection
import re
from docopt import docopt
import subprocess

engine15 = utils.connect(**cnx_params.wwl_2015)
engine16 = utils.connect(**cnx_params.wwl_2016)
engine17 = utils.connect(**cnx_params.wwl_2017)
engine_census = utils.connect(**cnx_params.census)
wwl = utils.connect(**cnx_params.wwl)
wwl_test = utils.connect(**cnx_params.wwl_test)

pt = 'SRID=2274;POINT({0} {1})'
geom = lambda x: pt.format(x['lon'], x['lat']) if x['lon'] != 'NaN' else None
wgs84 = Proj(init='epsg:4326', preserve_units=True)
tnspf = Proj(init='epsg:2274', preserve_units=True)

CITIES = ['Anthonyville', 'Clarkedale', 'Crawfordsville', 'Earle', 
          'Edmondson', 'Gilmore', 'Horseshoe Lake', 'Jennette', 'Jericho', 
          'Marion', 'Sunset', 'Turrell', 'West Memphis', 'Bridgetown', 
          'Byhalia', 'Coldwater', 'Hernando', 'Holly Springs', 'Horn Lake', 
          'Lynchburg', 'North Tunica', 'Olive Branch', 'Potts Camp', 
          'Senatobia', 'Southaven', 'Tunica', 'Tunica Resorts', 'Walls', 
          'White Oak', 'Arlington', 'Atoka', 'Bartlett', 'Braden', 
          'Brighton', 'Burlison', 'Collierville', 'Covington', 'Gallaway', 
          'Garland', 'Germantown', 'Gilt Edge', 'La Grange', 'Lakeland', 
          'Mason', 'Memphis', 'Millington', 'Moscow', 'Munford', 'Oakland', 
          'Piperton', 'Rossville', 'Somerville', 'Williston'] 
COUNTIES = ['shelby', 'tipton', 'fayette', 'marshall', 'desoto', 'tate',
            'tunica', 'crittenden']
STATES = ['TN', 'MS', 'AR']   

SOURCES = ["2016 Shelby County Assessor''s Certified Roll",
           "Memphis Area Transit Authority",
           "Memphis Light, Gas, and Water",
           "Memphis Urban Area Metropolitan Planning Organization",
           "U.S. Census Bureau; American Community Survey, 2010-2014",
           ("U.S. Environmental Protection Agency Air Quality System, "
                "Annual Summary File 2016"),
           "U.S. Environmental Protection Agency, Uniform Resource Locator",
           "U.S. Green Building Council",
           ("Source: National Highway Traffic Safety Administration "
               "Fatality Analysis Reporting System")]

def add_geometry(row, lon, lat, srid='2274'):
    """
    Creates WKT string used to add new point to PostGIS table.
    Args:
        row:
        lon (float): x-coordinate
        lat (float): y-coordinate
        srid (str): spatial reference id. defualt is TN State Plane
    """
    pt = 'SRID={0};POINT({1} {2})'
    return pt.format(srid, row[lon], row[lat])

def convert_coord(row, lon, lat):
    """
    Method to convert point from WGS 84 to TN State Plane. Method is written
    to work as part of a lambda function run against a Pandas dataframe
    Args:
        row: row from Pandas dataframe
        lon (float): x-coordinate
        lat (float): y-coordinate
        srid (str): spatial reference id. Default is TN State Plane
    """
    new_x, new_y = transform(wgs84, tnspf, 
            row[lon], row[lat])
    return (new_x, new_y)

def usgbc():
    """
    Data downloaded from usgbc.org/projects
    """
    bldg_old = pd.read_sql('select * from environment.leed_buildings', engine16)
    os.chdir('./usgbc')
    df = pd.read_csv('PublicLEEDProjectDirectory.csv', sep="\t")
    df.columns = [col.lower() for col in df.columns]
    df = df[(df.state.isin(STATES)) & 
            (df.city.isin(CITIES)) & 
            (df.isconfidential == 'No') &
            (~df.id.isin(bldg_old.id.tolist()))]
    match, unmatch = utils.geocode(df, index='id')
    match = match.append(unmatch, ignore_index=True)
    match['wkb_geometry'] = match[['lon', 'lat']].apply(geom, axis=1)
    match.drop(['lat', 'lon'], axis=1, inplace=True)
    df = df.merge(match[['ResultID','wkb_geometry']], 
            left_on='id', right_on='ResultID')
    ##df.drop(['unnamed: 19', 'unnamed: 20','ResultID'], axis=1, inplace=True)
    df.drop(['ResultID'], axis=1, inplace=True)
    df.to_sql('leed_buildings', wwl_engine, index=False,
            schema='environment', if_exists='append')

def air_quality():
    """
    https://aqs.epa.gov/aqsweb/airdata/download_files.html#Annual
    """
    os.chdir('./epa')
    geoids = ['05035', '28137', '28033', '28093', 
              '28143', '47167', '47047', '47157']
    df = pd.read_csv('annual_conc_by_monitor_{}.csv'.format(update_year))
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    df.state_code = df.state_code.astype(str)
    df.county_code = df.county_code.astype(str)
    df.state_code = df.state_code.str.zfill(2)
    df.county_code = df.county_code.str.zfill(3)
    df = df[df.state_code.str.cat(df.county_code).isin(geoids)]
    df['lon'], df['lat'] = zip(*df.apply(convert_coord, axis=1,
                                         args=("longitude","latitude")))
    df['wkb_geometry'] = df[['lon', 'lat']].apply(geom, axis=1)
    df.drop(['latitude', 'longitude', 'lon', 'lat'], axis=1, inplace=True)
    df.to_sql('epa_airquality', wwl_engine,
            schema='environment', if_exists='replace')

def brown_fields():
    """
    https://www.epa.gov/enviro/epa-frs-facilities-state-single-file-csv-download
    """
    os.chdir('./epa')
    df = pd.read_csv('./NATIONAL_SINGLE.CSV', quotechar='"')
    df.columns = [col.lower() for col in df.columns]
    df = df[(df.state_code.isin(STATES)) & 
            (df.county_name.str.lower().isin(COUNTIES))]
    df['lon'], df['lat'] = zip(*df.apply(convert_coord, axis=1,
                                         args=("longitude83","latitude83")))
    df['wkb_geometry'] = df[['lon', 'lat']].apply(geom, axis=1)
    df.drop(['latitude83', 'longitude83', 'lat', 'lon'], axis=1, inplace=True)
    df.to_sql('tristate_brownfield', wwl_engine, 
            schema='environment', if_exists='replace')

def assessor(year):
    """
    """
    os.chdir("/home/nate/sharedworkspace/Data/Assessor/"+year)
    tables = ["addn", "agland", "asmt", "comdat", 
              "comintext", "dweldat", "pardat"]
    q_info_schema = ("select table_schema, table_name "
                     "from information_schema.tables "
                     "where substring(table_name, 1, 4) = 'sca_' "
                     "and right(table_name, 4) <> '2004' "
                     "and table_name not like '%_parcels_%'")
    sca_tables = pd.read_sql(text(q_info_schema), wwl_engine)
    dtypes = {"LUC": str,
              "ADRNO": str,
              "ZIP1": str}

    new_cols = lambda cols: [col.lower() for col in cols]
    for i in sca_tables.index:
        table_schema, table_name = sca_tables.loc[i]
        print(table_name)
        csv_name = "./{}.txt".format(table_name.split('_')[1].upper())
        df = pd.read_csv(csv_name, dtype=dtypes) 
        df.columns = new_cols(df.columns)
        if 'asmt' in table_name:
            table_name += "_{}".format(year)
        df.to_sql(table_name, wwl_engine, 
                schema=table_schema, if_exists='replace')
    print "Uploading parcel shapefile\n"
    shp = [f for f in os.listdir(".") if f.endswith("shp")][0].split(".")[0]
    cmd = ("shp2pgsql -s 2274 -g wkb_geometry -I -D {shp} built_env.sca_shelby_parcels_{year} "
           "| -h caeser-geo.memphis.edu -d livability_{year} -U postgres"
           )

    subprocess.call(cmd.format(shp=shp, year=year), shell=True)
    #TODO
    #read all values as floats, and convert back to integer after loading
    #to handle the null value issue
    # for table in tables:
        # df = pd.read_csv("select * from environment.sca_pardat limit 1", 
                # engine15)
        # df = pd.read_csv(table.upper()+".txt")

def acs(acs_year):
    """
    """
    schema_census = "acs5yr_" + acs_year
    #query to pull all census tables using regex
    q_acs_tables = ("select table_schema, "
                    "array_to_string(regexp_matches("
                        "table_name, '^([a-z][0-9]+[a-z]?)'), ', ') table_name "
                    "from information_schema.tables "
                    "where table_schema not in ('information_schema', "
                        "'tiger', 'pg_catalog')"
                    "order by table_name")

    q_census = ("select * from {schema_census}.{tbl} "
                "where (sumlevel in ('050', '160', '140') "
                   "or (sumlevel = '860' and geoid in "
                           "(select zip.geoid10 from tiger.zcta510 zip, " 
                                "(select geom from tiger.cbsa10 "
                                    "where cbsafp10 = '32820') msa "
                            "where st_within(st_centroid(zip.geom), msa.geom)))"
    	            "or (sumlevel = '310' and geoid = '32820')) "
                "and substring(fileid, 5,1) = 'e'")
    
    acs_tables = pd.read_sql(q_acs_tables, wwl_engine)
    for i in acs_tables.index:
        schema, tbl = acs_tables.loc[i]
        print(schema, tbl)
        census = pd.read_sql(q_census.format(schema_census=schema_census,
                                             tbl=tbl), engine_census)
        census.to_sql(tbl, wwl_engine, schema=schema, if_exists='replace')


def fars(fars_year):
    """
    ftp://ftp.nhtsa.dot.gov/fars/
    """
    os.chdir('./nhtsa')
    accident = pd.read_csv('./FARS{}NationalCSV/accident.csv'.format(fars_year))
    pbtype = pd.read_csv('./FARS{}NationalCSV/PBType.csv'.format(fars_year))
    new_cols = lambda cols: [col.lower() for col in cols]
    states = [5, 28, 47]
    accident.columns = new_cols(accident.columns)
    pbtype.columns = new_cols(pbtype.columns)
    accident = accident[accident.state.isin(states)]
    pbtype = pbtype[pbtype.state.isin(states)]
    accident = accident[accident.st_case.isin(pbtype.st_case.unique())]
    accident['lon'], accident['lat'] = zip(*accident.apply(convert_coord, 
                                        axis=1, args=('longitud', 'latitude')))
    accident['wkb_geometry'] = accident.apply(add_geometry, axis=1, 
                                            args=('lon', 'lat'))
    accident.to_sql('nhtsa_ped_bike_fatality', wwl_engine, 
                        schema='health', if_exists='replace')
    cur = wwl_engine.connect()
    cur.execute(("alter table health.nhtsa_ped_bike_fatality "
                   "alter column wkb_geometry type geometry(Point, 2274)"))

def data_source_and_dict(limit=True):
    """
    Generates data descriptions and sources for variables to be added to WWL.
    Initially set up to limit new data to only include variables written into
    the scope of work, but can be used to include everything by running method 
    twice and changing the value of 'limit' from True to False which will negate
    the check to see if values are in the SOURCES list.
    
    Args:
        limit (bool): Flag that determines whether the resulting records should
            be limited to the sources provided in the SOURCES list. If true,
            only variables that are derived from one of the sources will be 
            included, if False, only variables NOT in the sources list will
            be included.
    """
    
    def adjust_year(df, field_name, vals=None):
        unique_vals = SOURCES if not vals else vals
        r = re.compile(r'[0-9]{4}')
        for val in unique_vals:
            new_source = val
            for yr in r.findall(val):
                new_source = new_source.replace(yr, str(int(yr)+1))
            df.loc[df[field_name]==val, field_name] = new_source
    
    #Update years in source descriptions
    q_ds = ("select description, descid, source, title, categoryid "
            "from data_sources "
            "where source {limit} in ('{source}') "
            "and right(descid, 2) = '16'")
    params = {"source": "','".join(SOURCES),
              "limit": "" if limit else "not"
             }
    ds = pd.read_sql(q_ds.format(**params), con=wwl_engine)
    ds.descid = ds.descid.str[:-2] + '17'
    adjust_year(ds, 'source')
    ds.source = ds.source.str.replace("'", "''")
    ds.to_sql('data_sources', wwl_engine, if_exists='append', index=False)

    #update data descriptions
    q_dd = ("select units, field, descid citation, field_desc "
            "from data_dictionary "
            "join (select descid from data_sources "
            "where source {limit} in ('{source}') and right(descid, 2) = '16') ds "
            "on descid = citation")
    dd = pd.read_sql(q_dd.format(**params), con=wwl_engine)
    dd.citation = dd.citation.str[:-2] + '17'
    adjust_year(dd, 'units', dd.units.unique().tolist())
    dd.to_sql('data_dictionary', wwl_engine, index=False, if_exists='append')

def update_wwl_json(wwl_year):
    """converts data from livability_<year> database into json to be loaded
    into wwl_phase2 database for display on website. Data are pullled from
    summary_cen_<geography> or legend_values tables in the public schema and
    each row is converted to a json object and placed into a list to be
    pushed into the compiledgeographies.csv column in WWL_PHASE2 db.

    Args:
        year (string): year of WWL database that data needs to be converted from
            * 2015 -> livability_2015
            * 2016 -> livability_2016
            * 2017 -> livability_2017
    Returns:
        None

    """
    wwl_db = 'wwl-test'
    wwl_params = cnx_params.wwl_test
    engine = utils.connect(**cnx_params.engine_wwl)#connect(host, 'postgres', db)
    insp = reflection.Inspector.from_engine(engine)
#    engine_wwl = utils.connect(**wwl_params)
    tables = [t for t in insp.get_table_names(schema='public') \
            if t not in ['spatial_ref_sys', 'data_sources']]
    compiledgeog = pd.read_sql("select * from compiledgeographies", wwl_engine,
                                index_col='id')
    geographies = pd.read_sql("select * from geographies", wwl_engine,
                                index_col='id')
#    cnx = engine_wwl.connect()
    for table in tables:
        if table.split('_')[-1] == 'chng':
            year = 'chng'
        else:
            year = wwl_year #db.split('_')[-1]
        if table == 'legend_values':
            geotype = 'legend'
        elif 'data_dictionary' in table:
            geotype = 'datadictionary'
        else:
            geotype = table.split('_')[2]
        gid = geographies[(geographies.year==year)&
                (geographies.geotype==geotype)].index.values[0]

        df = pd.read_sql('select * from {}'.format(table), engine)
        j_val = df.to_json(orient='records').replace("'","''")
        update = "update compiledgeographies \
                    set csv = '{0}' where geographyid = {1}".format(j_val,gid)
        cnx.execute(update.replace('%','%%'))

def update_wwl_db():
    """
    Pulls new records from wwl-test that aren't in wwl and appends them to
    wwl tables.
    """
    q_gids = "select id from geographies"
    gids = [[i for i in g][0] for g in  wwl.execute(q_gids).fetchall()] 
    tables = ["compiledgeographies", "geographies", "geographylabels",
              "geographyvalues"
             ]
    for table in tables:
        q = "select * from {table} where {column} not in ({ids})"
        vals = {"table":table,
                "column": "geographyid" if table <> "geographies" else "id",
                "ids": ",".join([str(i) for i in gids])
                }
        df_wwl_test = pd.read_sql(q.format(**vals), wwl_test)
        df_wwl_test.to_sql(table, wwl, if_exists="append", index=False)


def main(args):
    if args["acs"]:
        year = args["<acs_year>"]
        acs(year)
    elif args["air_quality"]:
        air_quality()
    elif args["assessor"]:
        assessor(update_year)
    elif args["brownfields"]:
        brown_fields()
    elif args["fars"]:
        fars(args["<fars_year>"])
    elif args["usgbc"]:
        usgbc()
    elif args["update_wwl"]:
        update_wwl_db()
    elif args["all"]:
        acs(args["<year>"])
        air_quality()
        assessor()
        brown_fields()
        fars()
        usgbc()


if __name__=="__main__":
    args = docopt(__doc__)
    update_year = args["<year>"]
    os.chdir('/home/nate/dropbox-caeser/Data/CFGM/WWL/{}/data'.format(update_year))
    wwl_db = getattr(cnx_params, "wwl_"+args["<year>"])
    wwl_engine = utils.connect(**wwl_db)
    main(args)


