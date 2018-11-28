"""
This module contains all of the methods needed to create and maintain the project database
for the WhereWeLiveMidsouth.org web application.

Usage:
    wwldb.py newdb <year>
    wwldb.py update_json -db

Options:
    newdb               :create new project database and load data from previous year
    update_json         :update all JSON objects in existing database to reflect any 
                         changes to data
    year                :Database year
    -d, --database      :Name of the database used for connection parameters

Example:
    $ python wwldb.py newdb 2018
    $ python wwldb.py update_json livability_2017

"""


import sys
sys.path.append('/home/nate/dropbox/dev')
from caeser import utils
from config import cnx_params
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import schema, create_engine, engine, Table, MetaData
from sqlalchemy.sql import select
from sqlalchemy.engine import reflection
from sqlalchemy.types import Numeric
import subprocess
from collections import defaultdict
import os
import pandas as pd
import numpy as np
from docopt import docopt


def make_new_db(year):
    """
    Creates new database for data processing and storage and then copies data from 
    previous data update using pg_dump. Before creating new database, it will verify
    that it doesn't exist prompting the user to verify that they want to create a new one
    if it does.

    Args:
        year (str): year in YYYY format

    Returns:
        None
    """
    prev_year = str(int(year) - 1)
    params = getattr(cnx_params, "wwl_"+prev_year) #cnx_params.wwl_2017
    new_db = "livability_{}".format(year)
 
    #get schema for previous year to load new data
    engine = connect(**params)
    q_schema = ("select distinct(table_schema) "
                "from information_schema.tables "
                "where table_schema not in "
                    "('pg_catalog', 'topology', 'tiger', 'information_schema')"
                )
    schema = [s[0] for s in engine.execute(q_schema).fetchall()]

    params["db"] = new_db
    params["dbname"] = params.pop("db")
    #first check to make sure db doesn't already exist to avoid deleting current db
    cmd = "psql -h {host} -U {user} -lqt | cut -d \| -f 1 | grep -qw {dbname}"
    proc = subprocess.Popen(cmd.format(**params), stdin=subprocess.PIPE, shell=True)
    proc.wait()
    result = proc.poll()
    params["dbname"] = "postgres"
    con = psycopg2.connect(**params)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    if result == 0:
        msg = ("livability_{} already exists and continuing will delete all data."
               " Do you wish to proceed? Y or N\n")
        proceed = raw_input(msg.format(year))
        if proceed.lower() not in ["yes", 'y']:
           return
        else:
           cur.execute("drop database livability_{}".format(year))


    #create new database
    q_new = ("create database livability_{year} "
             "owner postgres")
    cur.execute(q_new.format(**{"year":year}))
    
    #load data from previous year's database to new database
    params["dbname"] = new_db
    params["db_old"] = new_db[:-4] + prev_year#str(int(year) - 1)
    cmd_pg_dump = ("pg_dump -h {host} -U {user} {db_old} | "
                   "psql -h {host} -U {user} -d {dbname}")
    print "Running backup and restore operation from {db_old} to {dbname}".format(**params)
    pg_dump = subprocess.Popen(cmd_pg_dump.format(**params), shell=True) 
    pg_dump.wait()
    cur.close()

def connect(host, user, db, port='5432', password=""):
    params = utils.connection_properties(host, user, port)
    #sqlalchemy connection string in format
    #dialect+driver://username:password@host:port/database
    cnxstr = 'postgresql://{0}:{1}@{2}:{3}/{4}'
    engine = create_engine(cnxstr.format(user, params['password'], host,
                                         params['port'],db))
    return engine

def build_schema():
    schemas = ['demographics', 'built_env', 'housing', 'transportation',
              'climate_energy', 'economy', 'education', 'arts', 'civic',
              'health', 'environment', 'funding']
    engine = connect(host, 'postgres')
    connection = engine.connect()
    for item in schemas:
        print item
        connection.execute(schema.CreateSchema(item))
    connection.close()

def create_summary_tables():
    engine = connect(host, 'postgres', db)
    cursor = engine.connect()
    q = ("select distinct table_name from information_schema.columns "
            "where table_schema = 'geography'")
    create =  ("create table public.summary_{0} as select t.geoid10, t.sqmiland, "
                "t.name10, t.acreland "
                "from geography.{0} as t;"
                 "ALTER TABLE summary_{0} ADD COLUMN id SERIAL;"
                 "UPDATE summary_{0} "
                 "SET id = nextval(pg_get_serial_sequence('summary_{0}','id'));"
                 "ALTER TABLE summary_{0} ADD PRIMARY KEY (id);")
    geographies = cursor.execute(q)
    for geography in geographies:
        cursor.execute(create.format(geography['table_name']))
    cursor.close()

def create_role():
    role = ("create role livability login;"
            "revoke connect on database livability from public;"
            "grant all privileges on database livability to livability;"
            "grant connect on database livability to livability;"
            )
    cursor = connect(host, 'postgres')
    cursor.execute(role)
    cursor.close()

def load_gdb():
    import os
    import csv
    import collections
    import arcpy
    print 'Pulling ArcGIS tables from geodatabase'
    arcpy.env.workspace = "S:/Projects/Current/HUD/LivabilityDashboard/Data/Incoming/LD_Incoming.gdb"
    tbls = [t for t in arcpy.ListTables()]
    gdb_feats = []#collections.defaultdict(lambda: collections.defaultdict(str))
    for dirname, paths, files in arcpy.da.Walk(datatype = 'FeatureClass'):
        gdb_feats.extend(files)


    fcs = collections.defaultdict(lambda: collections.defaultdict(str))
    engine = connect(host, 'postgres')
    cursor = engine.connect()
    with open('S:\Projects\Current\HUD\LivabilityDashboard\Docs\data_list_schema.txt', 'r') as r:
        reader = csv.reader(r)
        for row in reader:
            fcs[row[2]]['schema'] = row[0]
            fcs[row[2]]['directory'] = row[1]
            fcs[row[2]]['table'] = row[2]
    cmd_spatial = ('ogr2ogr -t_srs "EPSG:2274" -f "PostgreSQL" '
                   '"PG: dbname=livability '
                   'host=caeser-geo.memphis.edu user=postgres" '
                   '"S:/Projects/Current/HUD/LivabilityDashboard/Data/Incoming/LD_Incoming.gdb" '
                   '"{0}" -nln {1}.{0} -nlt PROMOTE_TO_MULTI -clipdst '
                   '"S:/Projects/Current/HUD/LivabilityDashboard/Data/Incoming/cen_msa_2013.shp"')
    cmd_tabular = ('ogr2ogr -f "PostgreSQL" "PG: dbname=livability '
                   'host=caeser-geo.memphis.edu user=postgres" '
                   '"S:/Projects/Current/HUD/LivabilityDashboard/Data/Incoming/LD_Incoming.gdb" '
                   '"{0}" -nln {1}.{0}')

    for tbl in gdb_feats:
        print '\nLoading ', tbl
        build_gindex(fcs[tbl]['schema'], tbl, cursor)

    for tbl in tbls:
        print '\nLoading ', tbl
        os.system(cmd_tabular.format(tbl, fcs[tbl]['schema']))
    cursor.close()

def build_gindex(schema = '', tbl = '', cursor = None):
    """
    Add spatial index to table
    input:
    tbl -> table name
    cursor -> sqlalchemy engine object
    """

    gix = "create index {0}_gix on {1}.{0} using gist(wkb_geometry)"

    print '\tAdding spatial index'
    cursor.execute(gix.format(tbl, schema))

def build_geoid_idx():
    engine = connect(host, 'postgres')
    cursor = engine.connect()
    tbl_q = """select * from information_schema.columns where \
            substring(column_name, 1,5) = 'geoid'"""
    idx = "create index {2}_{0}_idx on {1}.{2} ({0});"
    rows = cursor.execute(tbl_q)
    for row in rows:
        print idx.format(row[3], row[1], row[2])
        cursor.execute(idx.format(row[3], row[1], row[2]))
    cursor.close()

def build_parcelid_idx():
    engine = connect(host, 'postgres')
    cursor = engine.connect()
    tbl_q = """select * from information_schema.columns where \
            substring(table_name, 1,3) = 'sca'"""
    idx = "create index {2}_{0}_idx on {1}.{2} ({0});"
    rows = cursor.execute(tbl_q)
    for row in rows:
        tbl = row[2]
        schema = row[1]
        if row[3] in ['parcelid', 'parid']:
            parid = row[3]
            print idx.format(parid, schema, tbl)
            cursor.execute(idx.format(parid, schema, tbl))
    cursor.close()

def clean_up(tbl = '', cursor = None):
    vacuum = "vacuum analyze {}"
    print '\tVacuuming table'
    cursor.execute(vacuum.format(tbl))

def calc_sqmi_land(tbl):
    print "Converting aland10 to square miles on table ", tbl
    engine = connect(host, 'postgres')
    connection = engine.connect()
    alter = """alter table geography.{0} add column sqmiland float """
    update = """update geography.{0} set {0}.sqmiland = {0}.aland10/2589988"""
    connection.execute(alter.format(tbl))
    connection.execute(update.format(tbl))
    connection.close()

def calc_acre_land(tbl):
    print "Converting aland10 to acres on table ", tbl
    engine = connect(host, 'postgres')
    connection = engine.connect()
    alter = """alter table geography.{0} add column acreland float """
    update = """update geography.{0} set acreland = {0}.aland10/4046.86"""
    connection.execute(alter.format(tbl))
    connection.execute(update.format(tbl))
    connection.close()

def db_fields(table, schema):
    engine = connect(host, 'postgres',db)
    insp = reflection.Inspector.from_engine(engine)
    columns = insp.get_columns(table, schema=schema)
    return [c['name'] for c in columns]
    # cursor = engine.connect()
    # q = "select * from information_schema.columns where table_name = " +\
    # "'{}' and substring(column_name, 1,1) = 'b'"
    # tbl = cursor.execute(q.format(table))
    # columns = ','.join(row['column_name'] for row in tbl)
    # cursor.close()
    # return columns

def subquery_fields(fields):
    q = "{0} = subquery.{0}"
    return ', '.join(q.format(s.strip()) for s in fields.split(','))

def table_schema(db, by_table=True, in_schema='public'):
    """
    Method to get either a dictionary of table names with their corresponding
    schema or a list of tables. If parameter 'by_table' is true, then a dict
    of table names with schema is returned, otherwise the tables in a schema

    Args:
        by_table (boolean): determines whether a dictionary of tables and schema
        names (True) is returned, or a list of table names (False)
        in_schema (str): only applies when by_table is False. Used to specify
        the schema to be searched for list of returned tables
    Returns:
        tbls (dictionary) if by_table is True. Dictionary has form of:
            {'table':'schema'} for all tables and schema in database. This is
            as a helper function with process_data.py to get correct schema
            for each table at runtime when summarizing tables
        tbl_names (list) if by_table is False. Contains a list of all tables
        for schema provided by 'in_schema'
    """
    engine = connect(host, 'postgres', db)
    insp = reflection.Inspector.from_engine(engine)
    #md = MetaData(bind=engine, reflect=True)
    if by_table:
        tbls = defaultdict(lambda: defaultdict(str))
        for schema in insp.get_schema_names():
            for tbl in insp.get_table_names(schema):
                tbls[tbl] = schema
        return tbls
    else:
        tbl_names = insp.get_table_names(in_schema)
        return tbl_names

def create_data_dict():
    import csv
    engine = connect(host, 'postgres')
    cursor = engine.connect()
    os.chdir('/home/neighdough/Dropbox/cpgis/HUD/wwl_docs')

    with open('ACS_2013_SF_5YR_Appendices.csv', 'rb') as f:
        reader = csv.reader(f)
        acs = {row[0].lower(): row[1] for row in reader}

    shell = defaultdict(str)
    with open('ACS2013_TableShells.csv', 'rb') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            if row[2] != '':
                shell[row[2].lower()] =  row[3]

    lyrops = defaultdict(lambda: defaultdict(str))
    with open('layer_operations_11052015.csv', 'rb') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            fld = row[-1] #if row[-1] != '' else row[3]
            print fld
            lyrops[fld] = {'description':row[3],
                             'units':row[4],
                             'citation':row[2]}
    fields = [row[0] for row in cursor.execute("""select column_name from \
    information_schema.columns \
    where table_name = 'summary_cen_county_2010'""")]

    datadict = defaultdict(lambda: defaultdict(str))
    i = 0
    for field in fields:
        if field[:-3] in acs.keys():
            tbl = field[:-3]
            datadict[i]['field'] = field
            datadict[i]['field_desc'] = ':'.join(i for i in [acs[tbl],
                                                shell[field]])
            datadict[i]['citation'] = lyrops[tbl]['citation']
            datadict[i]['units'] = lyrops[tbl]['units']
        elif field in lyrops.keys():
            datadict[i]['field'] = field
            datadict[i]['field_desc'] = lyrops[field]['description']
            datadict[i]['citation'] = lyrops[field]['citation']
            datadict[i]['units'] = lyrops[field]['units']
        else:
            datadict[i]['field'] = field
            datadict[i]['field_desc'] = 'NA'
            datadict[i]['citation'] = 'NA'
            datadict[i]['units'] = 'NA'
        i += 1
    with open('datadict.csv', 'w') as f:
        writer = csv.writer(f, delimiter = ',', quotechar = '"')
        writer.writerow(datadict[0].keys())
        for k in datadict.keys():
            #print k, datadict[k]['units'], datadict[k]['field'],
            datadict[k]['citation'], datadict[k]['field_desc']

            writer.writerow([datadict[k]['units'], datadict[k]['field'],
                             datadict[k]['citation'], datadict[k]['field_desc']])

def process_data(completed=None):
    """need to create the streets_with_sdw view before processing begins
    since it's needed by several of the tables"""

#     streets_sdw = """create materialized view transportation.streets_with_sdw as \
#                      select *, \
#                         case when dynamap_id in (select distinct teleatlas_id
#                         from transportation.mpo_sidewalks) then 'YES'
#                             else 'NO'
#                         end as sidewalk
#                     from transportation .teleatlas_streets;
#                     create unique index dynamap_id_idx on \
#                             transportation.streets_with_sdw(dynamap_id);
#                     CREATE INDEX streets_with_sdw_gix ON \
#                             transportation.streets_with_sdw USING gist(wkb_geometry);"""
#     cursor = connect('caeser-geo.memphis.edu', 'postgres')
#     cursor.execute(streets_sdw)

    import inspect
    from ld import process_data
    methods = inspect.getmembers(process_data)

    geographies = ['cen_county_2010', 'cen_msa_2013',
                   'cen_place_2010', 'cen_tract_2010',
                   'cen_zip_2010']

    functions = defaultdict(str)

    for method in methods:
        for a in method:
            if inspect.isfunction(a):
                # functions[inspect.getsourcelines(a)[1]] =  a.__name__
                functions[inspect.getsourcelines(a)[1]] = a.func_name

    complete = [] if not completed else completed
    skip = ['broadband']
    for geography in geographies:
#         try:
        print geography
        for k in sorted(functions.keys()):
            if not [functions[k], geography] in complete and \
                    functions[k] not in skip:
                print '\t', functions[k]
                getattr(process_data, functions[k])(geography)
                with open('complete_vars.csv', 'a') as f:
                    f.write(functions[k]+','+ geography+'\n')
#                 complete.append([k, geography])
#         except Exception as e:
#             os.chdir('/home/vagrant/sharedworkspace/Projects/Current/HUD/LivabilityDashboard/Docs/wwl_docs/')
#             print '\n\n', e
#             with open('complete_vars.csv', 'a') as f:
#                 f.write('\n'.join(str(comp[0])+', '+ comp[1] for comp in complete))

def completed_processes(dir):
    """
    helper function used in conjunction with process_data. Reads in completed
    variables and returns nested list that determines which processes have
    already been run for each geography.
    """
    os.chdir(dir)
    completed = []
    with open('complete_vars.csv', 'rb') as f:
        for row in f.read().split('\n'):
            i = row.split(',')
            if i[0] != '':
                completed.append([i[0], i[1].strip()])
    return completed

def export_summary_tables(year_str):
    """
    exports summary tables to csv

    Args:
        year_str (str): string value representing the year for the data. String
            should be in format YYYY
    Returns:
        None
    """
    os.chdir('''/home/nate/source/wwl_phase2/wwwroot/data''')
    engine = connect(host, 'postgres',db)
    insp = reflection.Inspector.from_engine(engine)
    meta = MetaData(schema='public')
    meta.reflect(bind=engine)
    skip_tables = ['spatial_ref_sys', 'legend_values','data_dictionary', 
                    'data_dictionary_chng', 'data_sources']
    tables = [t for t in insp.get_table_names('public') if t not in skip_tables]

    for tbl_name in tables:
        print tbl_name
        tbl = Table(tbl_name, meta, autoload=True, autoload_with=engine)
        sel = select([tbl])
        tbl_pd = pd.read_sql(sel, engine)
        tbl_split = tbl_name.split('_')
        csv_name = tbl_split[2] +'_'+ year_str if len(tbl_split) < 5 else \
                tbl_split[2] +'_'+ tbl_split[-1]
        print csv_name
        tbl_pd.to_csv(csv_name+'.csv')

def build_legend_values(db_year):
    """Uses the pandas quantiles method to construct value ranges that will
    for table in tables:
        if '_msa_' in table[0]:
            tables.remove(table)
    be used to build legend break values in the final application. Break values
    are determined by calculating equal count quantiles for 5 classes of values.
    """
    engine = connect(host, 'postgres', db_year)

    quintiles = [.2,.4,.6,.8,1.]
    q_tables = """select table_name from information_schema.tables where
                table_schema = 'public' and substring(table_name,1,7) =
                'summary'"""
    legend_cols = {'table_name':np.str, 'column_name':np.str,
                    'q1':np.float, 'q2':np.float, 'q3':np.float, 'q4':np.float,
                    'q5':np.float, 'min':np.float}
    df_legend = pd.DataFrame(columns=legend_cols.keys())#, dtype=legend_cols)
    tables = engine.execute(q_tables).fetchall()
    df_index = 0
    table_count = 1
    for table in tables:
        print table[0], ' - ', table_count, ' of ', len(tables)
        table_count += 1
        column_count = 1
        q_cols = """select column_name from information_schema.columns where
                    table_name = '{}' and column_name not in ('geoid10',
                    'sqmiland', 'name10', 'id')"""
        df = pd.read_sql("select * from {}".format(table[0]), engine)
        columns = engine.execute(q_cols.format(table[0])).fetchall()
        if '_msa_' in table:
            for column in columns:
                val = np.round(df.loc[0][column[0]],decimals=2)\
                        if not pd.isnull(df.loc[0][column[0]]) else 0.
                series = {'table_name':table[0], 'column_name':column[0],
                          'min':val, 'q1':val, 'q2':val, 'q3':val,
                          'q4':val,'q5':val}
                df_legend.loc[df_index] = pd.Series(series)
                df_index += 1
            continue
        for column in columns:
            print '\t', column[0], ' - ', column_count, ' of ', len(columns)
            column_count += 1
            try:
                """There's a bug in pandas 1.18.1 that causes the quantile method
                to ignore a series if it contains a null value so the workaround
                was to run interpolate on the series first, but there's another
                bug that ignores any null value in the first index position
                of the series, so it's necessary to check for and replace any
                null value in the first row of the series. This null value is
                replaced with the median value for the series"""
                if pd.isnull(df.loc[0][column[0]]):
                    df.ix[0, column[0]] = df[column[0]].median()
                col_interp = df[column[0]].interpolate()
                table_quintiles = col_interp.quantile(quintiles)
               # if not isinstance(table_quintiles,float):
                series = {'table_name':table[0], 'column_name':column[0],
                        'min':df[column[0]].min(),
                        'q1':table_quintiles.values[0],
                        'q2':table_quintiles.values[1],
                        'q3':table_quintiles.values[2],
                        'q4':table_quintiles.values[3],
                        'q5':table_quintiles.values[4]}
            except:
                series = {'table_name':table[0], 'column_name':column[0],
                          'min':0.,
                          'q1':0.,
                          'q2':0.,
                          'q3':0.,
                          'q4':0.,
                          'q5':0.}

            df_legend.loc[df_index] = pd.Series(series)
            df_index += 1
    for column in df_legend.columns:
        if column not in ['table_name', 'column_name']:
            df_legend[column] = df_legend[column].round(decimals=2)
    df_legend.to_sql('legend_values', engine, if_exists='replace', schema='public')

def calculate_change(year1, year2):
    """
    Uses output from two years of WWL data to calculate total change for all
    variables.

    Args:
        year1 (str): name of database containing base year values
        year2 (str): name of database containing comparison year values

    Returns:
        None: creates new table in public schema for comparison year containing
        each variable name along with the percent change
    """
    #pd.set_eng_float_format(accuracy=2)
    engine_1 = connect(host, 'postgres', year1)
    engine_2 = connect(host, 'postgres', year2)
    #cursor_1 = engine_1.connect()
    #cursor_2 = engine_2.connect()
    tbls = table_schema(year2, False)
    tbl_skip = ['legend_values', 'spatial_ref_sys', 'data_dictionary',
                'data_dictionary_chng', 'data_sources']
    for tbl in tbls:
        if tbl not in tbl_skip and tbl.split('_')[-1] != 'chng':
            sql = 'select * from {}'.format(tbl)
            df_1 = pd.read_sql(sql, engine_1)
            df_2 = pd.read_sql(sql, engine_2)
            df_1.drop_duplicates(subset='geoid10',inplace=True)
            df_2.drop_duplicates(subset='geoid10',inplace=True)
            df_1.set_index('geoid10', inplace=True)
            df_2.set_index('geoid10', inplace=True)
            df_chg = pd.DataFrame(data=None, columns=df_2.columns,
                    index=df_2.index)
            skip_cols = set(['geoid10','sqmiland','name10','acreland','id'])
            cols = list(set(df_2.columns).intersection(set(df_1.columns)).\
                    difference(skip_cols))
            for col in cols:
                df_chg[col] = (df_2[col] - df_1[col])/df_2[col]*100
                if df_chg[col].ftype == 'float64:dense':
                    df_chg[col] = df_chg[col].round(decimals=2)
            df_chg.to_sql('{}_chng'.format(tbl),engine_2,
                    schema='public',if_exists='replace')

def export_all_tables():
    """
    """
    import sqlite3
    import subprocess
    cnx_params = utils.connection_properties('caeser-geo.memphis.edu',
                                             'postgres', db)
    engine = connect('caeser-geo.memphis.edu', 'postgres', 'livability_2016')
    insp = reflection.Inspector.from_engine(engine)
    schema = insp.get_schema_names()
    c = sqlite3.connect('/home/nate/temp/wwl.sqlite')
    tbl_list = c.execute("select name from sqlite_master where type='table' \
            order by name").fetchall()
    sql_tables = [t[0] for t in tbl_list]
    for s in schema:
        if s not in ['information_schema', 'tiger', 'tiger_data','topology']:
            cnx_params['sch'] = s
            print s
            tables = [t for t in insp.get_table_names(s) \
                    if t.split('_')[0] != 'sca' and t not in sql_tables]
            for table in tables:
                columns = insp.get_columns(table,s)
                cols = [col['name'] for col in columns if col['name'] != 'index']
                # sql = """select {0} from {1}""".format(','.join(c for c in cols),
                                                        # table)
                cnx_params['table'] = table
                db_cnx = '''PG:"dbname={db} host={host} user={user}\
                                password={password} port={port} \
                                schemas={sch} tables={table}"'''.format(**cnx_params)
                out_format = 'SQLITE'
                skip = 'PG_SKIP_VIEW YES'
                out_db = '/home/nate/temp/wwl.sqlite'
                launder = 'LAUNDER=YES'
                dsco_name = 'SPATIALITE=yes'
                sp_index = 'SPATIAL_INDEX=YES'
                select_cols = ','.join(c for c in cols)
                cmd = ['ogr2ogr', '--config', skip, '-f', out_format, out_db, db_cnx,
                       '-lco', launder, '-dsco', dsco_name, '-lco', sp_index,
                       '-select', select_cols, '-skipfailures']

                cmd = ['ogr2ogr',
                       '--config',
                       'PG_SKIP_VIEW',
                       'YES',
                       '-append',
                       '-f "SQLITE"',
                       '/home/nate/temp/wwl.sqlite',
                       '''PG:"dbname='{db}' host='{host}' user='{user}'\
                               password='{password}' port='{port}' \
                               schemas={sch} tables={table}"'''.format(**cnx_params),
                       '-lco LAUNDER=yes',
                       '-dsco SPATIALITE=yes',
                       '-lco SPATIAL_INDEX=yes',
                       '-gt 65536',
                       '-select {}'.format(','.join(c for c in cols)),
                       '-skipfailures']
                os.system(' '.join(c for c in cmd))
                print '\t', table

def update_wwl_json(db):
    """converts data from livability_<year> database into json to be loaded
    into wwl_phase2 database for display on website. Data are pullled from
    summary_cen_<geography> or legend_values tables in the public schema and
    each row is converted to a json object and placed into a list to be
    pushed into the compiledgeographies.csv column in WWL_PHASE2 db.

    Args:
        db (string): name of database that data needs to be converted from
            * livability_2015
            * livability_2016
    Returns:
        None

    """
    engine = connect('caeser-geo.memphis.edu', 'postgres', db)
    insp = reflection.Inspector.from_engine(engine)
    engine_wwl = connect('caeser-geo.memphis.edu', 'postgres', WWL_DB)
    tables = [t for t in insp.get_table_names(schema='public') \
            if t not in ['spatial_ref_sys', 'data_sources']]
    compiledgeog = pd.read_sql("select * from compiledgeographies", engine_wwl,
                                index_col='id')
    geographies = pd.read_sql("select * from geographies", engine_wwl,
                                index_col='id')
    cnx = engine_wwl.connect()
    for table in tables:
        if table.split('_')[-1] == 'chng':
            year = 'chng'
        else:
            year = db.split('_')[-1]
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

#def create_inventory():






if __name__ == '__main__':
    args = docopt(__doc__)
    if args["newdb"]:
        year = args["<year>"]
        make_new_db(year)
            

    WWL_DB = 'wwl-test'
    cred = cnx_params.wwl_2017
    host = cred['host']
    db = cred['dbname']


