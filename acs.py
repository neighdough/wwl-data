# -*- coding: utf-8 -*-

"""
Usage:
    acsdb.py <year>

Options:
    [year]  : year in YYYY format representing the ACS year to be loaded. The year of 
              should match the directory storing all of the raw data to be added to the
              database
Example:
    $ python acsdb.py 2016
    

README
Setup:
    1. Create directories to store all ACS information for time period
        ./ACS/acs5yr_yyyy
            /ACS_YYYY_SF_5YR_Appendices.xls
            /Sequence_Number_and_Table_Number_Lookup.txt
        ./ACS/acs5yr_yyyy/data
            /STATE_NAME_All_Geographies_Not_Tracts_Block_Groups.zip
            /STATE_NAME_All_Geographies_Tracts_Block_Groups_Only.zip
    Raw zip files containing sequence tables should be placed in 'data' 
        subdirectory
    Necessary files can be downloaded from ftp.census.gov
    Required files:
        ACS_YYYY_SF_5YR_Appendices.xls -> contains sequence file split indices
            had to repair Summary File Sequence Number and Summary File Starting 
            and Ending Positions for tables B20017C and B20017E
        Sequence_Number_and_Table_Number_Lookup.txt -> contains all table and 
            field information used to create tables in postgresql
Run:
    Setup:
        1. set root directory
        2. set data directory
        3. set schema name (psql_schema = acs5yr_yyyy)
        
    Methods:
        1. build_temp_tables()
        2. load_temp_tables()
        3. build_geoheader()
        4. load_geoheader()
        5. build_acs_tables()
"""

import csv
import os
import psycopg2 as psql
from caeser import utils
import zipfile
import xlrd
import collections
import re
import codecs
import chardet
from docopt import docopt
from config import cnx_params


def build_geoheader():
    create_geoheader = ("CREATE TABLE IF NOT EXISTS geoheader "
                        "({0}, CONSTRAINT geoheader_pkey PRIMARY KEY (geoid))")
    create_comment = """COMMENT ON COLUMN "geoheader".{0} is '{1}'"""
    
    #if clauses check year of data in order to accommodate changes in 
    #which increased length of geoheader from 200 to 1000 and added new fields   
    if int(yr) < 2011:
        cursor.execute(create_geoheader.format(', '.join(table[0].lower() + 
                                        ' text' for table in geoheader[:-3])))
        for table in geoheader[:-3]:
            cursor.execute(create_comment.format(table[0].lower(), table[1]))
    else:    
        cursor.execute(create_geoheader.format(', '.join(
            table[0].lower() + ' text' for table in geoheader)))
        for table in geoheader:
            cursor.execute(create_comment.format(table[0].lower(), table[1]))    
    db.commit()

def geoheader_query(reader, file_type='csv'):
    records = []
    for row in reader:        
        if file_type == 'fixed':
        #pulls values from geoheader list to parse fixed width file
            if int(yr) < 2011:
                row_temp = [str(row[i[2]:i[3]]).rstrip() for i in geoheader[:-3]]
            else:
                row_temp = [str(row[i[2]:i[3]]).rstrip() for i in geoheader]
            row = row_temp           
        for (i,item) in enumerate(row):
            curval = row[i].decode('windows_1252').encode('utf_8')
            if "'" in curval:
                #escapes ' in names with apostrophes (e.g. D'Arc)
                curval = curval.replace("'", "''")             
            row[i] = 'NULL' if item in ['.', ''] else "'"+curval+"'"
        record = ', '.join(r for r in row)
        records.append('('+record+')')
    return records        


def split_fixed_file(f):
    f_format = ''
    for row in f:
        f_format += ', '.join(row[i[2]:i[3]] for i in geoheader)
        
def load_geoheader(file_type):
    """
    loads data from geoheader csv into geoheader table
    file_type(String):
        -> csv: geoheader stored as csv
        -> fixed: fixed width  
    """
    #list used to hold processed geoheaders to avoid duplication 
    #since it exists in both state zip files
    geoheader_list = []
    for acs_zip in [f for f in os.listdir(".") if f.endswith("zip")]:        
        zip_file = zipfile.ZipFile(acs_zip)
        #uses csv geoheader published starting in 2011
        if file_type == 'csv':
            geo_file = [f for f in zip_file.namelist() if f.endswith('.csv')][0]
        elif file_type == 'fixed':
            geo_file = [f for f in zip_file.namelist() if f[0] == 'g'][0]            
        if not geo_file in geoheader_list:
            insert_query = 'insert into acs5yr_{0}.geoheader values {1};' 
            geoheader_list.append(geo_file)
            with zip_file.open(geo_file) as f:   
                if file_type == 'csv':
                    """updated reader to take in windows-1252 encoding, 
                    altered geoheader_query to take in string instead of list"""                
                    reader = csv.reader(f)
                    values = geoheader_query(reader, 'csv')
                    cursor.execute(insert_query.format(yr,','.join(value for  
                        value in values)))
                    db.commit()                 
            #accommodates fixed width geoheader file published exclusively 
            #prior to 2011
                elif file_type == 'fixed':
                    reader = geoheader_query(f, 'fixed')              
    
def build_temp_tables():
    #builds holding tables used in later queries for inserting GEOID
    schema = table_schema(1)
    create_temp_query = (("CREATE TABLE IF NOT EXISTS temp_{0} (fileid text, "
                      "stusab text, sumlevel text, component text,"
                      "logrecno text, {1} numeric, CONSTRAINT temp_{0}_pkey "
                      "PRIMARY KEY (fileid, stusab, logrecno))"))
    schema_sort = list(schema.keys())
    schema_sort.sort()
    for key in schema_sort:
        cursor.execute(create_temp_query.format(key[0],
            ' numeric,'.join(v[0] for v in schema[key])))
        db.commit()
        
def load_temp_tables():
    """
    loads data from downloaded zip files into temporary tables 
    """    
    #create lookup dictionary for sequences containing table names
    # and sequence index positions
    sequence_table_dict = collections.defaultdict(list) 
    for i in range(1, ws.nrows):
        table_no = ws.cell_value(i,0).lower()    
        while table_no == ws.cell_value(i,0).lower() and i < ws.nrows-1:
            seq = ws.cell_value(i,3)
            if type(seq) == float:
                sequence_no = str(int(seq)).zfill(4)
            else:
                sequence_no = seq
            #indices in census docs start at position 1, not 0  
            start_index = int(ws.cell_value(i,4).split('-')[0]) - 1
            #since indices start at 1, not subtracting slices table at 
            #correct index
            finish_index = int(ws.cell_value(i,4).split('-')[1])
            if not (sequence_no, start_index, finish_index) in sequence_table_dict[table_no]:
                sequence_table_dict[table_no].append((sequence_no, 
                    start_index, finish_index))
            i += 1
    #iterate over each state zip file and split sequence files into temporary tables
    for acs_zip in [f for f in os.listdir(".") if f.endswith("zip")]:           
        zip_file = zipfile.ZipFile(acs_zip)
        #build prefix for sequence text file from first item in each list
        sequence_prefix = zip_file.namelist()[0][1:-11]
        tbl_no = 1
        for key in sequence_table_dict.keys():
            insert_cursor = db.cursor()
            print tbl_no, ' -> ', key
            tbl_no += 1
            table = key
            records = list()
            record = list()
            full_table = {'e':[], 'm':[]}
            #changed to conditional to not equal, but not sure why conditional 
            #is here in first place
            if table != 'c27010':
                for value in sequence_table_dict[key]:
                    sequence_no = value[0]
                    start_index = value[1]
                    finish_index = value[2]
                    #estimate and margin of error prefix
                    for e_m in full_table.keys():
                        with zip_file.open(e_m + sequence_prefix + sequence_no +'000.txt') as f:     
                            reader = list(csv.reader(f, delimiter=',',quotechar='"'))        
                            for row in reader:
                                for (i,item) in enumerate(row):
                                    row[i] = 'NULL' if item in ['.', ''] else row[i]
                                if sequence_table_dict[key].index(value) == 0:
                                    full_table[e_m].append(row[1:6] + 
                                        row[start_index:finish_index])
                                else:
                                    full_table[e_m][reader.index(row)] += row[start_index:finish_index]       
                for e_m in full_table.keys():
                    records = list()
                    insert_query = 'insert into {0} values {1};'
                    for val in full_table[e_m]:
                        record = "{},{}".format(", ".join(["'{}'".format(x) for x in val[:5]]), 
                                                    ", ".join(map(str, val[5:])))
                        records.append('('+record+')')
                    if records:
                        sequence_table_dict[key].index(value)
                        insert_cursor.execute(insert_query.format('temp_'+table, 
                            ', '.join(r for r in records)))
                        db.commit()            
                    else:
                        print ("\n\n*********** no records in table ",
                            table, " *************\n\n"   )
            insert_cursor.close()            
         
def build_acs_tables():
    """
    
    """
    schema = table_schema(1)
    acs_query = ("drop table if exists {0};"
            "CREATE TABLE {0} as " 
            "SELECT trim('US' from "
            "substring(geoid from position('US' in geoid))) as geoid, "
            "geoheader.name, geoheader.stusab, geoheader. "
            "sumlevel, temp_{0}.fileid,"
            "temp_{0}.{1} from temp_{0} join geoheader "
            "on(geoheader.logrecno = temp_{0}.logrecno and "
            "lower(geoheader.stusab) = temp_{0}.stusab);")
    alter = ("ALTER TABLE {0} ADD COLUMN id SERIAL;"
             "ALTER TABLE {0} ADD CONSTRAINT {0}_pkey PRIMARY KEY (id);")
    schema_sort = list(schema.keys())
    schema_sort.sort()
    for key in schema_sort:
        print acs_query.format(key[0],
            ','.join(v[0].lower() for v in schema[key]))
        cursor.execute(acs_query.format(key[0],
            ','.join(v[0].lower() for v in schema[key])))
        cursor.execute(alter.format(key[0]))
        db.commit()
    add_comments(schema)

def add_comments(schema):
    '''adds comments to postgresql table        
    tbl_schema: dictionary created from Data Field Descriptors file        
    '''
    print 'Adding comments to tables...'        
    sql_comment = """\tCOMMENT ON TABLE "{0}" is '{1}'; \
                        COMMENT ON COLUMN "{0}".{2}"""
    for k in schema.keys():
        pair = '''COMMENT ON COLUMN "{0}".'''.join('"' + v[0].lower() + '"' + ' IS ' + 
                                                    "'"+ v[1]+
                                                    "';" for v in schema[k]).format(k[0].lower())      
        print sql_comment.format(k[0].lower(), k[1], pair)        
        cursor.execute(sql_comment.format(k[0].lower(), k[1], pair))
        db.commit()
                        
def schema_dictionary():
    sql_schema = collections.defaultdict(list)
    return sql_schema    

def table_schema(i):
    """
    Creates ACS (i = 1) or Sequence (i = 2) tables
    returns dictionary containing table name and field list to be used 
    in DDL query
    """
    schema = schema_dictionary()    
    with open('ACS_5yr_Seq_Table_Number_Lookup.txt', 'r') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        next(reader, None)
        tbl = [r for r in reader]
        row = 0
        while row < len(tbl) - 1:        
            header = tbl[row][i].lower()        
            tbl_comment = tbl[row][7].replace("\'","''") if i == 1 else ''   
            while tbl[row][i].lower() == header:
                tableid = tbl[row][1].lower()
                line_no = tbl[row][3]
                if line_no not in (' ', '') and '.' not in line_no:
                #if line_no != ' ' and '.' not in line_no:#modified to 
                #accommodate missing spaces between commas in 2009 file
                    #chr(147), chr(148) replaces the curly quotes                  
                    comment = tbl[row][7].replace("\'","''").replace(chr(147), 
                        "''").replace(chr(148), "''")
                    field = tableid + line_no.zfill(3)               
                    if not (field, comment) in schema[(header, tbl_comment)]:
                        schema[(header, tbl_comment)].append((field, comment)) 
                    if row + 1 == len(tbl):
                        break
                row += 1
    return schema    

def cleanup():
    """
    Clean up database by dropping all temp tables
    """
    cursor.execute(("select table_name from information_schema.tables "
                   "where substring(table_name, 1, 5) = 'temp_' "
                   "and table_schema = '{}'".format(psql_schema)))
    tables = [t[0] for t in cursor.fetchall()]
    i = 0
    for table in tables:
        i += 1
        cursor.execute("drop table {0}.{1} cascade;".format(psql_schema,table))
        #commit delete in batches of 300 to avoid shared memory lock
        if i % 300 == 0:
            db.commit()
    db.commit() #one final delete to pick up stragglers



def main():
    print("\nbuilding temp tables\n")
    build_temp_tables()
    print("\nloading temp tables\n")
    load_temp_tables()
    print('\nbuilding geoheader\n')
    build_geoheader()
    print('\nloading geoheader complete\n')
    load_geoheader('csv')
    print('\nloading geoheader complete\n')
    build_acs_tables()
    print('\nbuilding acs_tables complete\n')
    print("\ndropping all temp tables\n")
    cleanup()
    
    

if __name__ == '__main__':
    args = docopt(__doc__)

    #******************************Run time variables*****************************
    #subdirectory containing downloaded zip files
    yr = args["<year>"]#'2015'
    #root directory storing all of the census information
    os.chdir(os.path.join(os.environ["HOME"],
                            "sharedworkspace/Data/Census/ACS/acs5yr_{}".format(yr)))

    #Excel spreadsheet containing Census table names, sequence, and index positions
    #for each table within each sequence file
    wb = xlrd.open_workbook('ACS_{}_SF_5YR_Appendices.xls'.format(yr))
    ws = wb.sheet_by_name('Appendix A')

    psql_schema = 'acs5yr_{}'.format(yr)
    db = psql.connect(database='census', user='postgres', 
        host='caeser-geo.memphis.edu')
    db.set_client_encoding('UNICODE')
    cursor = db.cursor()

    cursor.execute("create schema if not exists {0}".format(psql_schema))
    cursor.execute("set session search_path to {0}".format(psql_schema))
    db.commit()

    
    #builds geoheader file using predefined list of column names pulled from
    #ACS Summary File Technical Documentation.
    #geoheader[0] -> field name
    #geoheader[1] -> field description
    #geoheader[2] -> starting index in fixed-width text file, not used but left in 
    #                in case of future need
    #geoheader[3] -> ending index in fixed-width text file, not used but left in 
    #                in case of future need  
    geoheader = [['fileid', 'Always equal to ACS Summary File identification', 0, 6],
                ['stusab', 'State Postal Abbreviation', 6, 8],
                ['sumlevel', 'Summary Level', 8, 11],
                ['component', 'Geographic Component', 11, 13],
                ['logrecno', 'Logical Record Number', 13, 20],
                ['us', 'US', 20, 21],
                ['region', 'Census Region', 21, 22],
                ['division', 'Census Division', 22, 23],
                ['statece', 'State (Census Code)', 23, 25],
                ['state', 'State (FIPS Code)', 25, 27],
                ['county', 'County of current residence', 27, 30],
                ['cousub', 'County Subdivision (FIPS)', 30, 35],
                ['place', 'Place (FIPS Code)', 35, 40],
                ['tract', 'Census Tract', 40, 46],
                ['blkgrp', 'Block Group', 46, 47],
                ['concit', 'Consolidated City', 47, 52],
                ['aianhh', ('American Indian Area/Alaska Native Area/ Hawaiian '
                    'Home Land (Census)'), 52, 56],
                ['aianhhfp', ('American Indian Area/Alaska Native Area/ Hawaiian '
                    'Home Land (FIPS)'), 56, 61],
                ['aihhtli', ('American Indian Trust Land/ Hawaiian Home Land '
                    'Indicator'), 61, 62],
                ['aitsce', 'American Indian Tribal Subdivision (Census)', 62, 65],
                ['aits', ('American Indian Tribal Subdivision (Census) Subdivision '
                    '(FIPS)'), 65, 70],
                ['anrc', 'Alaska Native Regional Corporation (FIPS)', 70, 75],
                ['cbsa', 'Metropolitan and Micropolitan Statistical Area', 75, 80],
                ['csa', 'Combined Statistical Area', 80, 83],
                ['metdiv', 'Metropolitan Statistical Area-Metropolitan Division',
                     83, 88],
                ['macc', 'Metropolitan Area Central City', 88, 89],
                ['memi', 'Metropolitan/Micropolitan Indicator Flag', 89, 90],
                ['necta', 'New England City and Town Area', 90, 95],
                ['cnecta', 'New England City and Town Combined Statistical Area', 
                    95, 98],
                ['nectadiv', 'New England City and Town Area Division', 98, 103],
                ['ua', 'Urban Area', 103, 108],
                ['blank1', '', 108, 113],
                ['cdcurr', 'Current Congressional District', 113, 115],
                ['sldu', 'State Legislative District Upper', 115, 118],
                ['sldl', 'State Legislative District Lower', 118, 121],
                ['blank2', '', 121, 127],
                ['blank3', '', 127, 130],
                ['zcta5', '5-digit ZIP Code Tabulation Area', 130, 135],
                ['submcd', 'Subminor Civil Division (FIPS)', 135, 140],
                ['sdelm', 'State-School District (Elementary)', 140, 145],
                ['sdsec', 'State-School District (Secondary)', 145, 150],
                ['sduni', 'State-School District (Unified)', 150, 155],
                ['ur', 'Urban/Rural', 155, 156],
                ['pci', 'Principal City Indicator', 156, 157],
                ['blank4', '', 157, 163],
                ['blank5', '', 163, 168],
                ['puma5', 'Public Use Microdata Area, 5% File', 168, 173],
                ['blank6', '', 173, 178],
                ['geoid', 'Geographic Identifier', 178, 218],
                ['name', 'Area Name', 218, 1218],
                ['bttr', 'Tribal Tract', 1218, 1224],
                ['btbg', 'Tribal Block Group', 1224, 1225],
                ['blank7', '', 1225, 1269]]
    main()
        


