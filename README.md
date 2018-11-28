# wwl-data: WhereWeLive Data


This repo contains all of the ETL scripts needed to update data for the wherewelivemidsouth.org website.

Contents:


* Overview of repository
* Setting up the project database
* Data
  * EPA Air Quality
  * EPA Brownfields
  * American Community Survey
* Preparing data
  * collecting and adding data
* Processing data
  * processing and loading data

## Overview of repository

### acs.py

Module used to create and build tables for the American Community Survey (ACS)

### process_data.py

Module containing all of the queries and code to convert data from its raw PostgreSQL format into processed data to be loaded into the final web application. 

### requirements.txt

Pip requirements file containing a list of all libraries used throughout the project.

### tables_and_sources.sql

SQL query file to extract table listing all of the variables in WWL along with their update frequency, source, and description.

### updates.py

Module containing code to process layers to be updated according to current scope of work

### wwldb.py

Module containing methods needed to build, maintain, and update WWL staging database (e.g. livability_2017)

## Setting up the project database

A new project database is created by copying the previous year's database and then loading new data in as it is updated. All of this is handled through the *make_new_db* method in *wwldb.py* and can be accessed through the command line through

    python wwldb.py newdb <YYYY>

where <YYYY> is the year suffix to be added to the database name. Data is copied from the previous database by subtracting the current database year by one. In otherwords, if the current database being created is for 2018, the new database name will be "livability_2018" and "livability_2017" will be used as a template for creating it.

## Data
Data are updated in a staged manner where the original data is downloaded or acquired from its source, placed into a staging folder in the caeser Dropbox Data folder for CFGM (Data/CFGM/WWL/<YYYY>/data). All tools needed to add new data to the newly created livability database are in the *updates.py* module.

### EPA Air Quality
This dataset is downloaded from the [EPA AQS Web air data site](https://aqs.epa.gov/aqsweb/airdata/download_files.html#Annual) using the Table of Annual Summary Data. The file that should be downloaded is *annual_conc_by_monitor_<YYYY>.zip* where <YYYY> is the year corresponding to the current update and placed into ./epa directory.
    
    python updates.py air_quality

### EPA Brownfields
This data is downloaded from the [EPA FRS Facilities State Single File CSV Download](https://www.epa.gov/enviro/epa-frs-facilities-state-single-file-csv-download) page. Since the Memphis MSA covers 3 states, the National Zip File should be downloaded, not the state. Once downloaded, data should be placed in the ./epa directory.

    python updates.py brownfields

### Shelby County Assessor
Assessor data is accessed via Shelby County's [SFTP site](https://xfer.shelbycountytn.gov).

    user: F202GIS

    pwd: Y73JdVSS

    port: 22

Once downloaded, all data should be placed within the Assessor directory under the sharedworkspace/DATA.

### American Community Survey

All of the Census data used in WWL has been derived from the American Community Survey (ACS) 5-year estimates. Data should be downloaded from the [Census FTP site](https://www2.census.gov/programs-surveys/acs/summary_file/) for the year to be loaded using the *5_year_by_state* directory which contains a compressed copy of all sequence tables for each state. For each state, two zip files should be downloaded, *<StateName>_Tracts_Block_Groups_Only.zip* and *<StateName>_All_Geographies_Not_Tracts_Block_Groups.zip*. Current databases contain tables for Arkansas, Mississippi, Tennessee, and UnitedStates.

Once downloaded, the compressed file should be placed on Sharedworkspace under
Data/Census/ACS/acs5yr_<YYYY>.

In addition to the zip files containing all of the data, both the ACS_<YYYY>_SF_5YR_Appendices.xls and the ACS_5yr_Seq_Table_Number_Lookup.txt files should be downladed from ACS ftp location under ./documentation/user_tools

Once all of the data and necessary files have been downloaded and placed in the correct directory, open a terminal and run

    python acsdb.py YYYY

where *YYYY* is the 4-digit year of the ACS data being loaded. It may be necessary to check to make sure that the correct connections to sharedworkspace have been specified in the python file before running.

## Preparing data

### collecting and adding data

## Processing data

### processing and loading data
