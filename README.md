wwl-data: WhereWeLive Data
====================

This repo contains all of the ETL scripts needed to update data for the wherewelivemidsouth.org website.

Contents:


* [Setting up the project database]{#setup}
* [Data]{#data}
	* [American Community Survey (ACS)]{#acs}
* Preparing data
	* collecting and adding data
* Processing data
	* processing and loading data


#Setting up project database {#setup}


# Data

## American Community Survey (ACS){#acs}

All of the Census data used in WWL has been derived from the American Community Survey 5-year estimates. Data should be downloaded from the [Census FTP site](https://www2.census.gov/programs-surveys/acs/summary_file/) for the year to be loaded using the *5_year_by_state* directory which contains a compressed copy of all sequence tables for each state. For each state, two zip files should be downloaded, *<StateName>_Tracts_Block_Groups_Only.zip* and *<StateName>_All_Geographies_Not_Tracts_Block_Groups.zip*. Current databases contain tables for Arkansas, Mississippi, Tennessee, and UnitedStates.

Once downloaded, the compressed file should be placed on Sharedworkspace under
Data/Census/ACS/acs5yr_<YYYY>.

In addition to the zip files containing all of the data, both the ACS_<YYYY>_SF_5YR_Appendices.xls and the ACS_5yr_Seq_Table_Number_Lookup.txt files should be downladed from ACS ftp location under ./documentation/user_tools

Once all of the data and necessary files have been downloaded and placed in the correct directory, open a terminal and run

    $ python acsdb.py YYYY

where YYYY is the 4-digit year of the ACS data being loaded. It may be necessary to check to make sure that the correct connections to sharedworkspace have been specified in the python file before running.


# Preparing data

# Processing data
