<a href="https://zenodo.org/badge/latestdoi/160184706"><img src="https://zenodo.org/badge/160184706.svg" alt="DOI"></a>  
<img src="/delretrieve/data/images/DEL_logo.png" alt="DEL Logo" width="200" height="150" align="left"/>


# South African <br/> Domestic Electrical Load Study <br/> Data Retrieval

## About this package

This package contains tools to retrieve primary data from the South African Domestic Electrical Load (DEL) database. It requires access to a MSSQL server installation of the original General_LR4 database produced during the NRS Load Research study. 

**Notes on data access:** 

There are easier options than setting up your own server instance of the database.  
1. Data access can be requested from [Data First](www.datafirst.uct.ac.za) at the University of Cape Town (UCT). On site access to the complete 5 minute data is available through their secure server room and does not require a MSSQL server installation.   
2. Several datasets with aggregated views are available [online](https://www.datafirst.uct.ac.za/dataportal/index.php/catalog/DELS/about) and can be accessed for academic purposes.  

Other useful packages are:  
[delprocess](https://github.com/wiebket/delprocess) for cleaning, wrangling, processing, aggregating and feature engineering of the DEL data.

### Package structure

```bash
delretrieve
    |-- delretrieve
        |-- data
            |-- blobAnon.csv
            |-- charAnon.csv
            |-- cnxnstr.txt	
            |-- store_path.txt
            |-- DEL_logo.png
        |-- __init__.py
        |-- command_line.py
        |-- retrieve_del.py	
        |-- support.py
    |-- MANIFEST.in
    |-- README.md
    |-- setup.py
```

## Setup instructions
Ensure that python 3 is installed on your computer. A simple way of getting it is to install it with [Anaconda](https://conda.io/docs/user-guide/install/index.html). Once python has been installed, the delretrieve package can be installed.

1. Clone this repository from github.
2. Navigate to the root directory (`delretrieve`) and run `python setup.py install`. 
3. Update the database connection information in `USER_HOME/del_data/usr/cnxnstr.txt`. The data retrieval process will ONLY work if you have installed the database on a MSSQL server instance and have access permissions to the data. 

## Data retrieval

From the command line (or Anaconda Prompt on windows) run: 

1. `delretrieve_data -p` to retrieve 5min load profile timeseries data
	You will be prompted to enter the start and end year (choose in the range of 1994 - 2014) for which you want to retrieve data
2. `delretrieve_data -t` to retrieve all supplementary tables
3. `delretrieve_data -s` to retrieve anonymised survey responses

When you use the command line interface for the first time, you will be requested to confirm the path for storing retrieved data. The 5 minute load profile data is ~120GB. Ensure that you choose a location with sufficient storage space! The default location is`USER_HOME/del_data/observations/`. You can change the storage location by creating a new target directory and altering the path in `USER_HOME/del_data/usr/store_path.txt`, or by deleting the path and entering a new path when prompted by the command line.

*Additional command-line options*

`-c`: (optional) saves files as .csv files instead of .feather (see notes on file format below)  
`-y`: (optional) start year for profile data retrieval  
`-z`: (optional) end year for profile data retrieval  

### Output
The default format for retrieving data is as a .feather file, which provides fast and efficient retrieval and uploads for data frames. Feather is compatible with both R and python. Feather files should be stored for working purposes only as the file format is not suitable for archiving. Feather files built under one version can be incompatible with those built under a new version, in which case you will need to reconstruct them from the raw MSSQL database. Learn more about [feather](https://github.com/wesm/feather).

## Data Exploration
getGroups, getProfiles, writeProfiles, writeTables, saveTables, saveAnswers, saveRawProfiles

## Acknowledgements

### Citation
Toussaint, Wiebke. delretrieve: Data Retrieval of the South African Domestic Electrical Load Study, version 1.01. Zenodo. https://doi.org/10.5281/zenodo.3605425 (2019).

### Funding
This code has been developed by the Energy Research Centre at the University of Cape Town with funding from the South African National Energy Development Initiative under the CESAR programme.


 Developed by          	|  Funded by
:----------------------:|:-------------------------:
<img src="/delretrieve/data/images/erc_logo.jpg" alt="ERC Logo" width="206" height="71" align="left" hspace="20" />   |  <img src="/delretrieve/data/images/sanedi_logo.jpg" alt="Sanedi Logo" width="177" height="98" align="left" hspace="20" />

