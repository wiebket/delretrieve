# South African Domestic Load Research Data Retrieval

## About this package

This package contains tools to retrieve primary data from the South African Domestic Load Research database. It requires access to a MSSQL server installation of the DLR database. 

**Note on data access:** 
There are easier options than setting up your own server instance of the database.  
1. Data access can be requested from [Data First](www.datafirst.uct.ac.za) at the University of Cape Town (UCT). On site access to the complete 5 minute data is available through their secure server room and does not require a MSSQL server installation.   
2. Several datasets with aggregated views are available [online]() and can be accessed for academic purposes.  

Other useful packages are:
[dlr_data_processing](https://github.com/wiebket/dlrprocess) for cleaning, wrangling, processing, aggregating and feature engineering of the DLR data.

### Package structure

```bash
dlrretrieve
    |-- dlrretrieve
    	|-- data
    	    |-- blobAnon.csv
    	    |-- charAnon.csv
    		|-- cnxnstr.txt	
			|-- store_path.txt
		|-- __init__.py
    	|-- command_line.py
    	|-- retrieve_dlr.py	
    	|-- support.py
	|-- __init__.py
    |-- MANIFEST.in
	|-- README.md
	|-- setup.py
```

## Setup instructions
Ensure that python 3 is installed on your computer. A simple way of getting it is to install it with [Anaconda](https://conda.io/docs/user-guide/install/index.html). Once python has been installed, the dlrretrieve package can be installed.

1. Clone this repository from github.
2. Navigate to the root directory (`dlrretrieve`) and run `python setup.py install`. 
3. Update the database connection information in `USER_HOME/dlr_data/usr/cnxnstr.txt`. The data retrieval process will ONLY work if you have installed the database on a MSSQL server instance and have access permissions to the data. 

## Data retrieval

From the command line (or Anaconda Prompt on windows) run 

1. `python dlrretrieve_data -p` to retrieve 5min load profile timeseries data
	You will be prompted to enter the start and end year (choose in the range of 1994 - 2014) for which you want to retrieve data
2. `python dlrretrieve_data -t` to retrieve all supplementary tables
3. `python dlrretrieve_data -s` to retrieve anonymised survey responses

When you use the command line interface for the first time, you will be requested to confirm the path for storing retrieved data. The 5 minute load profile data is ~120GB. Ensure that you choose a location with sufficient storage space! The default location is`USER_HOME/dlr_data/observations/`. You can change the storage location by creating a new target directory and altering the path in `USER_HOME/dlr_data/usr/store_path.txt`, or by deleting the path and entering a new path when prompted by the command line.

### Additional command-line options

`-c`: (optional) saves files as .csv files instead of .feather (see notes on file format below)  
`-y`: (optional) start year for profile data retrieval  
`-z`: (optional) end year for profile data retrieval  

### File format
The default format for retrieving data is as a .feather file, which provides fast and efficient retrieval and uploads for data frames. Feather is compatible with both R and python. Feather files should be stored for working purposes only as the file format is not suitable for archiving. Feather files built under one version can be incompatible with those built under a new version, in which case you will need to reconstruct them from the raw MSSQL database. Learn more about [feather](https://github.com/wesm/feather).

To save the data in csv format, pass `--csv` as option to your command line arguements.
