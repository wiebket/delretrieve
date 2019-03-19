# South African Domestic Load Research Data Retrieval

```bash
dlrretrieve
    |-- dlrretrieve
    	|-- data
    	    |-- blobAnon.csv
    	    |-- charAnon.csv
	|-- __init__.py
    	|-- cnxnstr.txt	
    	|-- command_line.py
    	|-- retrieve_dlr.py
    	|-- support.py
    |-- setup.py
    |-- README.md
    |-- MANIFEST.in

```

## About this package

This package contains tools to retrieve primary data from the South African Domestic Load Research database. It requires access to a MSSQL installation of the DLR database. On site access is available through [Data First's](www.datafirst.uct.ac.za) secure server room at the University of Cape Town (UCT). Researchers on the UCT campus can request remote access to the timeseries data.

## Setup instructions
Ensure that python 3 is installed on your computer. A simple way of getting it is to install it with [Anaconda](https://conda.io/docs/user-guide/install/index.html). Once python has been installed, you are ready for installing the dlrretrieve package.

1. Clone this repository from github.
2. Navigate to the root directory (`dlrretrieve`) and run the `setup.py` script. This will setup the necessary requirements and create the following directory structure for saving retrieved data:

```bash
usr/documents/dlr_data
    |-- observations
	|-- profiles
	    |-- raw
		|-- GroupYear
		    |-- ObsYear-ObsMonth
		|-- tables
		    |-- csv
		    |-- feather
    |-- features
```

3. Update the database connection information in [`dlrretrieve.cnxnstr.txt`](dlrretrieve/cnxnstr.txt). The data retrieval process will ONLY work if you have access permissions to the data. 

## Data retrieval

From the command line run 

1. `python retrieve_dlr -p` to retrieve 5min load profile timeseries data
	You will be prompted to enter the start and end year (choose in the range of 1994 - 2014) for which you want to retrieve data
2. `python retrieve_dlr -t` to retrieve all supplementary tables
3. `python retrieve_dlr -s` to retrieve anonymised survey responses

### Additional command-line options

`-csv`: (optional) saves files as .csv files instead of .feather (see notes on file format below)  
`-y`: (optional) start year for profile data retrieval  
`-z`: (optional) end year for profile data retrieval  

### File format
The default format for retrieving data is as a .feather file, which provides fast and efficient retrieval and uploads for data frames. It is compatible with both R and python. Feather files should be stored for working purposes only as the file format is not suitable for archiving. All feather files have been built under `feather.__version__ = 0.4.0`. If your feather package is of a later version, you may have trouble reading the files and will need to reconstruct them from the raw MSSQL database. Learn more about [feather](https://github.com/wesm/feather).

To save the data in csv format instead, pass `-csv` as option to your command line arguements.

## Data Processing

Install [dlr_data_processing]() for data processing.
