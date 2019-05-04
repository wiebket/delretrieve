#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the delretrieve package.

Updated: 4 May 2019
"""

import os
from pathlib import Path
import datetime as dt

#Data structure
home_dir = Path.home()
usr_dir = os.path.join(home_dir, 'del_data','usr')

def getDataDir():
    """
    This function checks if a valid data directory has been specified in
    USER_HOME/del_data/usr/store_path.txt.
    """
    
    filepath = []
    #read directory paths from store_path.txt file
    with open(os.path.join(usr_dir,'store_path.txt')) as f:
        for line in f:
            try:
                filepath.append(line.strip())
            except:
                pass
    mydir = filepath[0]
    validdir = os.path.isdir(mydir)
    #check if directory paths exist (does not validate directory structure)
    if validdir is False:
        #print('Your stored {} data path is invalid.'.format(k))
        raise 
    else:
        print('Your data path is \n{}'.format(mydir))
    
    return mydir


def specifyDataDir():
    """  
    This function creates the directory structure for saving data.    

    The following default locations are created:
    |-- your_data_dir (default: USER_HOME/del_data)
        |-- observations
            |-- profiles
            |-- tables
            
    """
    
    temp_obs_dir = os.path.join(home_dir,'del_data', 'observations') #default directory for observational data
    
    try:
        mydir = getDataDir()
    
    except:
        print('Data path not set or invalid directory.')       
        while True:
            mydir = input('The default path for storing data is \n{}\n Hit enter to keep the default or paste a new path to change it.\n'.format(temp_obs_dir))
            validdir = os.path.isdir(mydir)
            
            if validdir is False:
                print('\nThe directory does not exit. Creating it now ...')
                if len(mydir) == 0:
                    mydir = temp_obs_dir
                os.makedirs(mydir, exist_ok=True)
                    
            print('The data path has been set to \n{}\n'.format(mydir))
            break
        
        #write data dir to file   
        f = open(os.path.join(usr_dir,'store_path.txt'),'w')
        f.write(mydir)
        f.close()
        
    print('You can change it in USER_HOME/del_data/usr/store_path.txt')
    
    profiles_dir = os.path.join(mydir, 'profiles')
    table_dir = os.path.join(mydir, 'tables')
    rawprofiles_dir = os.path.join(profiles_dir, 'raw')
    
    return mydir, profiles_dir, table_dir, rawprofiles_dir

    
class InputError(ValueError):
    """
    Exception raised for errors in the input.

    *input*
    -------
    expression: input expression in which the error occurred
    message (str): explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
        
       
def validYears(*args):
    """
    This function checks if study was conducted during years specfied. 
    
    *input*
    -------
    *args (int)
    
    Valid arguments are years between 1994 and 2014.
    """
    
    for year in args:
        if year >= 1994 and year <= 2014:
            pass
        else:
            raise InputError([year], 'Year is out of range. Please select a year between 1994 and 2014')           
    return

def writeLog(log_line, file_name):    
    """
    This function adds a timestamp to a log line and writes it to a log file. 
    
    *input*
    -------
    log_line (dataframe)
    file_name (str): directory appended to USER_HOME/del_data/usr/ in which logs will be saved.
    """
    
    #Create log_dir and file to log path
    log_dir = os.path.join(usr_dir, 'logs')
    os.makedirs(log_dir , exist_ok=True)
    log_path = os.path.join(log_dir, file_name+'.csv')
    
    #Add timestamp
    log_line.insert(0, 'timestamp', dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    #Write log data to file
    if os.path.isfile(log_path):
        log_line.to_csv(log_path, mode='a', header=False, columns = log_line.columns, index=False)
        print('Log entries added to log/' + file_name + '.csv\n')
    else:
        log_line.to_csv(log_path, mode='w', columns = log_line.columns, index=False)
        print('Log file created and log entries added to log/' + file_name + '.csv\n')    
    return log_line
