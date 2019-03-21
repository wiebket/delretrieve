#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the dlrretrieve module

Updated: 21 March 2019
"""

import os
from pathlib import Path
import datetime as dt

#Data structure
home_dir = Path.home()
usr_dir = os.path.join(home_dir, 'dlr_data','usr')
os.makedirs(usr_dir, exist_ok=True)

def getDataDir():
    """
    This function checks if a valid data directory has been specified.
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
    """
    temp_obs_dir = os.path.join(home_dir,'dlr_data', 'observations') #default directory for observational data
    
    try:
        mydir = getDataDir()
    
    except:
        print('Data path not set or invalid directory.')       
        while True:
            mydir = input('The default path for storing data is \n{}\nHit enter to keep the default or paste a new path to change it.\n'.format(temp_obs_dir))
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
        
    print('You can change it in your_home_directory/dlr_data/usr/store_path.txt')
    
    profiles_dir = os.path.join(mydir, 'profiles')
    table_dir = os.path.join(mydir, 'tables')
    rawprofiles_dir = os.path.join(profiles_dir, 'raw')
    
    return mydir, profiles_dir, table_dir, rawprofiles_dir

def createDataDirs(table_dir, rawprofiles_dir):
        
    print('Creating subdirectories...')
    for d in [table_dir, rawprofiles_dir]:
        os.makedirs(d, exist_ok=True)
    
    return
    
class InputError(ValueError):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
        
       
def validYears(*args):
    """
    Checks if year range is valid. Valid years are between 1994 and 2014.
    """
    
    for year in args:
        if year >= 1994 and year <= 2014:
            pass
        else:
            raise InputError([year], 'Year is out of range. Please select a year between 1994 and 2014')           
    return

def writeLog(log_line, file_name):    
    """
    Adds timestamp column to dataframe, then write dataframe to csv log file. 
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
