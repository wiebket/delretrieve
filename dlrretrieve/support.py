#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the dlrretrieve module
"""

import os
from pathlib import Path
import datetime as dt

#Data structure
home_dir = Path.home()
usr_dir = os.path.join(home_dir, 'dlr_data','usr')
os.makedirs(usr_dir, exist_ok=True)

temp_table_dir = os.path.join(home_dir,'dlr_data', 'observations', 'tables') #default tables directory
temp_profiles_dir = os.path.join(home_dir,'dlr_data', 'observations', 'profiles') #default profiles directory
    
def specifyDataDir():
    """      
    """
    
    dirs = {'profiles':temp_profiles_dir, 'tables':temp_table_dir}

    for k, v in dirs.items():
        #check if dlr_data/.../profiles/ and dlr_data/.../tables/ exists in default locations

        try:
            filepaths = {}
            #read directory paths from store_path.txt file
            with open(os.path.join(usr_dir,'store_path.txt')) as f:
                for line in f:
                    try:
                        i, j = line.split(',')
                        filepaths[i] = j.strip()
                    except:
                        pass

            mydir = filepaths[k]
            validdir = os.path.isdir(mydir)
            #check if directory paths exist (does not validate directory structure)
            if validdir is False:
                #print('Your stored {} data path is invalid.'.format(k))
                raise 
            else:
                print('Your {} data path is {}.'.format(k, mydir))
        
        except:
            print('The default path for storing {} data is \n{}'.format(k, v))
            
            while True:
                mydir = input('Hit enter to keep the default or paste a new {} path to change.\n'.format(k))
                validdir = os.path.isdir(mydir)
                
                if validdir is False:
                    if len(mydir) == 0:
                        mydir = v
                        validdir = True
                    else:
                        print('This is not a directory. Try again.')
                        continue
                if validdir is True:
                    break
        dirs[k] = mydir
        
    #write rawprofiles dir to file   
    f = open(os.path.join(usr_dir,'store_path.txt'),'w')
    for i in dirs.items():
        f.write(', '.join(i)+'\n')
    f.close()
    
    print('\nYou can change your data paths in /your_home_directory/dlr_data/usr/store_path.txt')
    
    return dirs['profiles'], dirs['tables']

#Data structure
usr_data = specifyDataDir()
profiles_dir = usr_data[0]
table_dir = usr_data[1]
rawprofiles_dir = os.path.join(profiles_dir, 'raw')
obs_dir = os.path.dirname(profiles_dir)
data_dir = os.path.dirname(obs_dir)
fdata_dir = os.path.join(os.path.dirname(usr_dir), 'survey_features')
pdata_dir = os.path.join(os.path.dirname(usr_dir), 'resampled_profiles')    

for d in [table_dir, rawprofiles_dir]:
    os.makedirs(d, exist_ok=True)

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
