#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Functions to fetch data from the Domestic Load Research General_LR4 MSSQL Server 
database. Requires a database instance and connection file that specifies access 
details.

Updated: 22 March 2019
"""

import pandas as pd
import numpy as np
import pyodbc 
import feather
import os

from .support import usr_dir, specifyDataDir, validYears, writeLog, InputError

obs_dir, profiles_dir, table_dir, rawprofiles_dir = specifyDataDir()

def getObs(tablename = None, querystring = 'SELECT * FROM tablename', chunksize = 10000):
    """
    This function fetches tables from a MSSQL server instance of the DLR database 
    and returns it as a pandas dataframe. 

    *input*
    -------
    tablename (str): valid table name in General_LR4 MSSQL database
    querystring (str): valid SQL SELECT statement
    chunksize (int): default 10000
    
    Requires USER_HOME/dlr_data/usr/cnxnstr.txt with database connection parameters.
    """
    
    if tablename == 'Profiletable':
        return print('The profiles table is too large to read into python in one go. \
                     Use the getProfiles() function.')
 
    else:
        #create connection object:
        try:
            with open(os.path.join(usr_dir, 'cnxnstr.txt'), 'r') as f: 
                cnxnstr = f.read().replace('\n', '')
        except FileNotFoundError as err:
            print("Cannot find file with connection information at \
                  USER_HOME/dlr_data/usr/cnxnstr.txt: {0}".format(err))
            raise
            
        try:
            cnxn = pyodbc.connect(cnxnstr)  
            #specify and execute query:
            if querystring == 'SELECT * FROM tablename':
                query = "SELECT * FROM [General_LR4].[dbo].%s" % (tablename)
            else:
                query = querystring  
            df = pd.read_sql(query, cnxn)   #read to dataframe   
            return df
        except Exception:
            raise


def getGroups():
    """
    This function fetches the group table and performs massive data wrangling 
    to reshape it into a more usable format.
    """
    
    groups = getObs('Groups')
    groups['ParentID'].fillna(0, inplace=True)
    groups['ParentID'] = groups['ParentID'].astype('int64')
    groups['GroupName'] = groups['GroupName'].str.strip()
    
    #Deconstruct groups table apart into levels
    #LEVEL 1 GROUPS: domestic/non-domestic
    groups_level_1 = groups[groups['ParentID']==0] 
    #LEVEL 2 GROUPS: Eskom LR, NRS LR, Namibia, Clinics, Shops, Schools
    groups_level_2 = groups[groups['ParentID'].isin(groups_level_1['GroupID'])]
    #LEVLE 3 GROUPS: Years
    groups_level_3 = groups[groups['ParentID'].isin(groups_level_2['GroupID'])]
    #LEVLE 4 GROUPS: Locations
    groups_level_4 = groups[groups['ParentID'].isin(groups_level_3['GroupID'])]
    
    #Slim down the group levels to only include columns requried for merging
    g1 = groups.loc[groups['ParentID']==0,['GroupID','ParentID','GroupName']].reset_index(drop=True)
    g2 = groups.loc[groups['ParentID'].isin(groups_level_1['GroupID']), [
            'GroupID','ParentID','GroupName']].reset_index(drop=True)
    g3 = groups.loc[groups['ParentID'].isin(groups_level_2['GroupID']), [
            'GroupID','ParentID','GroupName']].reset_index(drop=True)
    
    #reconstruct group levels as one pretty, multi-index table
    recon3 = pd.merge(groups_level_4, g3, left_on ='ParentID', 
                      right_on = 'GroupID' , how='left', suffixes = ['_4','_3'])
    recon2 = pd.merge(recon3, g2, left_on ='ParentID_3',
                      right_on = 'GroupID' , how='left', suffixes = ['_3','_2'])
    recon1 = pd.merge(recon2, g1, left_on ='ParentID', 
                      right_on = 'GroupID' , how='left', suffixes = ['_2','_1'])
    prettyg = recon1[['ContextID','GroupID_1','GroupID_2','GroupID_3','GroupID_4',
                      'GroupName_1','GroupName_2','GroupName_3','GroupName_4']]
    prettynames = ['ContextID', 'GroupID_1','GroupID_2','GroupID_3','GroupID',
                   'Dom_NonDom','Survey','Year','Location']
    prettyg.columns = prettynames
    
    #create multi-index dataframe
    allgroups = prettyg.set_index(['GroupID_1','GroupID_2','GroupID_3']).sort_index()
    allgroups['LocName'] = allgroups['Location'].apply(lambda x:x.partition(' ')[2])
        
    return allgroups


def getProfileID(group_year = None):
    """
    This function fetches all profile IDs by group year.
    
    *input*
    -------
    group_year (int): range(1994, 2014) inclusive
    
    If group_year is None, then profile IDs for all years are retrieved.
    """
    
    links = getObs('LinkTable')
    allprofiles = links[(links.GroupID != 0) & (links.ProfileID != 0)]
    if group_year is None:
        return allprofiles

    #match GroupIDs to getGroups to get the profile years:
    else:
        validYears(group_year) #check if year is valid
        allgroups = getGroups()
        allgroups.Year = allgroups.Year.astype(int)
        groupids = allgroups.loc[allgroups.Year == group_year, 'GroupID'] 
        profileid = pd.Series(allprofiles.loc[allprofiles.GroupID.isin(groupids),
                                              'ProfileID'].unique())
    return profileid


def getMetaProfiles(group_year, unit = None):
    """
    This function fetches profile meta data by group year and unit. 
    
    *input*
    -------
    group_year (int): range(1994, 2014) inclusive
    unit (str): None, 'A', 'V', 'kVA', 'Hz', 'kW'
    
    If unit is None, then meta data of profiles for all units is retrieved.
    """
    
    #list of profiles for the year:
    pids = pd.Series(map(str, getProfileID(group_year))) 
    #get observation metadata from the profiles table:
    metaprofiles = (getObs('profiles')[['Active','ProfileId','RecorderID',
                    'Unit of measurement']])
    #select subset of metaprofiles corresponding to query
    metaprofiles = metaprofiles[metaprofiles.ProfileId.isin(pids)] 
    metaprofiles.rename(columns={'Unit of measurement':'UoM'}, inplace=True)
    metaprofiles.loc[:,['UoM', 'RecorderID']] = (
            metaprofiles.loc[:,['UoM','RecorderID',]].apply(pd.Categorical))
    puom = getObs('ProfileUnitsOfMeasure').sort_values(by=['UnitsID'])
    cats = list(puom.loc[puom.UnitsID.isin(metaprofiles['UoM'].cat.categories), 'Description'])
    metaprofiles['UoM'].cat.categories = cats

    if unit is None:
        plist = metaprofiles['ProfileId']
    elif unit in ['V','A','kVA','kW']:
        uom = unit.strip() + ' avg'
        plist = metaprofiles[metaprofiles.UoM == uom]['ProfileId']
    elif unit=='Hz':
        uom = 'Hz'
        plist = metaprofiles[metaprofiles.UoM == uom]['ProfileId']
    else:
        return print('Check spelling and choose V, A, kVA, Hz or kW as units, \
                     or leave blank to get profiles of all units.')
    
    return metaprofiles, plist


def getProfiles(group_year, month, unit):
    """
    This function fetches the load profiles of one unit for one month for groups
    in one year. The retrieval is done incrementally to manage the large dataset.
    
    *input*
    -------
    group_year (int): range(1994, 2014) inclusive
    month (int): rang(1, 12) inclusive
    unit (str): 'A', 'V', 'kVA', 'Hz', 'kW'
    """
    
    print('G'+str(group_year), month, unit)
    
    # Get metadata
    mp, plist = getMetaProfiles(group_year, unit)
    
    ## Get profiles from server
    subquery = ', '.join(str(x) for x in plist)
    query = "SELECT pt.ProfileID \
     ,pt.Datefield \
     ,pt.Unitsread \
     ,pt.Valid \
    FROM [General_LR4].[dbo].[Profiletable] pt \
    WHERE pt.ProfileID IN (" + subquery + ") AND MONTH(Datefield) =" + str(month) + " \
    ORDER BY pt.Datefield, pt.ProfileID"
    profiles = getObs(querystring = query)
    
    #data output:    
    df = pd.merge(profiles, mp, left_on='ProfileID', right_on='ProfileId')
    df.drop('ProfileId', axis=1, inplace=True)
    #convert strings to category data type to reduce memory usage
    df.loc[:,['ProfileID','Valid']] = df.loc[:,['ProfileID','Valid']].apply(pd.Categorical)
       
    return df
    

def writeProfilePath(group_year, year, month, unit, filetype):
    """
    This function creates the directory hierarchy and file names for writing 
    raw profiles. 
    
    Files are named as follows:
        observationYear-observationMonth_GgroupYear_unit.filetype
        eg. 2008-1-G2007_A.csv 
        is current (A) data recorded in January 2008 for households in groups 
        for the year 2007 
                        
    *input*
    -------
    group_year (int): range(1994, 2014) inclusive
    month (int): rang(1, 12) inclusive
    unit (str): 'A', 'V', 'kVA', 'Hz', 'kW'
    filetype (str): 'csv', 'feather'
    """
    
    dir_path = os.path.join(rawprofiles_dir, str(unit), str(year))
    try:
        os.makedirs(dir_path , exist_ok=True) #create profile directory if it does not exist
    except Exception as e:
        return e
    
    file_path = os.path.join(dir_path, str(year)+'-'+str(month)+'_G'+str(group_year)+'_'+str(unit)+'.'+filetype)
      
    return file_path


def writeProfiles(group_year, month, unit, filetype):
    """
    This function retrieves profiles by group year, month and units and saves 
    them to disk. The retrieval is done incrementally to manage the large dataset.
    
    *input*
    -------
    group_year (int): range(1994, 2014) inclusive
    month (int): rang(1, 12) inclusive
    unit (str): 'A', 'V', 'kVA', 'Hz', 'kW'
    filetype (str): 'csv', 'feather'
    """
    
    df = getProfiles(group_year, month, unit)
    try:
        yrs = df.Datefield.dt.year.unique()
    except:
        expr = '-'.join(['G'+str(group_year), str(month), unit])
        raise InputError(expr, 'no data collected.')
        
    for y in yrs:
        path = writeProfilePath(group_year, y, month, unit, filetype)
        filtered_df = df[df.Datefield.dt.year == y].reset_index(drop=True)
        
        try:
            if filetype=='feather':
                feather.write_dataframe(filtered_df, path)
            elif filetype=='csv':
                filtered_df.to_csv(path)
            print(y, ': Write success')
        except Exception as e:
            print(y, ': Write FAIL')
            raise e
                
    return


def writeTables(names, dataframes): 
    """
    This function saves a list of names with an associated list of dataframes 
    as csv and feather files. The getObs() and getGroups() functions can be 
    used to construct the dataframes.
    
    *input*
    -------
    names (list): list items must be of type str
    dataframes (list): list items must be of type dataframe
    
    """
    #create data directories
    
    os.makedirs(os.path.join(table_dir, 'feather') , exist_ok=True)
    os.makedirs(os.path.join(table_dir, 'csv') , exist_ok=True)

    #get data        
    datadict = dict(zip(names, dataframes))
    for k in datadict.keys():
        if datadict[k].size == datadict[k].count().sum():
            data = datadict[k]
        else:  
            data = datadict[k].fillna(np.nan) #feather doesn't write None type
        
        try:
            feather_path = os.path.join(table_dir, 'feather', k + '.feather')
            feather.write_dataframe(data, feather_path)
            csv_path = os.path.join(table_dir, 'csv', k + '.csv')
            data.to_csv(csv_path, index=False)
            print('successfully saved table '  + k)
        except Exception as e:
            print(e)
            pass            
    return


def saveTables():
    """
    This function fetches tables from MSSQL server and saves them as both a
    csv file and a feather object.
    
    The following directory structure is created:
        data_path (defined in USER_HOME/dlr_data/usr/store_path.txt)
        |---tables
            |---csv
                |---[files].csv
            |---feather
                |---[files].feather
    """
    
    #Get important tables from DLR MSSQL Server
    groups = getGroups() 
    questions = getObs('Questions')
    questionaires = getObs('Questionaires')
    qdtype = getObs('QDataType')
    qredundancy = getObs('QRedundancy')
    qconstraints = getObs('QConstraints')
    answers = getObs('Answers')
    links = getObs('LinkTable')
    profiles = getObs('Profiles')
    profilesummary = getObs('ProfileSummaryTable')
    recorderinstall = getObs('RECORDER_INSTALL_TABLE')
    
    tablenames = ['groups', 'questions', 'questionaires', 'qdtype', 'qredundancy', 'qconstraints', 'answers', 'links', 'profiles' ,'profilesummary','recorderinstall']
    tabledata = [groups, questions, questionaires, qdtype, qredundancy, qconstraints, answers, links, profiles, profilesummary, recorderinstall]
    
    writeTables(tablenames, tabledata) #saves tables to disk
    
    return print('Save database tables complete.\n')
 
def saveAnswers():
    """
    This function fetches survey responses and anonymises them to remove all 
    discriminating personal information of respondents. The anonymised dataset 
    is returned and saved as both a csv file and a feather object. 
    
    Specifications for questions to anonymise are contained in 
    USER_HOME/dlr_data/usr/blobAnon.csv and
    USER_HOME/dlr_data/usr/charAnon.csv.
    
    The following directory structure is created:
        data_path (defined in USER_HOME/dlr_data/usr/store_path.txt)
        |---tables
            |---csv
                |---[files].csv
            |---feather
                |---[files].feather
    """
    
    anstables = {'Answers_blob':'blobAnon.csv', 'Answers_char':'charAnon.csv', 'Answers_Number':None}    
    for k,v in anstables.items():
        a = getObs(k) #get all answers
        if v is None:
            pass
        else:
            qs = pd.read_csv(os.path.join(usr_dir, v))
            qs = qs.loc[lambda qs: qs.anonymise == 1, :]
            qanon = pd.merge(getObs('Answers'), qs, left_on='QuestionaireID', right_on='QuestionaireID')[['AnswerID','ColumnNo','anonymise']]
            for i, rows in qanon.iterrows():
                a.set_value(a[a.AnswerID == rows.AnswerID].index[0], str(rows.ColumnNo),'a')
        
        writeTables([k.lower() + '_anonymised'],[a]) #saves answers to disk
    
    return print('Save anonymised survey responses complete.\n')
    

def saveRawProfiles(yearstart, yearend, filetype='feather'):
    """
    This function saves all profiles for all groups in an ordered directory 
    structure by unit and year.
    
    Data loggers were changed in 2009 and the following profile unit restrictions
    are considered for groups before and after the logger change:
        [A, V] for group years 1994 - 2008 
        [A, V, kVA, Hz, kW] for group years 2009 - 2014
        
    The following directory structure is created:
        data_path (defined in USER_HOME/dlr_data/usr/store_path.txt)
        |---profiles
            |---raw
                |---unit
                    |---year
                        |---[files].csv/.feather
        
    *input*
    -------
    yearstart (int): range(1994, 2014) inclusive
    yearend (int): range(1994, 2014) inclusive
    filetype (str): 'csv', 'feather'
    """
    
    for year in range(yearstart, yearend + 1):
        if year < 2009:
            for unit in ['A','V']:
                for month in range(1, 13):
                    try:
                        writeProfiles(year, month, unit, filetype)
                    except Exception as e:
                        print(e)
                        logline = ['G'+str(year), unit, month, e]
                        log_lines = pd.DataFrame([logline], columns = ['group_year', 'unit', 'month', 'error'])
                        writeLog(log_lines,'log_dlrretrieve_profiles')
    
        elif year >= 2009:
            for unit in ['A', 'V', 'kVA', 'Hz', 'kW']:
                for month in range(1, 13):
                    try:
                        writeProfiles(year, month, unit, filetype)
                    except Exception as e:
                        print(e)
                        logline = ['G'+str(year), unit, month, e]
                        log_lines = pd.DataFrame([logline], columns = ['group_year', 'unit', 'month', 'error'])
                        writeLog(log_lines,'log_dlrretrieve_profiles')
    
    return print('Save profiles complete.')