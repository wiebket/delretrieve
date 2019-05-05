#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Functions to fetch data from the NRS Load Research General_LR4 MSSQL Server database. 
Requires a database instance and connection file that specifies access details.

Updated: 4 May 2019
"""

import pandas as pd
import numpy as np
import pyodbc 
import feather
import os

from .support import usr_dir, specifyDataDir, validYears, writeLog, InputError

obs_dir, profiles_dir, table_dir, rawprofiles_dir = specifyDataDir()


def getObs(tablename = None, querystring = 'SELECT * FROM tablename', chunksize = 10000):
    """Retrieves tables from a MSSQL server instance of the General_LR4 database. 

    Parameters:
        tablename (str): Valid table name in General_LR4 MSSQL database.
        querystring (str): Valid SQL SELECT statement.
        chunksize (int): Defaults to 10000.
    
    Requires USER_HOME/del_data/usr/cnxnstr.txt with database connection parameters.
    
    Returns:
        pandas dataframe: Tablename from General_LR4 MSSQL database.    
    """
    
    if tablename == 'Profiletable':
        return print('The profiles table is too large to read into python in one go. \
                     Use the getProfiles() function.')
 
    else:
        # Create connection object
        try:
            with open(os.path.join(usr_dir, 'cnxnstr.txt'), 'r') as f: 
                cnxnstr = f.read().replace('\n', '')
        except FileNotFoundError as err:
            print("Cannot find file with connection information at \
                  USER_HOME/del_data/usr/cnxnstr.txt: {0}".format(err))
            raise
            
        try:
            cnxn = pyodbc.connect(cnxnstr)  
            # Specify and execute query
            if querystring == 'SELECT * FROM tablename':
                query = "SELECT * FROM [General_LR4].[dbo].%s" % (tablename)
            else:
                query = querystring  
            df = pd.read_sql(query, cnxn)
            return df
        except Exception:
            raise


def getGroups():
    """Fetches and wrangles the group table to reshape it into a more usable format.
    
    Returns:
        pandas dataframe: Wrangled 'groups' table.
    """
    groups = getObs('Groups')
    groups['ParentID'].fillna(0, inplace=True)
    groups['ParentID'] = groups['ParentID'].astype('int64')
    groups['GroupName'] = groups['GroupName'].str.strip()
    
    # Deconstruct groups table apart into levels
    #LEVEL 1 GROUPS: domestic/non-domestic
    groups_level_1 = groups[groups['ParentID']==0] 
    #LEVEL 2 GROUPS: Eskom LR, NRS LR, Namibia, Clinics, Shops, Schools
    groups_level_2 = groups[groups['ParentID'].isin(groups_level_1['GroupID'])]
    #LEVEL 3 GROUPS: Years
    groups_level_3 = groups[groups['ParentID'].isin(groups_level_2['GroupID'])]
    #LEVEL 4 GROUPS: Locations
    groups_level_4 = groups[groups['ParentID'].isin(groups_level_3['GroupID'])]
    
    # Slim down the group levels to only include columns requried for merging
    g1 = groups.loc[groups['ParentID']==0,['GroupID','ParentID','GroupName']].reset_index(drop=True)
    g2 = groups.loc[groups['ParentID'].isin(groups_level_1['GroupID']), [
            'GroupID','ParentID','GroupName']].reset_index(drop=True)
    g3 = groups.loc[groups['ParentID'].isin(groups_level_2['GroupID']), [
            'GroupID','ParentID','GroupName']].reset_index(drop=True)
    
    # Reconstruct group levels as one pretty, multi-index table
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
    
    # Create multi-index dataframe
    allgroups = prettyg.set_index(['GroupID_1','GroupID_2','GroupID_3']).sort_index()
    allgroups['LocName'] = allgroups['Location'].apply(lambda x:x.partition(' ')[2])
        
    return allgroups


def getProfileID(group_year = None):
    """Fetches all profile IDs by group_year.
    
    Parameters:
        group_year (int): 1994 <= year <= 2014. Defaults to None (returns all).
    
    Returns:
        pandas dataframe: ProfileIDs for groups in year group_year.
    """
    links = getObs('LinkTable')
    allprofiles = links[(links.GroupID != 0) & (links.ProfileID != 0)]
    if group_year is None:
        return allprofiles

    # Match GroupIDs to getGroups to get the profile years:
    else:
        validYears(group_year) 
        allgroups = getGroups()
        allgroups.Year = allgroups.Year.astype(int)
        groupids = allgroups.loc[allgroups.Year == group_year, 'GroupID'] 
        profileid = pd.Series(allprofiles.loc[allprofiles.GroupID.isin(groupids),
                                              'ProfileID'].unique())
    return profileid


def getMetaProfiles(group_year, unit = None):
    """Fetches profile meta data by group_year and unit. 
    
    Parameters:
        group_year (int): 1994 <= year <= 2014  
        unit (str): 'A', 'V', 'kVA', 'Hz', 'kW'. Defaults to None (uses all).
    
    Returns:
        dict (pandas dataframe, list): Profile metadata, ProfileIDs for unit 
            and groups in year group_year.
    """
    # List of profiles for the year:
    pids = pd.Series(map(str, getProfileID(group_year))) 
    # Get observation metadata from the profiles table:
    metaprofiles = (getObs('profiles')[['Active','ProfileId','RecorderID',
                    'Unit of measurement']])
    # Select subset of metaprofiles corresponding to query
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
    """Fetches the load profiles of one unit for one month for groups in one year. 
    
    The retrieval is done incrementally to manage the large dataset.
    
    Parameters:
        group_year (int): 1994 <= year <= 2014
        month (int): 1 <= month <= 12
        unit (str): 'A', 'V', 'kVA', 'Hz', 'kW'
    
    Returns:
        pandas dataframe: electricity meter readings for profiles in month, unit, group_year.    
    """
    print('G'+str(group_year), month, unit)
    
    # Get metadata
    mp, plist = getMetaProfiles(group_year, unit)
    
    # Get profiles from server
    subquery = ', '.join(str(x) for x in plist)
    query = "SELECT pt.ProfileID \
     ,pt.Datefield \
     ,pt.Unitsread \
     ,pt.Valid \
    FROM [General_LR4].[dbo].[Profiletable] pt \
    WHERE pt.ProfileID IN (" + subquery + ") AND MONTH(Datefield) =" + str(month) + " \
    ORDER BY pt.Datefield, pt.ProfileID"
    profiles = getObs(querystring = query)
      
    df = pd.merge(profiles, mp, left_on='ProfileID', right_on='ProfileId')
    df.drop('ProfileId', axis=1, inplace=True)
    # Convert strings to category data type to reduce memory usage
    df.loc[:,['ProfileID','Valid']] = df.loc[:,['ProfileID','Valid']].apply(pd.Categorical)
       
    return df
    

def writeProfilePath(group_year, year, month, unit, filetype):
    """Creates the directory hierarchy and file names for writing raw profiles. 
    
    Files are named as follows:
        observationYear-observationMonth_GgroupYear_unit.filetype
        eg. 2008-1-G2007_A.csv 
        is current (A) data recorded in January 2008 for households in groups 
        for the year 2007 
                        
    Parameters:
        group_year (int): 1994 <= year <= 2014
        month (int): 1 <= month <= 12
        unit (str): 'A', 'V', 'kVA', 'Hz', 'kW'
        filetype (str): 'csv', 'feather'
    
    Returns:
        os.path: path_name_for_profile_file.filetype.
            
    Directory structure:
        data_path (defined in USER_HOME/del_data/usr/store_path.txt)
        |---profiles
            |---raw
                |---unit
                    |---year    
    """
    dir_path = os.path.join(rawprofiles_dir, str(unit), str(year))
    try:
        # Create profile directory if it does not exist
        os.makedirs(dir_path , exist_ok=True) 
    except Exception as e:
        return e
    
    file_path = os.path.join(dir_path, str(year)+'-'+str(month)+
                             '_G'+str(group_year)+'_'+str(unit)+
                             '.'+filetype)
      
    return file_path


def writeProfiles(group_year, month, unit, filetype):
    """Retrieves and saves profiles by group_year, month and units.
    
    The retrieval is done incrementally to manage the large dataset.
    
    Parameters:
        group_year (int): 1994 <= year <= 2014
        month (int): 1 <= month <= 12
        unit (str): 'A', 'V', 'kVA', 'Hz', 'kW'
        filetype (str): 'csv', 'feather'
    
    Returns:
        File saved to disk.
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
                filtered_df.to_csv(path, index=False)
            print(y, ': Write success')
        except Exception as e:
            print(y, ': Write FAIL')
            raise e
                
    return


def writeTables(names, dataframes): 
    """Saves a list of names with an associated list of dataframes as csv file. 
    
    getObs() and getGroups() functions can be used to construct the dataframes.
    
    Parameters:
        names (list): Names of tables to write. List items must be of type str.
        dataframes (list): Data to write. List items must be of type dataframe.
    
    Returns:
    Directory structure:
        data_path (defined in USER_HOME/del_data/usr/store_path.txt)
        |---tables
            |---[files].csv
    """
    # Create tables directory
    os.makedirs(table_dir, exist_ok=True)
    # Get data        
    datadict = dict(zip(names, dataframes))

    for k in datadict.keys():
        if datadict[k].size == datadict[k].count().sum():
            data = datadict[k]
        else:  
            data = datadict[k].fillna(np.nan) #feather doesn't write None type
        
        try:
            csv_path = os.path.join(table_dir, k + '.csv')
            data.to_csv(csv_path, index=False)
            print('successfully saved table '  + k)
        except Exception as e:
            print(e)
            pass            
    return


def saveTables():
    """Fetches tables from MSSQL server and saves them as csv files.
    
    Returns:
        Files saved to disk.
    """
    # Get important tables from DLR MSSQL Server
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
    
    tablenames = ['groups', 'questions', 'questionaires', 'qdtype', 
                  'qredundancy', 'qconstraints', 'answers', 'links', 
                  'profiles' ,'profilesummary','recorderinstall']
    tabledata = [groups, questions, questionaires, qdtype, qredundancy, 
                 qconstraints, answers, links, profiles, profilesummary, 
                 recorderinstall]
    # Saves tables to disk
    writeTables(tablenames, tabledata) 
    
    return print('Save database tables complete.\n')
 
    
def saveAnswers(anon=True):
    """Fetches survey responses.
    
    Parameters:
        anon (bool): Defaults to True.
    
    Returns:
        Files saved to disk.
    
    By default responses are anoynimsed and all discriminating personal information of respondents 
    are removed. The personally-identifying information is strictly confidential. This data should 
    only be used by qualified individuals. South African POPI regulations must be observed when 
    storing, processing and using personally identifying information.
    
    Specifications for questions to anonymise are contained in 
    USER_HOME/del_data/usr/blobAnon.csv and
    USER_HOME/del_data/usr/charAnon.csv.
    
    The following directory structure is created:
        data_path (defined in USER_HOME/del_data/usr/store_path.txt)
        |---tables
            |---answerfiles].csv
    """
    if anon is True:
        anstables = {'Answers_blob':'blobAnon.csv', 'Answers_char':'charAnon.csv', 'Answers_Number':None}    
        for k,v in anstables.items():
            # Get all answers
            a = getObs(k) 
            if v is None:
                pass
            else:
                qs = pd.read_csv(os.path.join(usr_dir, v))
                qs = qs.loc[lambda qs: qs.anonymise == 1, :]
                qanon = pd.merge(getObs('Answers'), qs, left_on='QuestionaireID', 
                                 right_on='QuestionaireID')[['AnswerID','ColumnNo','anonymise']]
                for i, rows in qanon.iterrows():
                    a.set_value(a[a.AnswerID == rows.AnswerID].index[0], str(rows.ColumnNo),'a')
            # Save anonymised answers to disk
            writeTables([k.lower() + '_anonymised'],[a]) 
    
        return print('Save anonymised survey responses complete.\n')
    
    else:
        anstables = ['Answers_blob','Answers_char','Answers_Number']
        ansdata = [getObs(a) for a in anstables]
        writeTables([a.lower() for a in anstables], ansdata)
        
        return print('Save survey responses complete.\nThis is personally-identifying, strictly confidential information. \nsYou are required to observe South African POPI regulations when storing and using this data.\n')
    

def saveRawProfiles(yearstart, yearend, filetype='feather'):
    """Saves all profiles for all groups in an ordered directory structure.
    
    Data loggers were changed in 2009 and the following profile unit restrictions
    are considered for groups before and after the logger change:
        [A, V] for group years 1994 - 2008 
        [A, V, kVA, Hz, kW] for group years 2009 - 2014
        
    Parameters:
        yearstart (int): 1994 <= year_start <= 2014
        yearend (int): year_start <= year_end <= 2014
        filetype (str): 'csv', 'feather'
    
    Returns:
        Files saved to disk.
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
                        writeLog(log_lines,'log_delretrieve_profiles')
    
        elif year >= 2009:
            for unit in ['A', 'V', 'kVA', 'Hz', 'kW']:
                for month in range(1, 13):
                    try:
                        writeProfiles(year, month, unit, filetype)
                    except Exception as e:
                        print(e)
                        logline = ['G'+str(year), unit, month, e]
                        log_lines = pd.DataFrame([logline], columns = ['group_year', 'unit', 'month', 'error'])
                        writeLog(log_lines,'log_delretrieve_profiles')
    
    return print('Save profiles complete.')