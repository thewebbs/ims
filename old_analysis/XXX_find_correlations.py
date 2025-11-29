#------------------------------------------------------------
# Filename  : find_correlations.py
# Project   : ava
#
# Descr     : This file contains code to graph pricing data
#
# Params    : database
#             start_date
#             end_date
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2016-02-19   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db
from datetime import date 
from decimal import Decimal
from apis.dataload.load_drive_table import load_drive_table
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from utils.utils_dataframes import format_drive_table_df
from utils.utils_graphs import plot_corr

 
def find_correlations(database, start_date, end_date):     

    
    #
    #=============================================================================
    #
    # drive graph from the drive_table but populate it first with investments in a sector

    print("first load data frame from the drive table but populate it first with some investments from a few sectors")
    
    run_name = 'findcorrelation'    
    '''
    delete_first = True
    load_type = 'SECTOR'
    sector_name = 'Tech'
    load_drive_table(run_name, load_type, sector_name, '', delete_first, start_date, end_date)
    
    delete_first = True
    load_type = 'SECTOR'
    sector_name = 'Financial'
    load_drive_table(run_name, load_type, sector_name, '', delete_first, start_date, end_date)
    '''
    delete_first = True
    load_type = 'SECTOR'
    sector_name = 'REIT'
    load_drive_table(run_name, load_type, sector_name, '', delete_first, start_date, end_date)
    
    #
    #=============================================================================
    #
    # next load these investments into a dataframe
    print("next load these investments into a dataframe")
    correlated_df = pd.DataFrame()
    investments_df = format_drive_table_df(run_name, start_date, end_date)
    #
    #=============================================================================
    #
    # next find the correlation between these investments into a new dataframe
    #print("next find the correlation between these investments into a new dataframe")
    correlated_df = pd.DataFrame()
    correlated_df = investments_df.corr(method='pearson', min_periods=6)    
    print(correlated_df)
    #
    
    
    # need to remove any NaN columns
    
    
    #=============================================================================
    #
    # next calculate the mean and standard deviations of the correlation between these investments into a new series
    print("next calculate the mean and standard deviations of the correlation between these investments into a new series")
    mean_s = correlated_df.mean()
    mean_s = mean_s[~np.isnan(mean_s)]
    
    std_s = correlated_df.mean()
    std_s = std_s[~np.isnan(std_s)]
    
    #
    #=============================================================================
    #
    # next sort the records into a new series
    sorted_mean_s = mean_s.sort_values(ascending=False)
    print("sorted_mean")
    print(sorted_mean_s)
    
    sorted_std_s = std_s.sort_values(ascending=False)
    #print("sorted_std")
    #print sorted_std_s


    if len(correlated_df.index) > 0:
        plot_corr(correlated_df,size=20)
        plt.show()
        
    return 
 
 
if __name__ == "__main__":
    
    print("Started find_correlations")
    print(" ")
    print("Open Database")
     
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)    
        
    start_date = '2016-01-03'
    end_date   = '2016-12-31'   
 
    find_correlations(database   = database,
                      start_date = start_date,
                      end_date   = end_date)
    
    print("Close database")
    
    close_db(database = database)   
    
    print("Finished find_correlations") 
 