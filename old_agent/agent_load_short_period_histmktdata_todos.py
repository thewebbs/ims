#------------------------------------------------------------
# Filename  : agent_load_short_period_histmktdata_todos.py
# Project   : ava
#
# Descr     : This holds routine to load ims_load_todos with requests for
#             historic market data for a short duration only - between start 
#             and end datetime format '2020-12-01 23:59:59'
#
# Params    : database
#             rec_type
#             freq_type
#             inv_ticker
#             str_start_datetime
#             str_end_datetime
#             progress_status
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-12-02   1 MW  Initial write based on agent_load_histmktdata_todos
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


#from datetime import datetime, timedelta, time
from infrastructure.blackboard.load_todos_manager import create_batch_load_todos
from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import sql_select_all
#from utils.utils_datetime import daterange
from utils.utils_database import close_db, open_db
#from infrastructure.blackboard.old.load_todos_manager import load_todos_manager


def agent_load_short_period_histmktdata_todos(database, rec_type, freq_type, inv_ticker, 
                     str_start_datetime, str_end_datetime, progress_status):

    ticker_list = get_not_errored_tickers(database = database, inv_ticker = inv_ticker)
    
    if ticker_list != None:
                
                for (this_inv_ticker, inv_load_priority) in ticker_list:
                    
                    progress = 'Creating ims_load_todos HISTMKTDATA %s %s records for %s from %s to %s' % (rec_type, freq_type, this_inv_ticker, str_start_datetime, str_end_datetime)
                    print(progress)
                            
                    create_batch_load_todos(database = database,
                                            inv_ticker         = this_inv_ticker, 
                                            rec_type           = rec_type, 
                                            freq_type          = freq_type, 
                                            str_start_datetime = str_start_datetime, 
                                            str_end_datetime   = str_end_datetime, 
                                            progress_status    = progress_status, 
                                            load_priority      = inv_load_priority)

    return

def get_not_errored_tickers(database, inv_ticker):
    
    sql_statement = "SELECT inv_ticker, inv_load_priority FROM ims_investments "
    sql_statement += " WHERE inv_ticker like '%s' " % (inv_ticker)
    sql_statement += " and inv_load_status not like 'Err%' "
    sql_statement += " order by inv_ticker "  
    print(sql_statement)
    if DEBUG:
        print("get_not_errored_tickers")
          
    valid_tickers_db = sql_select_all(database      = database, 
                                      sql_statement = sql_statement)
    
    print(valid_tickers_db)
    
    if DEBUG:
        print(valid_tickers_db)
          
    return valid_tickers_db
    


if __name__ == "__main__":

    print("Open db")
    print(" ")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   

      
    freq_type          = '1 min'
    inv_sec_name       = '%'
    str_start_datetime = '2020-01-01 06:30:00' 
    str_end_datetime   = '2020-12-28 23:59:59'
    progress_status    = 'RDY1TICKER'
    rec_type           = 'HISTMKTDATA_BID'
 
    # both NYSE and TO
    #for inv_ticker in ('L.NYSE','TD.NYSE','ATH.NYSE','AXL.NYSE','BX.NYSE','BYD.NYSE','CBL.NYSE','CCS.NYSE','CEF.NYSE','CM.NYSE',
    #                   'L.TO','TD.TO','ATH.TO','AXL.TO','BX.TO','BYD.TO','CBL.TO','CCS.TO','CEF.TO','CM.TO'): 
    #for inv_ticker in ('AAV.NYSE','AGF.NYSE','AET.NYSE','AF.NYSE','AGU.NYSE','AUG.NYSE','AOI.NYSE','AVP.NYSE','AVA.NYSE','BAA.NYSE',
    #                   'AAV.TO','AGF.TO','AET.TO','AF.TO','AGU.TO','AUG.TO','AOI.TO','AVP.TO','AVA.TO','BAA.TO'): 
    
    # just NYSE
    #for inv_ticker in ('A.NYSE','AA.NYSE','AACS.NYSE','AAL.NYSE','AAMC.NYSE','AAME.NYSE','AAN.NYSE','AAOI.NYSE','AAON.NYSE','AAP.NYSE'): 
    #for inv_ticker in ('AAAP.NYSE','AABVF.NYSE','AAC.NYSE','AACAY.NYSE','AACTF.NYSE','AAEH.NYSE','AAGIY.NYSE','AAIIQ.NYSE','AAIR.NYSE','AAPC.NYSE'): 
    #for inv_ticker in ('ABBY.NYSE','ACET.NYSE','ACNV.NYSE','ACRL.NYSE','ADTC.NYSE','AFGE.NYSE','AFTM.NYSE','AGC.NYSE','ACCZ.NYSE','AGIN.NYSE','AHROQ.NYSE'):
    for inv_ticker in ('AGCZ.NYSE','ALAN.NYSE','ALIF.NYSE','ALVRQ.NYSE','AMAZ.NYSE','AMBS.NYSE','APGI.NYSE','APSI.NYSE','ARBU.NYSE','ARNI.NYSE',\
                       'ARRY.NYSE','ARSN.NYSE','ASPZ.NYSE','ATLDF.NYSE','ATLS.NYSE', 'ATVK.NYSE', 'AUTR.NYSE', 'AVEW.NYSE','AVIR.NYSE', 'AVRN.NYSE'):
         
        agent_load_short_period_histmktdata_todos(database           = database, 
                                                  rec_type           = rec_type, 
                                                  freq_type          = freq_type,
                                                  inv_ticker         = inv_ticker, 
                                                  str_start_datetime = str_start_datetime, 
                                                  str_end_datetime   = str_end_datetime, 
                                                  progress_status    = progress_status) 
        
        rec_type = 'HISTMKTDATA_ASK'
        
        agent_load_short_period_histmktdata_todos(database           = database, 
                                                  rec_type           = rec_type, 
                                                  freq_type          = freq_type,
                                                  inv_ticker         = inv_ticker, 
                                                  str_start_datetime = str_start_datetime, 
                                                  str_end_datetime   = str_end_datetime, 
                                                  progress_status    = progress_status) 
        
        rec_type = 'HISTMKTDATA_TRADES'
        
        agent_load_short_period_histmktdata_todos(database           = database, 
                                                  rec_type           = rec_type, 
                                                  freq_type          = freq_type,
                                                  inv_ticker         = inv_ticker, 
                                                  str_start_datetime = str_start_datetime, 
                                                  str_end_datetime   = str_end_datetime, 
                                                  progress_status    = progress_status) 
        
    print(" ")
    print("Close db")
    print(" ")

    close_db(database = database)  
    

