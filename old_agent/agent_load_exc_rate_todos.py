#------------------------------------------------------------
# Filename  : agent_load_exc_rate_todos.py
# Project   : ava
#
# Descr     : This holds routine to load ims_exc_load_todos with requests for
#             exchange rate data
#
# Params    : database
#             freq_type
#             cty_symbol1
#             cty_sysmbol2
#             str_start_datetime
#             str_end_datetime
#             progress_status
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-05-20   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


from datetime import datetime, timedelta
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db
from infrastructure.blackboard.load_exe_todos_manager import load_exe_todos_manager


def agent_load_exc_rate_todos(database, freq_type, cty_symbol1, cty_symbol2,
                              str_start_datetime, str_end_datetime, progress_status):

    load_exe_todos_manager(database           = database,
                           freq_type          = freq_type, 
                           cty_symbol1        = cty_symbol1,
                           cty_symbol2        = cty_symbol2,           
                           str_start_datetime = str_start_datetime, 
                           str_end_datetime   = str_end_datetime, 
                           progress_status    = progress_status)

    return


if __name__ == "__main__":

    print("Open db")
    print(" ")
    
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   

    # We want to load todos up to end of business yesterday. If it runs on a 
    # monday it will just get no data for sat and sun
      
    freq_type          = '1 min'
    cty_symbol1        = 'USD'
    cty_symbol2        = 'CAD'
    str_start_datetime = '%' 
    str_end_date       = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    str_end_datetime   = str_end_date + " 23:59:59"
    progress_status    = 'RDY'
    
    agent_load_exc_rate_todos(database           = database, 
                              freq_type          = freq_type,
                              cty_symbol1        = cty_symbol1,
                              cty_symbol2        = cty_symbol2, 
                              str_start_datetime = str_start_datetime, 
                              str_end_datetime   = str_end_datetime, 
                              progress_status    = progress_status) 
    
    print(" ")
    print("Close db")
    print(" ")

    close_db(database = database)  
    

