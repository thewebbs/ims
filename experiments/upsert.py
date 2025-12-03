# ------------------------------------------------------------
# filename  : upsert.py
# descr     : test general ourpose upsert 
#
# in       : ()
# out      : ()
#
# date       ver who change
# ---------- --- --- ------
# 2025-12-02 101 DW  initial write
# ------------------------------------------------------------

import AvaAgtOra
import AvaAgtLog

# ============================================================================================================================
# config
# ============================================================================================================================

from utils.config import AGT_KND_ERR, AGT_KND_LOG
from utils.config import DB_HOST,     DB_PORT,          DB_TIMEZONE,     DB_TNS_SERVICE, DB_USERNAME, DB_PASSWORD
from utils.config import FOLDER_ERR,  FOLDER_LOG


# ============================================================================================================================
# functions
# ============================================================================================================================

# ---------------------------------------------------------------------------------------------------------------------------
# upsert_test
# ----------------------------------------------------------------------------------------------------------------------------

def upsert_test(agt_err, agt_log, agt_ora):
   
    data_dict = {'PEO_KEY'      : 'D',
                 'PEO_NAME'     : 'Dave',
                 'PEO_EMOJI'    : 'x',
                 'PEO_OURA_PAT' : '',
                 'PEO_ACTIVE_YN': 'Y'
                 }
        
    agt_ora.agt_put(table_name = 'AVA_PEOPLE', data_dict = data_dict)
        
    return




    
#============================================================================================================================
# main 
#============================================================================================================================

if __name__ == '__main__':

    # create log agent
        
    file_folder = FOLDER_LOG
    file_name   = 'read_emails.log'
    file_kind   = AGT_KND_LOG
  
    log_params  = (file_folder, file_name, file_kind)
    agt_log     = AvaAgtLog(key = 'agent - log', params = log_params)
    
    agt_log.title_put(text = 'starting creating ava_agents')
        
    # create error agent
    
    file_folder = FOLDER_ERR
    file_name   = 'read_emails.err'
    file_kind   = AGT_KND_ERR

    err_params  = (file_folder, file_name, file_kind)
    agt_err     = AvaAgtLog(key = 'agent - err', params = err_params)   
       
    # create oracle agent
        
    ora_params      = (DB_HOST, DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE, DB_USERNAME, DB_PASSWORD)
    agt_ora         = AvaAgtOra(key = 'agent - ora', agt_ctl = None, agt_err = agt_err, agt_log = agt_log, params = ora_params)
   
    
    upsert_test(agt_err = agt_err, agt_log = agt_log, agt_ora = agt_ora)