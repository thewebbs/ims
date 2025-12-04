# ------------------------------------------------------------
# filename  : cre_data_ims.py
# descr     : create reference data for ims 
#
# in       : ()
# out      : ()
#
# date       ver who change
# ---------- --- --- ------
# 2025-12-03 101 DW  initial write
# ------------------------------------------------------------

from agents.AvaAgtOra        import AvaAgtOra
from agents.AvaAgtLog        import AvaAgtLog

from objects.ImsCurrencyType import ImsCurrencyType
from objects.ImsExchange     import ImsExchange
from objects.ImsSector       import ImsSector

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
# cre_data_ims
# ----------------------------------------------------------------------------------------------------------------------------

def cre_data_ims(agt_err, agt_log, agt_ora):
   
    agt_log.title_put(text = 'starting creating data')
    
    #
    # exchanges
    #
    
    agt_log.title_put(text = 'ImsExchanges')

    row_lis = []
    
    row_lis.append(ImsExchange('ASK',      'Australian Securities Exchange', '', '').obj_get(dict))
    row_lis.append(ImsExchange('EURONEXT', 'Euronext',                       '', '').obj_get(dict))
    row_lis.append(ImsExchange('FWB',      'Frankfurt Stock Exchange',       '', '').obj_get(dict))
    row_lis.append(ImsExchange('LSE',      'London Stock Exchange',          '', '').obj_get(dict))
    row_lis.append(ImsExchange('NYSE',     'New York Stock Exchange',        '', '').obj_get(dict))
    row_lis.append(ImsExchange('NASDAQ',   'Nasdaq Stock Market',            '', '').obj_get(dict))
    row_lis.append(ImsExchange('TSE',      'Tokyo Stock Exchange',           '', '').obj_get(dict))
    row_lis.append(ImsExchange('TSX',      'Toronto Stock Exchange',         '', '').obj_get(dict))
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', row_data = row_lis, agt_log = agt_log)

    #
    # sectors
    #
    
    agt_log.title_put(text = 'ImsSectors')

    row_lis = []
    
    row_lis.append(ImsSector('BANK', '1',).obj_get(dict))

    agt_ora.agt_put(table_name = 'IMS_SECTORS', row_data = row_lis, agt_log = agt_log)

    #
    # currency types
    #
    
    agt_log.title_put(text = 'ImsCurrencyTypes')

    row_lis = []
    
    row_lis.append(ImsCurrencyType('AUD', 'Australian dollar').obj_get(dict))
    row_lis.append(ImsCurrencyType('CAD', 'Canadian dollar'  ).obj_get(dict))
    row_lis.append(ImsCurrencyType('EUR', 'Euro'             ).obj_get(dict))
    row_lis.append(ImsCurrencyType('GBP', 'British pound'    ).obj_get(dict))
    row_lis.append(ImsCurrencyType('USD', 'US dollar'        ).obj_get(dict))

    agt_ora.agt_put(table_name = 'IMS_CURRENCY_TYPES', row_data = row_lis, agt_log = agt_log)
  
    agt_log.title_put(text = 'finished creating data')
        
    return




    
# ============================================================================================================================
# main 
# ============================================================================================================================

if __name__ == '__main__':

    # create log agent
        
    file_folder = FOLDER_LOG
    file_name   = 'cre_data_ims.log'
    file_kind   = AGT_KND_LOG
  
    log_params  = (file_folder, file_name, file_kind)
    agt_log     = AvaAgtLog(key = 'agent - log', params = log_params)
    
    # create error agent
    
    file_folder = FOLDER_ERR
    file_name   = 'cre_data_ims.err'
    file_kind   = AGT_KND_ERR

    err_params  = (file_folder, file_name, file_kind)
    agt_err     = AvaAgtLog(key = 'agent - err', params = err_params)   
       
    # create oracle agent
        
    ora_params      = (DB_HOST, DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE, 'IMS', DB_PASSWORD)
    agt_ora         = AvaAgtOra(key = 'agent - ora', agt_ctl = None, agt_err = agt_err, agt_log = agt_log, params = ora_params)
   
    cre_data_ims(agt_err = agt_err, agt_log = agt_log, agt_ora = agt_ora)
    
    # tidy up
    
    agt_ora.agt_clo() # close oracle agent
    agt_log.agt_clo() # close log file
    agt_err.agt_clo() # close err file