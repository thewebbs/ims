# ------------------------------------------------------------
# filename  : create_data_ims.py
# descr     : test general purpose create_data_ims 
#
# in       : ()
# out      : ()
#
# date       ver who change
# ---------- --- --- ------
# 2025-12-02 101 DW  initial write
# ------------------------------------------------------------

from agents.AvaAgtOra import AvaAgtOra
from agents.AvaAgtLog import AvaAgtLog
from datetime import datetime

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
# create_data_ims_test
# ----------------------------------------------------------------------------------------------------------------------------

def create_data_ims_test(agt_err, agt_log, agt_ora):
   
   
    # IMS_EXCHANGES
    
    data_dict = {
                "EXC_SYMBOL": "NYSE",
                "EXC_NAME": "New York Stock Exchange"
                }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict = {
                "EXC_SYMBOL": "NASDAQ",
                "EXC_NAME": "Nasdaq Stock Market"
                }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =             {
                "EXC_SYMBOL": "TSE",
                "EXC_NAME": "Tokyo Stock Exchange"
                }
                
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =             {
                "EXC_SYMBOL": "SSE",
                "EXC_NAME": "Shanghai Stock Exchange"
                }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =             {
                "EXC_SYMBOL": "HKEX",
                "EXC_NAME": "Hong Kong Stock Exchange"
                }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =             {
                "EXC_SYMBOL": "LSE",
                "EXC_NAME": "London Stock Exchange"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =          {
                "EXC_SYMBOL": "SZSE",
                "EXC_NAME": "Shenzhen Stock Exchange"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "EURONEXT",
                "EXC_NAME": "Euronext"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "NSE",
                "EXC_NAME": "National Stock Exchange of India"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "BSE",
                "EXC_NAME": "Bombay Stock Exchange"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "TSX",
                "EXC_NAME": "Toronto Stock Exchange"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "ASX",
                "EXC_NAME": "Australian Securities Exchange"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "B3",
                "EXC_NAME": "Brasil Bolsa Balcão"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "JSE",
                "EXC_NAME": "Johannesburg Stock Exchange"
              }
    
    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)   
             
    data_dict =           {
                "EXC_SYMBOL": "FWB",
                "EXC_NAME": "Frankfurt Stock Exchange (Deutsche Börse)"
              }

    agt_ora.agt_put(table_name = 'IMS_EXCHANGES', data_dict = data_dict)
        
        
    # IMS_SECTORS
    
    data_dict = {
                "SEC_NAME"     : "BANK",
                "SEC_REP_ORDER": "1"
                }
    
    agt_ora.agt_put(table_name = 'IMS_SECTORS', data_dict = data_dict)   
    
    # IMS_CURRENCY_TYPES
    
    data_dict = {
                "CTY_SYMBOL"  : "CAD",
                "CTY_NAME"    : "Canadian Dollar"
                }
    
    agt_ora.agt_put(table_name = 'IMS_CURRENCY_TYPES', data_dict = data_dict)   
    
    
    # IMS_INVESTMENTS
    
    data_dict = {
                "INV_TICKER"    : "RY",
                "INV_EXC_SYMBOL": "TSX",
                "INV_NAME"      : "Royal Bank of Canada",
                "INV_SEC_NAME"  : "BANK",
                "INV_CTY_SYMBOL": "CAD",
                "INV_LATEST_PRICE_DATETIME": datetime(2025, 12, 1, 9, 30)
                }
    
    agt_ora.agt_put(table_name = 'IMS_INVESTMENTS', data_dict = data_dict)   
    
    data_dict = {
                "INV_TICKER"    : "TD",
                "INV_EXC_SYMBOL": "TSX",
                "INV_NAME"      : "Toronto-Dominion Bank",
                "INV_SEC_NAME"  : "BANK",
                "INV_CTY_SYMBOL": "CAD",
                "INV_LATEST_PRICE_DATETIME": datetime(2025, 12, 1, 9, 30)
                }
    
    agt_ora.agt_put(table_name = 'IMS_INVESTMENTS', data_dict = data_dict)   
    
    data_dict = {
                "INV_TICKER"    : "BNS",
                "INV_EXC_SYMBOL": "TSX",
                "INV_NAME"      : "Bank of Nova Scotia (Scotiabank)",
                "INV_SEC_NAME"  : "BANK",
                "INV_CTY_SYMBOL": "CAD",
                "INV_LATEST_PRICE_DATETIME": datetime(2025, 12, 1, 9, 30)
                }
    
    agt_ora.agt_put(table_name = 'IMS_INVESTMENTS', data_dict = data_dict)   
    
    data_dict = {
                "INV_TICKER"    : "BMO",
                "INV_EXC_SYMBOL": "TSX",
                "INV_NAME"      : "Bank of Montreal",
                "INV_SEC_NAME"  : "BANK",
                "INV_CTY_SYMBOL": "CAD",
                "INV_LATEST_PRICE_DATETIME": datetime(2025, 12, 1, 9, 30)
                }
    
    agt_ora.agt_put(table_name = 'IMS_INVESTMENTS', data_dict = data_dict)   
    
    data_dict = {
                "INV_TICKER"    : "CM",
                "INV_EXC_SYMBOL": "TSX",
                "INV_NAME"      : "Canadian Imperial Bank of Commerce",
                "INV_SEC_NAME"  : "BANK",
                "INV_CTY_SYMBOL": "CAD",
                "INV_LATEST_PRICE_DATETIME": datetime(2025, 12, 1, 9, 30)
                }
    
    agt_ora.agt_put(table_name = 'IMS_INVESTMENTS', data_dict = data_dict)   
    
    agt_ora.agt_clo()
    agt_log.agt_clo()
    agt_err.agt_clo() 
    
    return




    
#============================================================================================================================
# main 
#============================================================================================================================

if __name__ == '__main__':

    # create log agent
        
    file_folder = FOLDER_LOG
    file_name   = 'create_data_ims.log'
    file_kind   = AGT_KND_LOG
  
    log_params  = (file_folder, file_name, file_kind)
    agt_log     = AvaAgtLog(key = 'agent - log', params = log_params)
    
    agt_log.title_put(text = 'starting creating ava_agents')
        
    # create error agent
    
    file_folder = FOLDER_ERR
    file_name   = 'create_data_ims.err'
    file_kind   = AGT_KND_ERR

    err_params  = (file_folder, file_name, file_kind)
    agt_err     = AvaAgtLog(key = 'agent - err', params = err_params)   
       
    # create oracle agent
        
    ora_params      = (DB_HOST, DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE, "IMS", DB_PASSWORD)
    agt_ora         = AvaAgtOra(key = 'agent - ora', agt_ctl = None, agt_err = agt_err, agt_log = agt_log, params = ora_params)
   
    
    create_data_ims_test(agt_err = agt_err, agt_log = agt_log, agt_ora = agt_ora)
    
    