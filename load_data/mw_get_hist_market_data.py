# ------------------------------------------------------------
# filename : mw_get_hist_mkt_data.py
# descr    : Get Historical market data from IB 
#             
# date       ver who change
# ---------- --- --- ------
# 2025-12-02 101 MW  initial write
# ------------------------------------------------------------

from   ibapi.sync_wrapper import TWSSyncWrapper
from   ibapi.contract     import Contract
import oracledb
from   datetime           import datetime
from   agents.AvaAgtOra   import AvaAgtOra
from   agents.AvaAgtLog   import AvaAgtLog

# ============================================================================================================================
# config
# ============================================================================================================================

from utils.config import DB_HOST, DB_PASSWORD, DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE, DB_USERNAME
from utils.config import AGT_KND_ERR, AGT_KND_LOG
from utils.config import FOLDER_ERR,  FOLDER_LOG

# ============================================================================================================================
# globals
# ============================================================================================================================

# ============================================================================================================================
# methods
# ============================================================================================================================

# ---------------------------------------------------------------------------------------------------------------------------
# mw_get_hist_mkt_data
# ----------------------------------------------------------------------------------------------------------------------------

def mw_get_hist_mkt_data(agt_err, agt_log, agt_ora):

    # Create the wrapper
    tws = TWSSyncWrapper(timeout=10) # 10 seconds timeout

    try:
        # Connect to TWS
        #if not tws.connect_and_start("127.0.0.1", 7497, 0): # using Trader Workstation
        if not tws.connect_and_start("127.0.0.1", 4002, 0):  # using Gateway
            print("Failed to connect to TWS")
            exit(1)
    
        print("Connected to TWS")
        
        # Get server time
        try:
            time_value = tws.get_current_time()
            print(f"Server time: {time_value}")
        except Exception as e:
            print(f"Error getting server time: {e}")

        
        # Get the tickers I want to get historic market data for
        
        ticker_list =[]
        ticker_list = get_tickers()
        
        # Set up the parameters to ask for
        #my_end_date_time="", # Empty for current time
        my_end_date_time="20251130 23:59:59 UTC" # Hardcoded to test format
        #my_duration_str="1 D" # goes back 1 day
        my_duration_str="1 W" # goes back 1 week
        #my_duration_str="1 M" # goes back 1 month
        #my_duration_str="1 Y" # goes back 1 year
        #my_bar_size_setting="1 hour"
        my_bar_size_setting="1 MIN"
        #my_what_to_show="TRADES"
        my_what_to_show="BID"
        
    
        for this_ticker in ticker_list:
            print("this_ticker", this_ticker)
            
            # Define a contract (e.g., AAPL stock)
            contract = Contract()
            contract.symbol = this_ticker
            contract.secType = "STK"
            contract.currency = "CAD"
            contract.exchange = "TSE"
        
            # Get contract details
            try:
                details = ''
                details = tws.get_contract_details(contract)
                #print(f"Contract details: {details[0].longName if details else 'No details'}")
                print(f"Contract details: {this_ticker if details else 'No details'}")
            except Exception as e:
                print(f"Error getting contract details: {e}")
                
            # Get historical data
            
            print("Historical Data for this_ticker",    this_ticker,
                  "end date time",                      my_end_date_time, 
                  "duration_str",                       my_duration_str, 
                  "bar_size_setting ",                  my_bar_size_setting,
                  "what_to_show ",                      my_what_to_show)
            
            try:
                bars = tws.get_historical_data(
                                contract=               contract,
                                end_date_time=          my_end_date_time, 
                                duration_str=           my_duration_str, 
                                bar_size_setting=       my_bar_size_setting,
                                what_to_show=           my_what_to_show,
                                use_rth=                True
                )
                print(f"Historical data: {len(bars)} bars")
                for bar in bars[:3]: # Print first 3 bars
                    
                    status = save_hist_market_data(  agt_err                 = agt_err,
                                                     agt_log                 = agt_log, 
                                                     agt_ora                 = agt_ora, 
                                                     HMD_FREQ_TYPE           = my_bar_size_setting,
                                                     HMD_INV_TICKER          = this_ticker,
                                                     HMD_START_DATETIME      = bar.date, 
                                                     HMD_END_DATETIME        = bar.date, 
                                                     HMD_START_BID_PRICE     = bar.open, 
                                                     HMD_HIGHEST_BID_PRICE   = bar.high, 
                                                     HMD_LOWEST_BID_PRICE    = bar.low, 
                                                     HMD_LAST_BID_PRICE      = bar.close, 
                                                     HMD_TOTAL_TRADED_VOLUME = bar.volume)
                    
            except Exception as e:
                print(f"Error getting historical data: {e}")
            
    
    
    finally:
        # Disconnect
        print("Disconnecting...")
        tws.disconnect_and_stop()
        print("Disconnected")
        
   
    agt_ora.agt_clo()
    agt_log.agt_clo()
    agt_err.agt_clo() 


    return 

# ------------------------------------------------------------------------------------------------------------------------
# function : get_tickers
# descr    : get the tickers that want historic market data for
# 
# in         : ()
# out        : (ticker_list)
#------------------------------------------------------------------------------------------------------------------------

def get_tickers():
    
    ticker_list = ['RY', 'TD', 'BNS', 'CM', 'BMO']

    return ticker_list
    
# ------------------------------------------------------------------------------------------------------------------------
# function : save_hist_market_data
# descr    : save the historic market data to the database
# 
# in         : (connection, bar_date, bar_open, bar_high, bar_low, bar_close, bar_volume)
# out        : (status)
#------------------------------------------------------------------------------------------------------------------------

def save_hist_market_data(agt_err, agt_log, agt_ora, HMD_FREQ_TYPE, HMD_INV_TICKER, HMD_START_DATETIME, HMD_END_DATETIME, HMD_START_BID_PRICE, HMD_HIGHEST_BID_PRICE, HMD_LOWEST_BID_PRICE, HMD_LAST_BID_PRICE, HMD_TOTAL_TRADED_VOLUME):
    
    status = ''
    print(f" {HMD_START_DATETIME}: O={HMD_START_BID_PRICE}, H={HMD_HIGHEST_BID_PRICE}, L={HMD_LOWEST_BID_PRICE}, C={HMD_LAST_BID_PRICE}, V={HMD_TOTAL_TRADED_VOLUME}")
  
    
    
    # Example single row of data
    '''
    params = {
        "HMD_INV_TICKER"          : HMD_INV_TICKER,
        "HMD_START_DATETIME"      : HMD_START_DATETIME,
        "HMD_END_DATETIME"        : HMD_END_DATETIME,
        "HMD_FREQ_TYPE"           : "DAILY",
        "HMD_START_BID_PRICE"     : HMD_START_BID_PRICE,
        "HMD_HIGHEST_BID_PRICE"   : HMD_HIGHEST_BID_PRICE,
        "HMD_LOWEST_BID_PRICE"    : HMD_LOWEST_BID_PRICE,
        "HMD_LAST_BID_PRICE"      : HMD_LAST_BID_PRICE,
        "HMD_START_ASK_PRICE"     : 1,
        "HMD_HIGHEST_ASK_PRICE"   : 1,
        "HMD_LOWEST_ASK_PRICE"    : 1,
        "HMD_LAST_ASK_PRICE"      : 1,
        "HMD_FIRST_TRADED_PRICE"  : 1,
        "HMD_HIGHEST_TRADED_PRICE": 1,
        "HMD_LOWEST_TRADED_PRICE" : 1,
        "HMD_LAST_TRADED_PRICE"   : 1,
        "HMD_TOTAL_TRADED_VOLUME" : HMD_TOTAL_TRADED_VOLUME
    }
    
    agt_ora.agt_put(table_name = 'IMS_HIST_MKT_DATA', data_dict = params) 
    '''
    
    params = {
    "HMD_INV_TICKER": HMD_INV_TICKER,
    "HMD_START_DATETIME": datetime(2025, 12, 1, 9, 30),
    "HMD_END_DATETIME": datetime(2025, 12, 1, 16, 0),
    "HMD_FREQ_TYPE": "DAILY",
    "HMD_START_BID_PRICE": 180.50,
    "HMD_HIGHEST_BID_PRICE": 185.00,
    "HMD_LOWEST_BID_PRICE": 179.00,
    "HMD_LAST_BID_PRICE": 182.75,
    "HMD_START_ASK_PRICE": 181.00,
    "HMD_HIGHEST_ASK_PRICE": 185.50,
    "HMD_LOWEST_ASK_PRICE": 179.50,
    "HMD_LAST_ASK_PRICE": 183.00,
    "HMD_FIRST_TRADED_PRICE": 180.75,
    "HMD_HIGHEST_TRADED_PRICE": 185.25,
    "HMD_LOWEST_TRADED_PRICE": 179.25,
    "HMD_LAST_TRADED_PRICE": 182.90,
    "HMD_TOTAL_TRADED_VOLUME": 1000000
    }
    print("params",params)
    agt_ora.agt_put(table_name = 'IMS_HIST_MKT_DATA', data_dict = params) 

    return status
  
  
      
#============================================================================================================================
# main 
#============================================================================================================================

if __name__ == '__main__':
 
 # create log agent
        
    file_folder = FOLDER_LOG
    file_name   = 'mw_get_hist_mkt_data.log'
    file_kind   = AGT_KND_LOG
  
    log_params  = (file_folder, file_name, file_kind)
    agt_log     = AvaAgtLog(key = 'agent - log', params = log_params)
    
    agt_log.title_put(text = 'starting creating ava_agents')
        
    # create error agent
    
    file_folder = FOLDER_ERR
    file_name   = 'mw_get_hist_mkt_data.err'
    file_kind   = AGT_KND_ERR

    err_params  = (file_folder, file_name, file_kind)
    agt_err     = AvaAgtLog(key = 'agent - err', params = err_params)   
       
    # create oracle agent
        
    ora_params      = ('L02.local', DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE, "IMS", 'l02focus')
    agt_ora         = AvaAgtOra(key = 'agent - ora', agt_ctl = None, agt_err = agt_err, agt_log = agt_log, params = ora_params)
   
    status = mw_get_hist_mkt_data(agt_err = agt_err, 
                                  agt_log = agt_log, 
                                  agt_ora = agt_ora)
    
    