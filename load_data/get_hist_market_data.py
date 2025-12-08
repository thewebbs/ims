# ------------------------------------------------------------
# filename : get_hist_mkt_data.py
# descr    : Get Historical market data from IB 
#             
# date       ver who change
# ---------- --- --- ------
# 2025-12-02 101 MW  initial write
# 2025-12-07 102 MW  changing to use ImsHistMktData and to first
#                    run for BID then for ASK saving in a list
# ------------------------------------------------------------

from   ibapi.sync_wrapper import TWSSyncWrapper
from   ibapi.contract     import Contract
import oracledb
from   datetime           import datetime
from   zoneinfo import ZoneInfo

from   agents.AvaAgtOra   import AvaAgtOra
from   agents.AvaAgtLog   import AvaAgtLog
from   objects.ImsHistMktData import ImsHistMktData

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

def get_hist_mkt_data(agt_err, agt_log, agt_ora):

    # Create the wrapper
    tws = TWSSyncWrapper(timeout=10) # 10 seconds timeout

    try:
        # Connect to TWS
        #if not tws.connect_and_start("127.0.0.1", 7497, 0): # using Trader Workstation
        if not tws.connect_and_start("127.0.0.1", 4002, 0):  # using Gateway
            agt_err.title_put(text = 'Failed to connect to TWS')
            print("Failed to connect to TWS")
            exit(1)
    
        agt_log.title_put(text = 'Connected to TWS')
        print("Connected to TWS")
        
        # Get server time
        try:
            time_value = tws.get_current_time()
            print(f"Server time: {time_value}")
        except Exception as e:
            agt_err.title_put(text = 'Error getting server time')
            agt_err.title_put(text = e)
            print(f"Error getting server time: {e}")

        
        # Get the tickers I want to get historic market data for
        
        ticker_list =[]
        ticker_list = get_tickers()
        
        # Set up the parameters to ask for
        #my_end_date_time="", # Empty for current time
        my_end_date_time="20251130 23:59:59 UTC" # Hardcoded to test format
        my_duration_str="1 D" # goes back 1 day
        #my_duration_str="1 W" # goes back 1 week
        #my_duration_str="1 M" # goes back 1 month
        #my_duration_str="1 Y" # goes back 1 year
        #my_bar_size_setting="1 hour"
        my_bar_size_setting="1 MIN"

        # set up a dict to be used for the BID and then the OFFER hist mkt data
        # which will be keyed on inv_ticker
        
        hist_mkt_data_dict = {}
        
        # go through each ticker at a time
        
        for this_ticker in ticker_list:
            agt_log.title_put(text = this_ticker)
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
                agt_err.title_put(text = 'Error getting server time')
                agt_err.title_put(text = e)
                print(f"Error getting contract details: {e}")
                
            # First get the historical BID data for this ticker
            my_what_to_show="BID"
            print("About to request Historical BID Data for this_ticker",    this_ticker,
                  "end date time",                      my_end_date_time, 
                  "duration_str",                       my_duration_str, 
                  "bar_size_setting ",                  my_bar_size_setting,
                  "what_to_show ",                      my_what_to_show)
            
            try:
                bid_bars = tws.get_historical_data(
                                contract=               contract,
                                end_date_time=          my_end_date_time, 
                                duration_str=           my_duration_str, 
                                bar_size_setting=       my_bar_size_setting,
                                what_to_show=           my_what_to_show,
                                use_rth=                True
                )
                agt_log.title_put(text = 'Requesting bid data')
                print(f"Historical BID data: {len(bid_bars)} bid_bars")
                for bid_bar in bid_bars[:3]: # Print first 3 bars
                    
                    date_str = bid_bar.date
                    # convert the date string to the correct format
                    # Split into datetime part and timezone part
                    dt_part, tz_part = date_str.rsplit(" ", 1)

                    # Parse the datetime portion
                    dt_obj = datetime.strptime(dt_part, "%Y%m%d %H:%M:%S")

                    # Attach timezone
                    dt_obj = dt_obj.replace(tzinfo=ZoneInfo(tz_part))
                    print(dt_obj)

                    # save the BID data as an object before adding it in a Dict for the
                    # key of inv_ticker, inv_start_datetime, freq_type
                    
                    new_hist_mkt_data = ImsHistMktData(hmd_inv_ticker           = this_ticker, 
                                                       hmd_start_datetime       = dt_obj, 
                                                       hmd_end_datetime         = dt_obj, 
                                                       hmd_freq_type            = 'DAILY', 
                                                       hmd_start_bid_price      = bid_bar.open, 
                                                       hmd_highest_bid_price    = bid_bar.high, 
                                                       hmd_lowest_bid_price     = bid_bar.low, 
                                                       hmd_last_bid_price       = bid_bar.close, 
                                                       hmd_total_traded_volume  = bid_bar.volume,
                                                       hmd_start_ask_price      = 0, 
                                                       hmd_highest_ask_price    = 0, 
                                                       hmd_lowest_ask_price     = 0, 
                                                       hmd_last_ask_price       = 0, 
                                                       hmd_first_traded_price   = 0, 
                                                       hmd_highest_traded_price = 0, 
                                                       hmd_lowest_traded_price  = 0, 
                                                       hmd_last_traded_price    = 0
                                                      )
                    
                    
                    hist_mkt_data_dict[new_hist_mkt_data.hmd_inv_ticker,new_hist_mkt_data.hmd_start_datetime, new_hist_mkt_data.hmd_freq_type] = new_hist_mkt_data
            
            except Exception as e:
                agt_err.title_put(text = 'Error getting contract details')
                agt_err.title_put(text = e)
                print(f"Error getting contract details: {e}")
                        
            # Next get the historical ASK data for this ticker
            my_what_to_show="ASK"
            agt_log.title_put(text = 'Requesting ask data')
            print("About to request Historical ASK Data for this_ticker",    this_ticker,
                  "end date time",                      my_end_date_time, 
                  "duration_str",                       my_duration_str, 
                  "bar_size_setting ",                  my_bar_size_setting,
                  "what_to_show ",                      my_what_to_show)
            
            try:
                ask_bars = tws.get_historical_data(
                                contract=               contract,
                                end_date_time=          my_end_date_time, 
                                duration_str=           my_duration_str, 
                                bar_size_setting=       my_bar_size_setting,
                                what_to_show=           my_what_to_show,
                                use_rth=                True
                )
                print(f"Historical ASK data: {len(bid_bars)} ask_bars")
                for ask_bar in ask_bars[:3]: # Print first 3 bars
                    
                    date_str = ask_bar.date
                    # convert the date string to the correct format
                    # Split into datetime part and timezone part
                    dt_part, tz_part = date_str.rsplit(" ", 1)

                    # Parse the datetime portion
                    dt_obj = datetime.strptime(dt_part, "%Y%m%d %H:%M:%S")

                    # Attach timezone
                    dt_obj = dt_obj.replace(tzinfo=ZoneInfo(tz_part))
                    
                    # save the ASK data as an object before adding it in a Dict for the
                    # key of inv_ticker, inv_start_datetime, freq_type
                    
                    new_hist_mkt_data = ImsHistMktData(hmd_inv_ticker           = this_ticker, 
                                                       hmd_start_datetime       = dt_obj, 
                                                       hmd_end_datetime         = dt_obj,        
                                                       hmd_freq_type            = 'DAILY', 
                                                       hmd_start_bid_price      = 0, 
                                                       hmd_highest_bid_price    = 0, 
                                                       hmd_lowest_bid_price     = 0, 
                                                       hmd_last_bid_price       = 0, 
                                                       hmd_total_traded_volume  = 0,
                                                       hmd_start_ask_price      = ask_bar.open, 
                                                       hmd_highest_ask_price    = ask_bar.high, 
                                                       hmd_lowest_ask_price     = ask_bar.low, 
                                                       hmd_last_ask_price       = ask_bar.close, 
                                                       hmd_first_traded_price   = 0, 
                                                       hmd_highest_traded_price = 0, 
                                                       hmd_lowest_traded_price  = 0, 
                                                       hmd_last_traded_price    = 0
                                                      )
                    
                    hist_mkt_data_dict[new_hist_mkt_data.hmd_inv_ticker,new_hist_mkt_data.hmd_start_datetime, new_hist_mkt_data.hmd_freq_type] = new_hist_mkt_data
     
                    
            except Exception as e:
                agt_err.title_put(text = 'Error getting historical ask data')
                agt_err.title_put(text = e)
                print(f"Error getting historical ask data: {e}")
                
            
        # Now we have the dict full of all of the data we should save it to the database
            
        agt_log.title_put(text = 'About to save to the database')
        print("About to save to the database")
        status = save_hist_market_data(agt_err, agt_log, agt_ora, hist_mkt_data_dict)
        agt_log.title_put(text = 'After saving to the database - status')
        agt_log.title_put(text = status)
        print("Status after saving to database", status)
        
    finally:
        # Disconnect
        agt_log.title_put(text = 'Disconnecting')
        print("Disconnecting...")
        tws.disconnect_and_stop()
        agt_log.title_put(text = 'Disconnected')
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

def save_hist_market_data(agt_err, agt_log, agt_ora, hist_mkt_data_dict):
    
    status = ''
    
    # loop through the dict, pulling out one record at a time and saving it to the database
    
    for obj in hist_mkt_data_dict.values():
        print(obj)
        obj.put_db(agt_db = agt_ora)
    
    
    return status
  
  
      
#============================================================================================================================
# main 
#============================================================================================================================

if __name__ == '__main__':
 
 # create log agent
        
    file_folder = FOLDER_LOG
    file_name   = 'get_hist_mkt_data.log'
    file_kind   = AGT_KND_LOG
  
    log_params  = (file_folder, file_name, file_kind)
    agt_log     = AvaAgtLog(key = 'agent - log', params = log_params)
    
    agt_log.title_put(text = 'starting creating ava_agents')
        
    # create error agent
    
    file_folder = FOLDER_ERR
    file_name   = 'get_hist_mkt_data.err'
    file_kind   = AGT_KND_ERR

    err_params  = (file_folder, file_name, file_kind)
    agt_err     = AvaAgtLog(key = 'agent - err', params = err_params)   
       
    # create oracle agent
        
    ora_params      = ('L02.local', DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE, "IMS", 'l02focus')
    agt_ora         = AvaAgtOra(key = 'agent - ora', agt_ctl = None, agt_err = agt_err, agt_log = agt_log, params = ora_params)
   
    status = get_hist_mkt_data(agt_err = agt_err, 
                               agt_log = agt_log, 
                               agt_ora = agt_ora)
    
    