# ------------------------------------------------------------
# filename : get_hist_mkt_data.py
# descr    : Get Historical market data from IB 
#             
# date       ver who change
# ---------- --- --- ------
# 2025-12-02 101 MW  initial write
# 2025-12-07 102 MW  changing to use ImsHistMktData and to first
#                    run for BID then for ASK saving in a list
# 2025-12-13 103 MW  changing so that ASK prices don't overwrite 
#                    the BID prices - using bid_dict and ask_dict
# 2025-12-14 104 MW  using start and end date times as parameters
#                    and calculating the period from those
# ------------------------------------------------------------

from   ibapi.sync_wrapper import TWSSyncWrapper
from   ibapi.contract     import Contract
from   datetime           import datetime
from   zoneinfo           import ZoneInfo

from   agents.AvaAgtOra   import AvaAgtOra
from   agents.AvaAgtLog   import AvaAgtLog


# ============================================================================================================================
# config
# ============================================================================================================================

from utils.config import DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE
from utils.config import AGT_KIND_ERR, AGT_KIND_LOG
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

def get_hist_mkt_data(agt_err, agt_log, agt_ora, start_datetime_str, end_datetime_str):

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
            agt_err.log_put(e)
            print(f"Error getting server time: {e}")

        
        # Get the tickers I want to get historic market data for
        
        ticker_list =[]
        ticker_list = get_tickers()
        
        # Set up the parameters to ask for
        bar_size_setting="1 MIN"

        # calculate the duration_str from the start and end date times
        fmt = "%Y%m%d %H:%M:%S %Z"
        start_dt = datetime.strptime(start_datetime_str, fmt)
        end_dt = datetime.strptime(end_datetime_str, fmt)

        delta = end_dt - start_dt
        number_days = int(delta.total_seconds() / 86400)  # exact fractional days convert to integer
        
        duration_str = f"{number_days} D"
        print("duration_str:", duration_str)
        
        agt_log.log_put(f"Requesting date for duration string {duration_str}")
            
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
                agt_err.log_put(e)
                print(f"Error getting contract details: {e}")
                
            # First get the historical BID data for this ticker
            what_to_show="BID"
            
            agt_log.log_put(f"About to request Historical BID Data for : {this_ticker} {end_datetime_str} {duration_str} {bar_size_setting} {what_to_show}")
            try:
                bid_bars = tws.get_historical_data(
                                contract=               contract,
                                end_date_time=          end_datetime_str, 
                                duration_str=           duration_str, 
                                bar_size_setting=       bar_size_setting,
                                what_to_show=           what_to_show,
                                use_rth=                True
                )
                #agt_log.title_put(text = 'Requesting bid data')
                agt_log.log_put(f"BID data received: {len(bid_bars)} bid_bars")
                
                for bid_bar in bid_bars: 
                    #agt_log.log_put(bid_bar.date)
                    date_str = bid_bar.date
                    # convert the date string to the correct format
                    # Split into datetime part and timezone part
                    dt_part, tz_part = date_str.rsplit(" ", 1)

                    # Parse the datetime portion
                    dt_obj = datetime.strptime(dt_part, "%Y%m%d %H:%M:%S")

                    # Attach timezone
                    dt_obj = dt_obj.replace(tzinfo=ZoneInfo(tz_part))
                    
                    # save the BID data as a dict                   
                    bid_dict = {'hmd_inv_ticker'          : this_ticker, 
                                'hmd_start_datetime'      : dt_obj, 
                                'hmd_end_datetime'        : dt_obj, 
                                'hmd_freq_type'           : 'DAILY', 
                                'hmd_start_bid_price'     : bid_bar.open, 
                                'hmd_highest_bid_price'   : bid_bar.high, 
                                'hmd_lowest_bid_price'    : bid_bar.low, 
                                'hmd_last_bid_price'      : bid_bar.close, 
                                'hmd_total_traded_volume' : bid_bar.volume}
                    
                    # save this BID data directly to the database
                    #agt_ora.agt_put(table_name = 'IMS_HIST_MKT_DATA', row_data_lis=[bid_dict], agt_log = agt_log)
                    agt_ora.agt_put(table_name = 'IMS_HIST_MKT_DATA', row_data_lis=[bid_dict])
                    
                                
            except Exception as e:
                agt_err.title_put(text = 'Error getting server time')
                agt_err.log_put(e)
                print(f"Error getting contract details: {e}")
                        
            # Next get the historical ASK data for this ticker
            what_to_show="ASK"
            #agt_log.title_put(text = 'Requesting ask data')
            agt_log.log_put(f"About to request Historical ASK Data for : {this_ticker} {end_datetime_str} {duration_str} {bar_size_setting} {what_to_show}")
            
            try:
                ask_bars = tws.get_historical_data(
                                contract=               contract,
                                end_date_time=          end_datetime_str, 
                                duration_str=           duration_str, 
                                bar_size_setting=       bar_size_setting,
                                what_to_show=           what_to_show,
                                use_rth=                True
                )
                agt_log.log_put(f"ASK data received: {len(ask_bars)} ask_bars")
                
                for ask_bar in ask_bars: # Print all
                    #agt_log.log_put(ask_bar.date)
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
                    
                    # save the ASK data as a dict                   
                    ask_dict = {'hmd_inv_ticker'          : this_ticker, 
                                'hmd_start_datetime'      : dt_obj, 
                                'hmd_end_datetime'        : dt_obj, 
                                'hmd_freq_type'           : 'DAILY', 
                                'hmd_start_ask_price'     : ask_bar.open, 
                                'hmd_highest_ask_price'   : ask_bar.high, 
                                'hmd_lowest_ask_price'    : ask_bar.low, 
                                'hmd_last_ask_price'      : ask_bar.close}
                    
                    # save this ASK data directly to the database
                    #agt_ora.agt_put(table_name = 'IMS_HIST_MKT_DATA', row_data_lis=[ask_dict], agt_log = agt_log)
                    agt_ora.agt_put(table_name = 'IMS_HIST_MKT_DATA', row_data_lis=[ask_dict])
                    
                    
            except Exception as e:
                agt_err.title_put(text = 'Error getting historical ask data')
                agt_err.log_put(e)
                print(f"Error getting historical ask data: {e}")
                    
    
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
    

      
#============================================================================================================================
# main 
#============================================================================================================================

if __name__ == '__main__':
 
 # create log agent
        
    file_folder = FOLDER_LOG
    file_name   = 'get_hist_mkt_data.log'
    file_kind   = AGT_KIND_LOG
  
    log_params  = (file_folder, file_name, file_kind)
    agt_log     = AvaAgtLog(key = 'agent - log', params = log_params)
    
    agt_log.title_put(text = 'starting creating ava_agents')
        
    # create error agent
    
    file_folder = FOLDER_ERR
    file_name   = 'get_hist_mkt_data.err'
    file_kind   = AGT_KIND_ERR

    err_params  = (file_folder, file_name, file_kind)
    agt_err     = AvaAgtLog(key = 'agent - err', params = err_params)   
       
    # create oracle agent
        
    ora_params      = ('L02.local', DB_PORT, DB_TIMEZONE, DB_TNS_SERVICE, "IMS", 'l02focus')
    agt_ora         = AvaAgtOra(key     = 'agent - ora', 
                                agt_ctl = None, 
                                agt_err = agt_err, 
                                agt_log = agt_log, 
                                params  = ora_params)
   
    # setting up start and end date times as strings
    
    start_datetime_str ="20251120 23:59:59 UTC"
    end_datetime_str   ="20251130 23:59:59 UTC"
    
    status = get_hist_mkt_data(agt_err            = agt_err, 
                               agt_log            = agt_log, 
                               agt_ora            = agt_ora,
                               start_datetime_str = start_datetime_str, 
                               end_datetime_str   = end_datetime_str)
    
    