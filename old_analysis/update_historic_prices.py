#------------------------------------------------------------
# Filename  : update_historic_prices.py
# Project   : ava-trade
#
# Descr     : get historic prices and store in database
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2022-12-06 200 DW  Initial write 
#------------------------------------------------------------

from ibapi.client  import *
from ibapi.wrapper import *

#import asyncio 
import time

from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD, DEBUG
from utils.config import IB_API_PORT, IB_API_GATEWAY_IP

from utils.utils_database import close_db, open_db

from database.datamodel.ava_trade.db_objects.ImsHistMktDataDB    import insert_ask, insert_bid
from database.datamodel.ava_trade.db_objects.ImsLoadDoneDB       import ImsLoadDoneDB
from database.datamodel.ava_trade.db_objects.ImsLoadTodoDB       import get_bid_ask_load_todos, set_todo_status
from database.datamodel.ava_trade.db_objects.ImsProcessControlDB import get_process_control_setting

global DATABASE
global REQ_ID_DICT
global OUTSTANDING_REQUESTS
OUTSTANDING_REQUESTS = 0

class IbApi(EClient, EWrapper):
    
    def __init__(self):
        EClient.__init__(self, self)
        self.next_req_id          = 1
        
    def contractDetails(self, req_id, contract_details):
        
        global DATABASE
        
        if DEBUG:
            print(f"contract details: {contract_details}")
        
        return
        
        
    def contractDetailsEnd(self, req_id):
        print(f"contractDetailsEnd - reqId: {req_id}")
        # self.disconnect
        
        return
        
        
    def historicalData(self, req_id, bar):
        
        global REQ_ID_DICT 
        global DATABASE

        #print(f"historicalData: {req_id} : {bar}")
        
        if DEBUG:
            print(f"historicalData: {req_id} : {bar}")
     
        (hmd_inv_ticker, lto_req_type) = REQ_ID_DICT[req_id]
        hmd_start_datetime = bar.date
        
        # replace the 00 seconds with 59
        hmd_end_datetime   = hmd_start_datetime.replace(":00 ", ":59 ")
        
        hmd_freq_type = '1 min'
        
        # 
        # NB we need to remove the timezone that IB uses from the datetimes as oracle 
        # doesnt like it 
        #
        
        hmd_start_datetime = hmd_start_datetime[:17]
        hmd_end_datetime   = hmd_end_datetime[:17]
        
        if DEBUG:
            print(f" hmd_inv_ticker: {hmd_inv_ticker}")
            print(f" hmd_start_datetime: {hmd_start_datetime} ")
            print(f" hmd_end_datetime: {hmd_end_datetime}")
            print(f" hmd_freq_type: {hmd_freq_type}")
        
        if lto_req_type == 'HISTMKTDATA_BID':
            hmd_start_bid_price   = bar.open
            hmd_highest_bid_price = bar.high
            hmd_lowest_bid_price  = bar.low
            hmd_last_bid_price    = bar.close

            if DEBUG:
                print(f"hmd_start_bid_price   : {hmd_start_bid_price}")
                print(f"hmd_highest_bid_price : {hmd_highest_bid_price}")
                print(f"hmd_lowest_bid_price  : {hmd_lowest_bid_price}")
                print(f"hmd_last_bid_price    : {hmd_last_bid_price}")
            
            insert_bid(database              = DATABASE,
                       hmd_inv_ticker        = hmd_inv_ticker, 
                       hmd_start_datetime    = hmd_start_datetime, 
                       hmd_end_datetime      = hmd_end_datetime, 
                       hmd_freq_type         = hmd_freq_type, 
                       hmd_start_bid_price   = hmd_start_bid_price, 
                       hmd_highest_bid_price = hmd_highest_bid_price, 
                       hmd_lowest_bid_price  = hmd_lowest_bid_price, 
                       hmd_last_bid_price    = hmd_last_bid_price)
            
        else:
            hmd_start_ask_price   = bar.open
            hmd_highest_ask_price = bar.high
            hmd_lowest_ask_price  = bar.low
            hmd_last_ask_price    = bar.close

            if DEBUG:
                print(f"hmd_start_ask_price   : {hmd_start_ask_price}")
                print(f"hmd_highest_ask_price : {hmd_highest_ask_price}")
                print(f"hmd_lowest_ask_price  : {hmd_lowest_ask_price}")
                print(f"hmd_last_ask_price    : {hmd_last_ask_price}")

            insert_ask(database              = DATABASE, 
                       hmd_inv_ticker        = hmd_inv_ticker, 
                       hmd_start_datetime    = hmd_start_datetime, 
                       hmd_end_datetime      = hmd_end_datetime, 
                       hmd_freq_type         = hmd_freq_type, 
                       hmd_start_ask_price   = hmd_start_ask_price, 
                       hmd_highest_ask_price = hmd_highest_ask_price, 
                       hmd_lowest_ask_price  = hmd_lowest_ask_price, 
                       hmd_last_ask_price    = hmd_last_ask_price)
          
        return
        
        
    def historicalDataEnd(self, req_id, start, end):
        
        global OUTSTANDING_REQUESTS
 
        print(f"historicalDataEnd - reqId: {req_id} start: {start}, end: {end}")
        
        OUTSTANDING_REQUESTS = OUTSTANDING_REQUESTS - 1
        
        print(f"historicalDataEnd - OUTSTANDING_REQUESTS: {OUTSTANDING_REQUESTS}")
        
        if OUTSTANDING_REQUESTS == 0:
            print("about to sleep")
            time.sleep(120)
            print("call get more load todos")
            reqs_do()
        
        return
        
        
    def get_next_req_id(self):
        next_req_id       = self.next_req_id
        self.next_req_id += 1
        
        return next_req_id
    
    
    def nextValidId(self, reqId: int):
       
        print(f"IN nextValidId reqId {reqId}")
        reqs_do()
        
        return
    
        
def reqs_do():
    
    global IB_API_APP
    global DATABASE
    global REQ_ID_DICT 
    global OUTSTANDING_REQUESTS
    
    #
    # find any load todos that are BID or ASK
    #
 
    load_todos = get_bid_ask_load_todos(database       = DATABASE, 
                                        lto_inv_ticker = '%', 
                                        lto_freq_type  = '%') 

    print(f"reqs_do load_todos {load_todos}")
   
    for todo in load_todos:
        (lto_inv_ticker, lto_freq_type, lto_req_type, lto_start_datetime, lto_end_datetime) = todo
        
        print(todo)
            
        if DEBUG:
            print(todo)
            print(lto_inv_ticker, lto_freq_type, lto_req_type, lto_start_datetime, lto_end_datetime)
        
        #
        # create the contract 
        #
        
        the_contract = get_tsx_contract(inv_ticker = lto_inv_ticker)
        
        #
        # get the next request id
        #
        
        req_id = IB_API_APP.get_next_req_id()
        
        #
        # record the request in the tracking dict
        #
        
        REQ_ID_DICT[req_id] = (lto_inv_ticker, lto_req_type)
        
        print("----------------")
        print(REQ_ID_DICT)
        print("----------------")
        
        #
        # update status to "WIP"
        #
        
        # 
        # NB we need to remove the timezone that IB uses from the datetimes as oracle 
        # doesnt like it 
        #
    
        ora_start_datetime = lto_start_datetime[:17]
        
        set_todo_status(database           = DATABASE, 
                        lto_inv_ticker     = lto_inv_ticker, 
                        lto_freq_type      = lto_freq_type, 
                        lto_req_type       = lto_req_type, 
                        lto_start_datetime = ora_start_datetime, 
                        old_lto_status     = 'RDY', 
                        new_lto_status     = 'WIP')
        
        #
        # submit request for historic data
        #
        
        if lto_req_type == "HISTMKTDATA_BID":
            request_type = 'BID'
        else:
            request_type = 'ASK'
            
        OUTSTANDING_REQUESTS = OUTSTANDING_REQUESTS + 1
        
        print(f"reqs_do - OUTSTANDING_REQUESTS: {OUTSTANDING_REQUESTS}")


        # NB THIS MEANS THAT THE LTO_END_DATETIME SHOULD BE START TIME - 59 SECONDS 
        
        IB_API_APP.reqHistoricalData(req_id, the_contract, lto_end_datetime, "1 D", "1 min", request_type, 1, 1, 0, [])
        
    return
    

def get_tsx_contract(inv_ticker):
    
        contract                 = Contract()
        contract.symbol          = inv_ticker[:-3]    # remove the ".TO"
        contract.secType         = "STK"
        contract.currency        = "CAD"
        contract.exchange        = "SMART"
        contract.primaryExchange = "TSE"
        
        return contract
        
        
def update_historic_prices(database):
   
    global DATABASE
    DATABASE = database
    
    global REQ_ID_DICT 
    REQ_ID_DICT = {}
    
    global IB_API_APP
    
    
    #
    # connect to IB
    #
    
    IB_API_APP = IbApi()
    client_id = 2023
    
    IB_API_APP.connect(IB_API_GATEWAY_IP, IB_API_PORT, client_id)
    
    #
    # Pause to allow time for socket to be opened
    #
    
    time.sleep(5)
    
    '''
    pcn_status_setting = "FIRSTRUN"
    
    while pcn_status_setting != "STOP":
    
        #
        # find any load todos that are BID or ASK
        #
     
        load_todos = get_bid_ask_load_todos(database       = database, 
                                            lto_inv_ticker = '%', 
                                            lto_freq_type  = '%') 
       
        
               
        for todo in load_todos:
            (lto_inv_ticker, lto_freq_type, lto_req_type, lto_start_datetime, lto_end_datetime) = todo
            
            if DEBUG:
                print(todo)
                print(lto_inv_ticker, lto_freq_type, lto_req_type, lto_start_datetime, lto_end_datetime)
            
            #
            # create the contract 
            #
            
            the_contract = get_tsx_contract(inv_ticker = lto_inv_ticker)
            
            #
            # get the next request id
            #
            
            req_id = IB_API_APP.get_next_req_id()
        
            #
            # record the request in the tracking dict
            #
            
            REQ_ID_DICT[req_id] = (lto_inv_ticker, lto_req_type)
            
            #
            # update status to "WIP"
            #
            
            # 
            # NB we need to remove the timezone that IB uses from the datetimes as oracle 
            # doesnt like it 
            #
        
            ora_start_datetime = lto_start_datetime[:17]
            
            set_todo_status(database           = database, 
                            lto_inv_ticker     = lto_inv_ticker, 
                            lto_freq_type      = lto_freq_type, 
                            lto_req_type       = lto_req_type, 
                            lto_start_datetime = ora_start_datetime, 
                            old_lto_status     = 'RDY', 
                            new_lto_status     = 'WIP')
            
            #
            # submit request for historic data
            #
            
            if lto_req_type == "HISTMKTDATA_BID":
                request_type = 'BID'
            else:
                request_type = 'ASK'
                
                
            # NB THIS THAT THE LTO_END_DATETIME SHOULD BE START TIME - 59 SECONDS 
            
            IB_API_APP.reqHistoricalData(req_id, the_contract, lto_end_datetime, "1 D", "1 min", request_type, 1, 1, 0, [])
     
        print("----------------")
        print(REQ_ID_DICT)
        print("----------------")
        
        
        
        #
        # now see if the process control status is set to stop
        #
        
        pcn_status_setting = get_process_control_setting(database         = database,
                                                         pcn_process_name = "UPDATE_HISTORIC_PRICES",
                                                         pcn_server_name  = DB_HOST)
        #
        # pause for a bit
        #
        
        time.sleep(60)
  
    if pcn_status_setting == "FIRSTRUN":
        ib_api_app.run()
        pcn_status_setting = "RUN"
    '''
    
    IB_API_APP.run()
    
    return
    
    
if __name__ == "__main__":
    
    #
    # connect to database
    #
    
    print("Open db")
    print(" ")
    
    database = open_db(host=DB_HOST, port=DB_PORT, tns_service=DB_TNS_SERVICE, user_name=DB_USER_NAME, password=DB_PASSWORD)

    global DATABASE 
   
    DATABASE = database
    update_historic_prices(database = DATABASE)
    