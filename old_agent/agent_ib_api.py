#------------------------------------------------------------
# Filename  : agent_get_ib_api.py
# Project   : ava
#
# Descr     : This holds routines relating to the IB API 
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-08-21   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

import calendar

from utils.config import DEBUG, IB_API_PORT, IB_API_GATEWAY_IP

import datetime
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract
from apis.ib_api.OrderSamples import OrderSamples
from ava_agents.agent_trade import recheck_holding_status, update_holding, update_prices
from ibapi.wrapper import EWrapper
import numpy as np
import pandas as pd
import queue
from threading import Thread
import time



NUM_OPTION_REQUESTS=0

IB_TRAN_TYPE="MKT"  # Will hold whether this is being used to collect options data or straight mkt data

NEXTREQID = 0 # Will hold the next request id we should use

DEFAULT_MARKET_DATA_ID=60
DEFAULT_HISTORIC_DATA_ID=50
DEFAULT_GET_CONTRACT_ID=43
DATE_FORMAT = "%Y-%m-%d"


## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()




def get_next_reqid():
    
    global NEXTREQID
    
    NEXTREQID += 1
    
    return NEXTREQID

    
def get_IB_stockprice_data(app, inv_ticker, inv_exc_symbol, tradeagent):

    # set the global to show whether collecting options type market data or straight market data
    # here we want it for straight market data
    global IB_TRAN_TYPE
    
    IB_TRAN_TYPE = 'STK'
    
    #print('In get_IB_stockprice_data for',inv_ticker, inv_exc_symbol)
    
    
    # set up ib contract ready to validate it
    # note that we hard code the option type to C for validation purposes
    
    ibcontract = IBcontract()
    ibcontract.symbol = inv_ticker  
    ibcontract.secType = "STK" 
    
    if inv_exc_symbol == 'TSE':
        ibcontract.currency="CAD"
        ibcontract.exchange="SMART" 
        ibcontract.primaryExchange="TSE" 
        
    elif inv_exc_symbol == 'NYSE':
        ibcontract.currency="USD"
        ibcontract.exchange="SMART" 
           
    thisreqId = get_next_reqid() 
    
    resolved_ibcontract=app.resolve_ib_contract(ibcontract=ibcontract, reqId=thisreqId)
        
    thisreqId = get_next_reqid()  # to make it unique
    if resolved_ibcontract == ibcontract:
        print('ERROR - not resolved so not continuing')
        print('**************************************')
       
    else:
        if DEBUG:
            print("Resolved Contract: " + str(resolved_ibcontract))
        
        thisreqId = get_next_reqid() # to make it unique
        tickerid = app.start_getting_stock_prices(inv_ticker=inv_ticker, 
                                                  inv_exc_symbol=inv_exc_symbol, 
                                                  ibcontract=ibcontract,
                                                  this_tradeagent=tradeagent)
        
        if DEBUG:
            print('tickerid',tickerid)
            print('======================================================')
            print('Finished processing for this get_IB_stockprice_data call')
            print('======================================================')
        
    return 




def notify_results(notify_type, inv_ticker, inv_exc_symbol, data_type, data_datetime, data_price):
    
    ticker_and_symbol = "%s.%s" % (inv_ticker, inv_exc_symbol)
    if notify_type == 'PRINT':
        print('New data received ',ticker_and_symbol,'datetime',data_datetime,data_type,data_price)
                    
                    
    return 


def setup_environment():
    
    global NEXTREQID
    
    # instantiate the API app
    
    app = TestApp(IB_API_GATEWAY_IP, IB_API_PORT, 1)
    
    # set up the next request id using the calendar
    NEXTREQID = calendar.timegm(time.gmtime()) 
    
    return app


def cleanup_environment(app):
    
    app.disconnect()

    print(" ")
    print("After disconnecting")
    print(" ")
    
    return 


class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we have
                finished = True
                self.status = TIME_OUT


        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


def _nan_or_int(x):
    if not np.isnan(x):
        return int(x)
    else:
        return x


class stream_of_ticks(list):
    """
    Stream of ticks
    """

    def __init__(self, list_of_ticks):
        super().__init__(list_of_ticks)

    def as_pdDataFrame(self):
        if len(self)==0:
            ## no data; do a blank tick
            return tick(datetime.datetime.now()).as_pandas_row()

        pd_row_list=[tick.as_pandas_row() for tick in self]
        pd_data_frame=pd.concat(pd_row_list)

        return pd_data_frame


class tick(object):
    """
    Convenience method for storing ticks
    Not IB specific, use as abstract
    """
    def __init__(self, timestamp, bid_size=np.nan, bid_price=np.nan,
                 ask_size=np.nan, ask_price=np.nan,
                 last_trade_size=np.nan, last_trade_price=np.nan,
                 ignorable_tick_id=None):

        ## ignorable_tick_id keyword must match what is used in the IBtick class
        self.timestamp=timestamp
        self.bid_size=_nan_or_int(bid_size)
        self.bid_price=bid_price
        self.ask_size=_nan_or_int(ask_size)
        self.ask_price=ask_price
        self.last_trade_size=_nan_or_int(last_trade_size)
        self.last_trade_price=last_trade_price


    def __repr__(self):
        return self.as_pandas_row().__repr__()


    def as_pandas_row(self):
        """
        Tick as a pandas dataframe, single row, so we can concat together
        :return: pd.DataFrame
        """

        attributes=['bid_size','bid_price', 'ask_size', 'ask_price',
                    'last_trade_size', 'last_trade_price']

        self_as_dict=dict([(attr_name, getattr(self, attr_name)) for attr_name in attributes])

        return pd.DataFrame(self_as_dict, index=[self.timestamp])


class IBtick(tick):
    """
    Resolve IB tick categories
    """

    def __init__(self, timestamp, tickid, value):

        resolve_tickid=self.resolve_tickids(tickid)
        super().__init__(timestamp, **dict([(resolve_tickid, value)]))

    def resolve_tickids(self, tickid):
        tickid_dict=dict([("0", "bid_size"), ("1", "bid_price"), ("2", "ask_price"), ("3", "ask_size"),
                          ("4", "last_trade_price"), ("5", "last_trade_size")])

        if str(tickid) in tickid_dict.keys():
            return tickid_dict[str(tickid)]
        else:
            # This must be the same as the argument name in the parent class
            return "ignorable_tick_id"


## ====================== WRAPPER =================================


class TestWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens, like currentTime
    Extra methods are added as we need to store the results in this object
    """

    def __init__(self):
        self._my_contract_details = {}
        self._my_market_data_dict = {}
        self._my_stock_price_tracker = {}
        self._my_order_tracker = {}
        
        
    ## error handling code
    def init_error(self):
        error_queue=queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        #self._my_errors.put(errormsg)
        
        if not (errorCode == 2106) and not(errorCode == 2104) and not(errorCode == 366) and not(errorCode == 200) and not(errorCode == 2158):
            print('*** ERROR ', errormsg)

    ## ====================== CONTRACT DETAILS =================================
    
    ## get contract details code
    def init_contractDetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        ## overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractDetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        ## overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractDetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)


        ## ====================== MARKET_DATA =================================
    
        # market data
    def init_market_data(self, tickerid):
        market_data_queue = self._my_market_data_dict[tickerid] = queue.Queue()

        return market_data_queue

    def get_time_stamp(self):
        ## Time stamp to apply to market data
        ## We could also use IB server time
        return datetime.datetime.now()


    def tickPrice(self, tickerid , tickType, price, attrib):
        ##overriden method
        
        global IB_TRAN_TYPE
        
        # Note that what we do with the incoming data depends on whether we're collecting
        # options type market data or straight market data
        
        this_tick_data=IBtick(self.get_time_stamp(), tickType, price)
        self._my_market_data_dict[tickerid].put(this_tick_data)
        
        this_datetime = self.get_time_stamp()
        
                            
        # Handle the data for straight forwards stockprice market data
        if IB_TRAN_TYPE == 'STK':
            
            if (tickType == 1 or tickType == 2):
                # get the stockprice tracker info ready to create a database record
                stockprice_info = self.getstockpriceTrackerInfo(tickerid)
                stockprice_info_data = stockprice_info.queue[0]
                
                # format the datetime into one usable for sending to the database
                stockprice_datetime = this_datetime.strftime("%Y-%m-%d %H:%M:%S")
                
                #print('stockprice_info_data',stockprice_info_data)
                #print(' ')
                #print(' ')
                #print('stockprice_datetime',stockprice_datetime)
        
                frameid = stockprice_info_data["frameid"]
                inv_ticker = stockprice_info_data["inv_ticker"]
                inv_exc_symbol = stockprice_info_data["inv_exc_symbol"]
                datetime_last_bid_write_to_frame = stockprice_info_data["datetime_last_bid_write_to_frame"]
                datetime_last_ask_write_to_frame = stockprice_info_data["datetime_last_ask_write_to_frame"]
                tradeagent=stockprice_info_data["tradeagent"]
                
                current_datetime = datetime.datetime.now()
                
               
                if datetime_last_bid_write_to_frame != '':
                    # time since last write to the database in minutes
                    time_since_last_bid_write_to_frame = ((current_datetime - datetime_last_bid_write_to_frame).total_seconds())
                else:
                    time_since_last_bid_write_to_frame = 2
                    
                if datetime_last_ask_write_to_frame != '':
                    # time since last write to the database in minutes
                    time_since_last_ask_write_to_frame = ((current_datetime - datetime_last_ask_write_to_frame).total_seconds())
                else:
                    time_since_last_ask_write_to_frame = 2
                    
            
                # only update frame if more than a second has passed
                if (tickType == 1) and (time_since_last_bid_write_to_frame > 1):
                    notify_results(notify_type = 'PRINT', 
                                   inv_ticker     = inv_ticker, 
                                   inv_exc_symbol = inv_exc_symbol, 
                                   data_type      = 'BID', 
                                   data_datetime  = this_datetime, 
                                   data_price     = price)
                    
                    # update stockprice tracker
                    stockprice_info_data["datetime_last_bid_write_to_frame"] = datetime.datetime.now()
                    self._my_stock_price_tracker[tickerid].put(stockprice_info_data)

                    # update the latest price
                    update_prices(tradeagent, 'BID', price)
                    
                    # check if we need to change our holding
                    next_step = recheck_holding_status(tradeagent)
                    
                    if next_step != 'NO_CHANGE':
                        print('Next step for',inv_ticker,'is',next_step)
                        
                        # place an order
                        order = self.place_IB_order(inv_ticker     = inv_ticker, 
                                                    inv_exc_symbol = inv_exc_symbol, 
                                                    order_type     = 'LMT', 
                                                    order_action   = next_step, 
                                                    order_quantity = 100, 
                                                    order_price    = price,
                                                    tradeagent     = tradeagent)
                        
                        update_holding(tradeagent, next_step, 100, price)
                    
                    
                # only update frame if more than a second has passed
                if (tickType == 2) and (time_since_last_ask_write_to_frame > 1):
                    notify_results(notify_type = 'PRINT', 
                                   inv_ticker     = inv_ticker, 
                                   inv_exc_symbol = inv_exc_symbol, 
                                   data_type      = 'ASK', 
                                   data_datetime  = this_datetime, 
                                   data_price     = price)    
                    
                    # update stockprice tracker
                    stockprice_info_data["datetime_last_ask_write_to_frame"] = datetime.datetime.now()
                    self._my_stock_price_tracker[tickerid].put(stockprice_info_data)

                    # update the latest price
                    update_prices(tradeagent, 'ASK', price)
                    
                    # check if we need to change our holding
                    next_step = recheck_holding_status(tradeagent)
                    
                    if next_step != 'NO_CHANGE':
                        print('Next step for',inv_ticker,'is',next_step)
                        
                    update_holding(tradeagent, next_step, 100, price)
                    
                    
                    
    
    def tickSize(self, tickerid, tickType, size):
        ## overriden method
        this_tick_data=IBtick(self.get_time_stamp(), tickType, size)
        self._my_market_data_dict[tickerid].put(this_tick_data)


    def tickString(self, tickerid, tickType, value):
        ## overriden method
        #print('in tickstring',tickerid, tickType, value)
        ## value is a string, make it a float, and then in the parent class will be resolved to int if size
        ## added Moya 25-08-2020 because get alpha characters sometimes in value when place order
        if not(value.isalpha()):
            this_tick_data=IBtick(self.get_time_stamp(),tickType, float(value))
            self._my_market_data_dict[tickerid].put(this_tick_data)


    def tickGeneric(self, tickerid, tickType, value):
        ## overriden method
        this_tick_data=IBtick(self.get_time_stamp(),tickType, value)
        self._my_market_data_dict[tickerid].put(this_tick_data)

    ## ====================== stockprice TRACKING DATA ===================

    ## get contract details code
    def init_stockpriceTracker(self, tickerid):
        stockprice_tracker_queue = self._my_stock_price_tracker[tickerid] = queue.Queue()

        return stockprice_tracker_queue
    

    def savestockpriceTracker(self, tickerid, inv_ticker, inv_exc_symbol, \
                          datetime_last_bid_write_to_frame, datetime_last_ask_write_to_frame, \
                          tradeagent):

        # first the call
        this_tracking_dict = {
            "frameid":(inv_ticker + '.' + inv_exc_symbol),
            "inv_ticker":inv_ticker,
            "inv_exc_symbol":inv_exc_symbol,
            "datetime_last_bid_write_to_frame":datetime_last_bid_write_to_frame,
            "datetime_last_ask_write_to_frame":datetime_last_ask_write_to_frame,
            "tradeagent":tradeagent
            }
        
        if DEBUG:
            print('this_tracking_dict',this_tracking_dict)
        
        if tickerid not in self._my_stock_price_tracker.keys():
            self.init_stockpriceTracker(tickerid)
            
        self._my_stock_price_tracker[tickerid].put(this_tracking_dict) 
        
        return 
        
    def stockpriceTrackerEnd(self, tickerid):
        ## overriden method
        if tickerid not in self._my_stock_price_tracker.keys():
            self.init_stockpriceTracker(tickerid)

        self._my_stock_price_tracker[tickerid].put(FINISHED)
        

    def getstockpriceTrackerInfo(self, tickerid):
          
        stockprice_tracker_info = self._my_stock_price_tracker[tickerid]
        
        return stockprice_tracker_info
    
        
    ## ====================== ORDERS =================================

    
    def init_orderTracker(self, orderId):
        order_tracker_queue = self._my_order_tracker[orderId] = queue.Queue()

        return order_tracker_queue
    

    def saveorderTracker(self, orderId, inv_ticker, inv_exc_symbol, \
                          order_datetime, order_type, order_action, order_quantity,\
                          order_tif, order_price, order, permid, tradeagent, status, filled):

        print('in saveorderTracker orderId',orderId)
        # first the call
        this_tracking_dict = {
            "frameid":(inv_ticker + '.' + inv_exc_symbol),
            "inv_ticker":inv_ticker,
            "inv_exc_symbol":inv_exc_symbol,
            "order_datetime":order_datetime,
            "order_type":order_type,
            "order_action":order_action,
            "order_quantity":order_quantity,
            "order_tif":order_tif,
            "order_price":order_price,
            "order":order,
            "permid":permid,
            "tradeagent":tradeagent,
            "status":status,
            "filled":filled
            }
        
        if DEBUG:
            print('this_tracking_dict',this_tracking_dict)
        
        if orderId not in self._my_order_tracker.keys():
            self.init_orderTracker(orderId)
            
        self._my_order_tracker[orderId].put(this_tracking_dict) 
        print('_my_order_tracker',self._my_order_tracker)
        
        
    def orderTrackerEnd(self, orderId):
        ## overriden method
        if orderId not in self._my_order_tracker.keys():
            self.init_orderTracker(orderId)

        self._my_order_tracker[orderId].put(FINISHED)
        

    def getorderTrackerInfo(self, orderId):
          
        order_tracker_info = self._my_order_tracker[orderId]
        
        return order_tracker_info


    def place_IB_order(self, inv_ticker, inv_exc_symbol, order_type, order_action, order_quantity, order_price, tradeagent):

        print('In place_IB_order for',inv_ticker, inv_exc_symbol, order_type, order_action, order_quantity, order_price)
        
        ibcontract = IBcontract()
        ibcontract.symbol = inv_ticker  
        ibcontract.secType = "STK" 
        
        if inv_exc_symbol == 'TSE':
            ibcontract.currency="CAD"
            ibcontract.exchange="SMART" 
            ibcontract.primaryExchange="TSE" 
            
        elif inv_exc_symbol == 'NYSE':
            ibcontract.currency="USD"
            ibcontract.exchange="SMART" 
               
        order, order_id = self.place_order(ibcontract      = ibcontract, 
                                 order_type      = order_type, 
                                 order_action    = order_action, 
                                 order_quantity  = order_quantity, 
                                 order_price     = order_price)
        
        # now store the order in the tracking store
        timenow = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                                   
        self.saveorderTracker(orderId        = order_id,
                              inv_ticker     = inv_ticker, 
                              inv_exc_symbol = inv_exc_symbol,
                              order_datetime = timenow, 
                              order_type     = order_type, 
                              order_action   = order_action, 
                              order_quantity = order_quantity,
                              order_tif      = order.tif, 
                              order_price    = order_price, 
                              order          = order,
                              permid         = '', 
                              tradeagent     = tradeagent,
                              status         = 'NEW',
                              filled         = 0)
        
        print('order placed')    
        
        return order



    def place_order(self, ibcontract, order_type, order_action, order_quantity, order_price):
        # order_type is LMT or MKT
        # order_action is BUY or SELL
        
        self.simplePlaceOid = self.nextOrderId()
        
        
        if order_type == 'LMT':
            
            faOrderOneAccount = OrderSamples.LimitOrder(order_action, order_quantity, order_price)
            faOrderOneAccount.account = "DU2289696"
            faOrderOneAccount.tif = "GTC"
            #faOrderOneAccount.tif = "DAY"
            
            self.placeOrder(self.simplePlaceOid, ibcontract, faOrderOneAccount)
            
        elif order_type == 'MKT':
            
            faOrderOneAccount = OrderSamples.MarketOrder(order_action, order_quantity)
            faOrderOneAccount.account = "DU2289696"
            self.placeOrder(self.simplePlaceOid, ibcontract, faOrderOneAccount)
            
        return  faOrderOneAccount, self.simplePlaceOid
    

    def update_order(self, orderId, ibcontract, order, order_quantity, order_price ):
        
        print('in update_order', orderId, ibcontract, order_quantity, order_price)
                
        print("BEFORE", order)
        # get the order information we already have - must include the permid
        order_info = self.getorderTrackerInfo(orderId)
        order_info_data = order_info.queue[0]
        
        # set the permid in the order record 
        
        order.permId = order_info_data["permid"]
        order.totalQuantity = order_quantity
        if order.OrderType == 'LMT':
            order.LmtPrice = order_price
        
        print("AFTER", order)
        
                
        # then update the order_quantity and order_price which might have changed into our store

        order_datetime = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        order_info_data["order_datetime"] = order_datetime
        order_info_data["order_quantity"] = order_quantity
        order_info_data["order_price"]    = order_price
        
        print("order_info_data updated")
        
        # now place the order again with the same permid so it should do an update 
        # instead of creating a new order
        
        self.placeOrder(self.simplePlaceOid, ibcontract, order)
        
        # now store it again so we have the full information 
        self._my_order_tracker[orderId].put(order_info_data)
        

        return
     
     
    def openOrder(self, orderId, contract, order, orderState):
        print(' ')
        print('****')
        super().openOrder(orderId, contract, order, orderState)
        
        if orderState.status == 'Submitted':
            print("OpenOrder. **SUBMITTED PermId: ", order.permId, " OrderId:", orderId, 
              "Symbol:", contract.symbol, "Action:", order.action, "OrderType:", order.orderType,
              "TotalQty:", order.totalQuantity, "LmtPrice:", order.lmtPrice)
        elif orderState.status == 'Filled':
            print("OpenOrder. **FILLED PermId: ", order.permId, " OrderId:", orderId)
        else:
            print("OpenOrder some other status",orderState.status,"PermId: ", order.permId, " OrderId:", orderId) 
        '''
        print("OpenOrder. PermId: ", order.permId, "ClientId:", order.clientId, " OrderId:", orderId, 
              "Account:", order.account, "Symbol:", contract.symbol, "SecType:", contract.secType,
              "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
              "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty, 
              "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status)
        '''
        order.contract = contract
        self.permId2ord[order.permId] = order
        
        # get the order information we already have
        order_info = self.getorderTrackerInfo(orderId)
        
        
        if order_info is not None:
            
            order_info_data = order_info.queue[0]
                
            # then update the datetime and the permid and the order again just in case
    
            order_datetime = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            order_info_data["order_datetime"] = order_datetime
            order_info_data["permid"]         = order.permId
            order_info_data["order"]          = order
            
            print("order_info_data updated")
            print('****')
            print(' ')
            # now store it again so we have the full information 
            self._my_order_tracker[orderId].put(order_info_data)
        else:
            print('somehow got order info from previous session',order)
            print('****')
            print(' ')
            
        return
        
        
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        
        print(' ')
        print('****')
        if status == 'Submitted':
            print("in orderstatus **SUBMITTED Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "LastFillPrice:", lastFillPrice)
        elif status == 'Filled' and filled == 100:
            print("in orderstatus **100% FILLED Id:", orderId, "Status:", status, "AvgFillPrice:", avgFillPrice, "PermId:", permId)
        elif status == 'Filled' and filled < 100:
            print("in orderstatus **PART FILLED Id:", orderId, "Status:", status, "Remaining:", remaining,"AvgFillPrice:", avgFillPrice, "PermId:", permId)    
        else:
            print("Some other status in orderstatus Id:", orderId, "Status:", status)
        '''
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
              lastFillPrice, "ClientId:", clientId, "WhyHeld:",
              whyHeld, "MktCapPrice:", mktCapPrice)
        '''
        print('****')
        print(' ')
        
        # get the order information we already have
        order_info = self.getorderTrackerInfo(orderId)
        
        
        if order_info is not None:
            
            order_info_data = order_info.queue[0]
                
            # then update the datetime and the permid and the order again just in case
    
            order_datetime = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            order_info_data["order_datetime"] = order_datetime
            order_info_data["status"]         = status
            order_info_data["filled"]         = filled
            
            print("order_info_data updated")
            print('****')
            print(' ')
            # now store it again so we have the full information 
            self._my_order_tracker[orderId].put(order_info_data)
        else:
            print('somehow got order info from previous session',order)
            print('****')
            print(' ')

        return 
    
    
    def openOrderEnd(self):
        super().openOrderEnd()
        print("OpenOrderEnd")
        print("Received %d openOrders", len(self.permId2ord))
        return 
    
    
    def orderBound(self, orderId, apiClientId, apiOrderId):
        super().orderBound(orderId, apiClientId, apiOrderId)
        print("OrderBound.", "OrderId:", orderId, "ApiClientId:", apiClientId, "ApiOrderId:", apiOrderId)
        return
    
    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        #print("ExecDetails. ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType, "Currency:", contract.currency, execution)
        return 
    
    def commissionReport(self, commissionReport):
        super().commissionReport(commissionReport)
        #print("CommissionReport.", commissionReport)
        return
    
    
## ====================== CLIENT =================================
    

class TestClient(EClient):
    """
    The client method
    We don't override native methods, but instead call them from our own wrappers
    """
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

        self._market_data_q_dict = {}
        self._stock_price_data_q_dict = {}
        
        

    def start_getting_stock_prices(self, inv_ticker, inv_exc_symbol, ibcontract, this_tradeagent):
        """
        From the contract, request market data 
        :returns market prices
        """
        
        # first create a unique request id
        thisreqId = get_next_reqid() 
    
        # Now update the tracking record with the stock info
        # save info to the tracking dict ready for when data comes in
        self._stock_price_data_q_dict[thisreqId] = self.wrapper.init_stockpriceTracker(thisreqId) 
        
        self.wrapper.savestockpriceTracker(tickerid=thisreqId, 
                                           inv_ticker=inv_ticker, 
                                           inv_exc_symbol=inv_exc_symbol, 
                                           datetime_last_bid_write_to_frame='',
                                           datetime_last_ask_write_to_frame='',
                                           tradeagent=this_tradeagent)

        
        # start getting data 
        tickerid = self.start_getting_IB_market_data(ibcontract, tickerid = thisreqId)
        
        if DEBUG:
            print('After requested data for this stock')
            print('####################################')
            print('  ')
                           
        
        return 


    ## ====================== CONTRACT_DETAILS =================================
    
    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID):

        """
        From a partially formed contract, returns a fully fledged version
        :returns fully resolved IB contract
        """

        ## Make a place to store the data we're going to return
        contract_details_queue = finishableQueue(self.init_contractDetails(reqId))

        if DEBUG:
            print("Resolving contract with IB server...  reqId",reqId,'ibcontract',ibcontract)
        
        self.reqContractDetails(reqId, ibcontract)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 30
        new_contract_details = contract_details_queue.get(timeout = MAX_WAIT_SECONDS)

        
        while self.wrapper.is_error():
            print(self.get_error())

        if len(new_contract_details)>1:
            
            if DEBUG:
                print("got multiple contracts - finding first one with the right expiry date")
                print(' ')
            
            expected_expiry = ibcontract.lastTradeOrContractMonth
            found_it = False    
            for this_contract_details in new_contract_details:
                if not(found_it):
                    this_expiry = this_contract_details.contract.lastTradeDateOrContractMonth
                    if this_expiry == expected_expiry:
                        found_it = True
                        new_contract_details = this_contract_details
            
            # if couldn't find matching one then take the first
            if not(found_it):
                new_contract_details = new_contract_details[0]
            resolved_ibcontract=new_contract_details
            
        elif len(new_contract_details) == 1:
            # found only one!
            resolved_ibcontract=new_contract_details
            
        else:
            # didn't find anything
            print('*** ERROR - Ignoring this contract')
            resolved_ibcontract=ibcontract   
        

        return resolved_ibcontract
    
    ## ====================== MARKET_DATA =================================
    
    def start_getting_IB_market_data(self, resolved_ibcontract, tickerid=DEFAULT_MARKET_DATA_ID):
        """
        Kick off market data streaming
        :param resolved_ibcontract: a Contract object
        :param tickerid: the identifier for the request
        :return: tickerid
        """
        
        if DEBUG:
            print('Requesting IB_market_data for reqId=',tickerid,'resolved_ibcontract=',str(resolved_ibcontract))
        self._market_data_q_dict[tickerid] = self.wrapper.init_market_data(tickerid)
        self.reqMktData(tickerid, resolved_ibcontract, "", False, False, [])
        
        return tickerid
    

    def stop_getting_IB_market_data(self, tickerid):
        """
        Stops the stream of market data and returns all the data we've had since we last asked for it
        :param tickerid: identifier for the request
        :return: market data
        """
        print('stop_getting_IB_Market_data')
        ## native EClient method
        self.cancelMktData(tickerid)

        ## Sometimes a lag whilst this happens, this prevents 'orphan' ticks appearing
        time.sleep(5)

        market_data = self.get_IB_market_data(tickerid)

        ## output ay errors
        while self.wrapper.is_error():
            print(self.get_error())

        return market_data

    def get_IB_market_data(self, tickerid):
        """
        Takes all the market data we have received so far out of the stack, and clear the stack
        :param tickerid: identifier for the request
        :return: market data
        """
        print('get_IB_market_data')
        ## how long to wait for next item
        MAX_WAIT_MARKETDATEITEM = 5
        market_data_q = self._market_data_q_dict[tickerid]

        market_data=[]
        finished=False

        while not finished:
            try:
                market_data.append(market_data_q.get(timeout=MAX_WAIT_MARKETDATEITEM))
                if DEBUG:
                    print('length of market_data = ', len(market_data))
            except queue.Empty:
                ## no more data
                finished=True
        
        return stream_of_ticks(market_data)


## ====================== APP =================================

class TestApp(TestWrapper, TestClient):
    
    
    def __init__(self, ipaddress, portid, clientid):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)
        
        # The parameter is always ignored.
        self.reqIds(-1)
        self.nextValidOrderId = None
        self.simplePlaceOid   = None
        self.order_store = {}
        self.permId2ord = {}
        
        thread = Thread(target = self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.init_error()
        
        
    def nextValidId(self, orderId):
        super().nextValidId(orderId)

        print("setting nextValidOrderId: ", orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)    

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid
