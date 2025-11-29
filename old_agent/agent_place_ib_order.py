#------------------------------------------------------------
# Filename  : agent_place_ib_order.py
# Project   : ava
#
# Descr     : This holds routines relating to the IB API for placing orders
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-05-08   1 MW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------
               


import calendar

from utils.config import DEBUG, IB_API_PORT, IB_API_GATEWAY_IP

#import datetime
from ibapi.client import EClient
from ibapi.order import Order
from ibapi.order_condition import * # @UnusedWildImport
from ibapi.contract import Contract as IBcontract
from ibapi.execution import Execution, ExecutionFilter

from ibapi.wrapper import EWrapper
import numpy as np
#import pandas as pd
import queue
from threading import Thread
import time


# Globals

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


def create_contract(inv_ticker, inv_exc_symbol):
    
    if inv_exc_symbol == 'TSX':
        ibcontract = IBcontract()
        ibcontract.symbol = inv_ticker  
        ibcontract.secType = "STK" 
        ibcontract.currency="CAD"
        ibcontract.exchange="SMART" 
        ibcontract.primaryExchange="TSE" 
        
    if inv_exc_symbol == 'NYSE':
        ibcontract = IBcontract()
        ibcontract.symbol = inv_ticker  
        ibcontract.secType = "STK" 
        ibcontract.currency="USD"
        ibcontract.exchange="SMART" 
        ibcontract.primaryExchange="NYSE" 
        
    if inv_exc_symbol == 'LSE':
        ibcontract = IBcontract()
        ibcontract.symbol = inv_ticker  
        ibcontract.secType = "STK" 
        ibcontract.currency="GBP"
        ibcontract.exchange="SMART" 
        ibcontract.primaryExchange="LSE" 
        
    if inv_exc_symbol == 'ASX':
        ibcontract = IBcontract()
        ibcontract.symbol = inv_ticker  
        ibcontract.secType = "STK" 
        ibcontract.currency="AUD"
        ibcontract.exchange="SMART" 
        ibcontract.primaryExchange="ASX" 

    if inv_exc_symbol == 'TSEJ':
        ibcontract = IBcontract()
        ibcontract.symbol = inv_ticker  
        ibcontract.secType = "STK" 
        ibcontract.currency="JPY"
        ibcontract.exchange="SMART" 
        ibcontract.primaryExchange="TSEJ"     
           
    return ibcontract


def create_order(action, order_type, quantity, price):
    
    print('creating_order', action, order_type, quantity, price)
           
    if order_type == 'MKT':
        iborder = MarketOrder(action   = action, 
                              quantity = quantity)
    elif order_type == 'LMT':
        iborder = LimitOrder(action    = action, 
                             quantity   = quantity, 
                             limitprice = price)
    else:
        print('*** ERROR - invalid order_type', order_type)

    return iborder


def LimitOrder(action, quantity, limitprice):
    
    # ! [limitorder]
    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = limitprice
    # ! [limitorder]
    return order

    
def MarketOrder(action, quantity):
    
    #! [market]
    order = Order()
    order.action = action
    order.orderType = "MKT"
    order.totalQuantity = quantity
    #! [market]
    return order

    
def agent_place_ib_order(app, inv_ticker, inv_exc_symbol, action, order_type, quantity, price):

    thisreqId = get_next_reqid() 
    
    ibcontract = create_contract(inv_ticker, inv_exc_symbol)
    print('about to resolve ibcontract',ibcontract)
    resolved_ibcontract=app.resolve_ib_contract(ibcontract=ibcontract, reqId=thisreqId)
    
    if resolved_ibcontract == ibcontract:
        print('ERROR - not resolved so not continuing')
        print('**************************************')
       
    else:
        if DEBUG:
            print("Resolved Contract: " + str(resolved_ibcontract))
        
        thisreqId = get_next_reqid() # to make it unique
        
        iborder = create_order(action, order_type, quantity, price)
    
        app.place_order(inv_ticker, ibcontract, iborder)
        
        if DEBUG:
            print('======================================================')
            print('Finished processing for this get_IB_stockprice_data call')
            print('======================================================')
        

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

## ====================== WRAPPER =================================


class TestWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens, like currentTime
    Extra methods are added as we need to store the results in this object
    """

    def __init__(self):
        self._my_order_details = {}
        self._my_contract_details = {}
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


    ## ====================== order TRACKING DATA ===================

    ## get contract details code
    def init_orderTracker(self, tickerid):
        order_tracker_queue = self._my_order_tracker[tickerid] = queue.Queue()

        return order_tracker_queue
    

    def saveorderTracker(self, tickerid, inv_ticker, datetime_last_bid_write_to_frame, datetime_last_ask_write_to_frame):

        # first the call
        this_tracking_dict = {
            "frameid":(inv_ticker),
            "inv_ticker":inv_ticker,
            "datetime_last_bid_write_to_frame":datetime_last_bid_write_to_frame,
            "datetime_last_ask_write_to_frame":datetime_last_ask_write_to_frame
            }
        
        if DEBUG:
            print('this_tracking_dict',this_tracking_dict)
        
        if tickerid not in self._my_order_tracker.keys():
            self.init_orderTracker(tickerid)
            
        self._my_order_tracker[tickerid].put(this_tracking_dict) 
        
        
    def orderTrackerEnd(self, tickerid):
        ## overriden method
        if tickerid not in self._my_order_tracker.keys():
            self.init_orderTracker(tickerid)

        self._my_order_tracker[tickerid].put(FINISHED)
        

    def getorderTrackerInfo(self, tickerid):
          
        order_tracker_info = self._my_order_tracker[tickerid]
        
        return order_tracker_info
    
      
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
        self._order_data_q_dict = {}
        
        

    def place_order(self, inv_ticker, ibcontract, iborder):
        """
        From the contract and order, place the order 
        :returns ?
        """
        # first create a unique request id
        thisreqId = get_next_reqid() 
    
        # Now update the tracking record with the stock info
        # save info to the tracking dict ready for when data comes in
        self._order_data_q_dict[thisreqId] = self.wrapper.init_orderTracker(thisreqId) 
        
        self.wrapper.saveorderTracker(tickerid=thisreqId, 
                                           inv_ticker=inv_ticker, 
                                           datetime_last_bid_write_to_frame='',
                                           datetime_last_ask_write_to_frame='')

        
        self.placeOrder(thisreqId, ibcontract, iborder)
        
        if DEBUG:
            print('After placed order')
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
    
    
    ## ================================ execDetails ============================
    
    # ! [execdetails]
    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        print("ExecDetails. ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType, "Currency:", contract.currency, execution)
    # ! [execdetails]
     
    # ! [execdetailsend]
    def execDetailsEnd(self, reqId):
        super().execDetailsEnd(reqId)
        print("ExecDetailsEnd. ReqId:", reqId)
    # ! [execdetailsend]
     

## ====================== APP =================================

class TestApp(TestWrapper, TestClient):
    
    
    def __init__(self, ipaddress, portid, clientid):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target = self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.init_error()




    