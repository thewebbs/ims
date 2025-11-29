#------------------------------------------------------------
# Filename  : agent_action_dividend_todos.py
# Project   : ava
#
# Descr     : This holds routines relating to the IB API to get fundamentals data
#             This creates records in ims_dividends
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-01-01   1 MW  Initial write based on TWS Program.py
# ...
# 2021-12-19 100 DW  Added version 
# 2021-12-27 101 MW  Changed start_date to start_datetime
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------


"""
Copyright (C) 2019 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
 and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""

from utils.config import DEBUG, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD

import argparse
import datetime
import collections
import inspect
# SEP 2019 MW added next line
from apis.ib_api.ConnectionError import ConnectionError

import logging
import time
import os.path
import sys

from ibapi import wrapper
from ibapi import utils
from ibapi.client import EClient
from ibapi.utils import iswrapper

# types
from ibapi.common import * # @UnusedWildImport
from ibapi.order_condition import * # @UnusedWildImport
from ibapi.contract import * # @UnusedWildImport
from ibapi.order import * # @UnusedWildImport
from ibapi.order_state import * # @UnusedWildImport
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.ticktype import * # @UnusedWildImport
from ibapi.tag_value import TagValue

from ibapi.account_summary_tags import *

from apis.ib_api.ContractSamples import ContractSamples

from lxml import etree as ET

import pandas as pd
from database.db_objects.ImsDividendDB import ImsDividendDB
from database.db_objects.ImsLoadDoneDB import ImsLoadDoneDB
from database.db_objects.ImsLoadTodoDB import delete_load_todo, get_load_todo, set_todo_status
from database.db_objects.ImsExchangeDB import get_ticker, get_exchange

from utils.utils_database import close_db, open_db
from utils.config import DEBUG, IB_API_PORT, IB_API_GATEWAY_IP, SLACK_BOT_TOKEN, IB_API_STOP_TIME_HR
from utils.config import IB_API_STOP_TIME_MIN, IB_API_STOP_TIME_SEC, IB_API_START_TIME_HR, IB_API_START_TIME_MIN
from utils.config import IB_API_START_TIME_SEC, IB_API_SLEEP_PERIOD, IB_API_SLEEP_BEFORE_EXIT
from utils.config import IB_DIVIDEND_CLIENTID

# keep track of number of lost connection errors
LOST_CONNECTION_ERRORS = ConnectionError()

DB = None   

def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    #time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'

    timefmt = '%y%m%d_%H:%M:%S'

    #logging.basicConfig(filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
    logging.basicConfig(filename=time.strftime("log/agent_action_dividend_todos.%y%m%d_%H%M%S.log"),
                        filemode="w",
                        level=logging.ERROR,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)


def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        
        if (fn.__name__ != 'historicalDataOperations_req'):
            print("   done w/", fn.__name__)

    return fn2


def printinstance(inst: Object):
    attrs = vars(inst)
    print(', '.join("%s: %s" % item for item in attrs.items()))


class Activity(Object):
    def __init__(self, reqMsgId, ansMsgId, ansEndMsgId, reqId):
        self.reqMsdId = reqMsgId
        self.ansMsgId = ansMsgId
        self.ansEndMsgId = ansEndMsgId
        self.reqId = reqId


class RequestMgr(Object):
    def __init__(self):
        # I will keep this simple even if slower for now: only one list of
        # requests finding will be done by linear search
        self.requests = []

    def addReq(self, req):
        self.requests.append(req)

    def receivedMsg(self, msg):
        pass


# ! [socket_declare]
class TestClient(EClient):

    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        # ! [socket_declare]

        # how many times a method is called to see test coverage
        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
        self.setupDetectReqId()


    def countReqId(self, methName, fn):
        def countReqId_(*args, **kwargs):
            self.clntMeth2callCount[methName] += 1
            idx = self.clntMeth2reqIdIdx[methName]
            if idx >= 0:
                sign = -1 if 'cancel' in methName else 1
                self.reqId2nReq[sign * args[idx]] += 1
            return fn(*args, **kwargs)

        return countReqId_


    def setupDetectReqId(self):

        methods = inspect.getmembers(EClient, inspect.isfunction)
        for (methName, meth) in methods:
            if methName != "send_msg":
                # don't screw up the nice automated logging in the send_msg()
                self.clntMeth2callCount[methName] = 0
                # logging.debug("meth %s", name)
                sig = inspect.signature(meth)
                for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                    (paramName, param) = pnameNparam # @UnusedVariable
                    if paramName == "reqId":
                        self.clntMeth2reqIdIdx[methName] = idx

                setattr(TestClient, methName, self.countReqId(methName, meth))

                # print("TestClient.clntMeth2reqIdIdx", self.clntMeth2reqIdIdx)


# ! [ewrapperimpl]
class TestWrapper(wrapper.EWrapper):
    # ! [ewrapperimpl]
    
    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
        self.setupDetectWrapperReqId()

    # TODO: see how to factor this out !!

    def countWrapReqId(self, methName, fn):
        def countWrapReqId_(*args, **kwargs):
            self.wrapMeth2callCount[methName] += 1
            idx = self.wrapMeth2reqIdIdx[methName]
            if idx >= 0:
                self.reqId2nAns[args[idx]] += 1
            return fn(*args, **kwargs)

        return countWrapReqId_


    def setupDetectWrapperReqId(self):

        methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
        for (methName, meth) in methods:
            self.wrapMeth2callCount[methName] = 0
            # logging.debug("meth %s", name)
            sig = inspect.signature(meth)
            for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                (paramName, param) = pnameNparam # @UnusedVariable
                # we want to count the errors as 'error' not 'answer'
                if 'error' not in methName and paramName == "reqId":
                    self.wrapMeth2reqIdIdx[methName] = idx

            setattr(TestWrapper, methName, self.countWrapReqId(methName, meth))

            # print("TestClient.wrapMeth2reqIdIdx", self.wrapMeth2reqIdIdx)


# this is here for documentation generation
"""
#! [ereader]
        # You don't need to run this in your code!
        self.reader = reader.EReader(self.conn, self.msg_queue)
        self.reader.start()   # start thread
#! [ereader]
"""

# ! [socket_init]
class TestApp(TestWrapper, TestClient):

    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid = None
        self.req_id = 0
        
        self.number_active_requests = 0
        print('initialized number active requests to zero')
        
        
        # Jan 2020 adding following to track requests
        self.inv_ticker = ''
        self.start_datetime = ''
        self.end_datetime = ''
        self.freq_type = ''
        self.progress_store = {}
        self.tracking_store = {}
        self.ims_load_todos = {}
                
        # work out 8:30pm and 9:30pm as times for comparison
        self.starttime = datetime.datetime.today().replace(hour=IB_API_START_TIME_HR, minute=IB_API_START_TIME_MIN, second=IB_API_START_TIME_SEC, microsecond=0)
        
        self.stoptime = datetime.datetime.today().replace(hour=IB_API_STOP_TIME_HR, minute=IB_API_STOP_TIME_MIN, second=IB_API_STOP_TIME_SEC, microsecond=0)
        
        
    def dumpTestCoverageSituation(self):
        for clntMeth in sorted(self.clntMeth2callCount.keys()):
            logging.debug("ClntMeth: %-30s %6d" % (clntMeth,
                                                   self.clntMeth2callCount[clntMeth]))

        for wrapMeth in sorted(self.wrapMeth2callCount.keys()):
            logging.debug("WrapMeth: %-30s %6d" % (wrapMeth,
                                                   self.wrapMeth2callCount[wrapMeth]))

    def dumpReqAnsErrSituation(self):
        logging.debug("%s\t%s\t%s\t%s" % ("ReqId", "#Req", "#Ans", "#Err"))
        for reqId in sorted(self.reqId2nReq.keys()):
            nReq = self.reqId2nReq.get(reqId, 0)
            nAns = self.reqId2nAns.get(reqId, 0)
            nErr = self.reqId2nErr.get(reqId, 0)
            logging.debug("%d\t%d\t%s\t%d" % (reqId, nReq, nAns, nErr))

    @iswrapper
    # ! [connectack]
    def connectAck(self):
        if self.asynchronous:
            self.startApi()

    # ! [connectack]

    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        #print("NextValidId:", orderId)
    # ! [nextvalidid]

        # we can start now
        self.start()
        

    def start(self):
        
        global DB
       
        if self.started:
            
            return

        self.started = True
        if self.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.reqGlobalCancel()
        else:
            
            print(' ')
            print('Starting fundamental requests')   
            
            newStatus = 'RDY'
            oldStatus = 'WIP'
            result = set_todo_status(database           = DB,
                                     lto_inv_ticker     = '%', 
                                     lto_freq_type      = '%', 
                                     lto_req_type       = 'DIV', 
                                     lto_start_datetime = '%', 
                                     old_lto_status     = oldStatus, 
                                     new_lto_status     = newStatus)
            print(' ')
            print('Set WIP DIV ims_load_todos record to RDY')   
            
            self.fundamentalsOperations_req()
            
   
    def keyboardInterrupt(self):
        self.nKeybInt += 1
        if self.nKeybInt == 1:
            self.stop()
        else:
            print("Finishing test")
            self.done = True


    def stop(self):
        print("Executing cancels")
        
        self.fundamentalsOperations_cancel()
        
        print("Executing cancels ... finished")

        print('Exiting from here too')
        sys.exit()
        
        
    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid


    @iswrapper
    # ! [error]
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        if (errorCode != 2104) and (errorCode != 2106) and (errorCode != 2107) and (errorCode != 366):
            super().error(reqId, errorCode, errorString)
        
        global LOST_CONNECTION_ERRORS
        error_datetime = datetime.datetime.today() 
        
        if (errorCode != 2104) and (errorCode != 2106) and (errorCode != 2107) and (errorCode != 366):
            
            print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)

            if (errorCode == 200) or (errorCode == 162) or (errorCode == 165) or (errorCode == 430):
            
                this_rec = self.progress_store[reqId]
                request_type = this_rec[1]
                    
                this_div_id = self.progress_store[reqId][0]
                error_message = 'Error ' + str(errorCode) + ' for request id ' + str(this_div_id)
                print(error_datetime, error_message)
                                    
                # get the pk info for this div_id
                inv_ticker      = this_div_id.split('$')[0]
                freq_type       = this_div_id.split('$')[1]
                req_type        = this_div_id.split('$')[2]
                this_start_date = this_div_id.split('$')[3]
                this_end_date   = this_div_id.split('$')[4]
                #start_date = datetime.datetime.strptime(this_start_date,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                #end_date = datetime.datetime.strptime(this_end_date,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                    
                oldStatus = 'WIP'
                result = set_todo_status(database           = DB,
                                         lto_inv_ticker     = inv_ticker, 
                                         lto_freq_type      = freq_type, 
                                         lto_req_type       = req_type, 
                                         lto_start_datetime = this_start_date, 
                                         old_lto_status     = oldStatus, 
                                         new_lto_status     = errorCode)
    
    
                print(error_datetime, 'Set this ims_load_todos record to In Error code ',str(errorCode), inv_ticker, freq_type, req_type, this_start_date)   
                    
                print(error_datetime, 'About to cancel reqid ',reqId, ' for ', inv_ticker, freq_type, req_type, this_start_date)
                self.fundamentalsOperations_cancel()
                
                if self.number_active_requests > 0:
                    self.number_active_requests -= 1
                else:
                    print('was already zero - should not have happened')
                    
                print('after cancelling, number of active requests = ',self.number_active_requests)
                
                # update the progress store to True to show this reqId was completed
                self.progress_store[reqId][2] = True
        
                print(error_datetime, 'Cancelled reqid ',reqId, ' for ', inv_ticker, freq_type, req_type, this_start_date)
            
                
                the_message = '%s * Div Dataload request cancel for request %3.0f for ticker %s type %s %s for date %s ' % (error_datetime, reqId, inv_ticker, freq_type, req_type, this_start_date)
            
                
                print(error_datetime, 'Creating a new replacement request (DIV)')
                self.fundamentalsOperations_req()
        
            else:
            
                if (errorCode != 323) and (errorCode != 324) and (errorCode != 325) and (errorCode != 354):
                    
                    #
                    # It wasn't an invalid data request so keep track of lost connection
                    #
                    LOST_CONNECTION_ERRORS.add_error(error_datetime)   
                       
            if LOST_CONNECTION_ERRORS.too_many_errors():
                num_errors = LOST_CONNECTION_ERRORS.num_errors()
                print(error_datetime, " Killing self because too many connection errors ", num_errors)
                the_message = " %s Killing self because too many connection errors" % (error_datetime)
                                     
                sys.exit()
            
            if  (errorCode != 200) and (errorCode != 162) and (errorCode != 165):  
                super().error(reqId, errorCode, errorString)
                num_errors = LOST_CONNECTION_ERRORS.num_errors()
                print(error_datetime, "Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString, 'LOST_CONNECTION_ERRORS = ', num_errors)
        

    @iswrapper
    def winError(self, text: str, lastError: int):
        super().winError(text, lastError)

    @iswrapper
    # ! [openorder]
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        
        print('in orderStatus')
        
        
    @iswrapper
    # ! [openorderend]
    def openOrderEnd(self):
        super().openOrderEnd()
        
        print("in OpenOrderEnd")
        

    @iswrapper
    # ! [orderstatus]
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        
        print('in orderStatus')
        

    @printWhenExecuting
    def accountOperations_req(self):
        
        print('in accountOperations_req')
        

    @printWhenExecuting
    def accountOperations_cancel(self):
        
        print('in accountOperations_cancel')
  

    def pnlOperations_req(self):
        
        print('in pnlOperations_req')
  

    def pnlOperations_cancel(self):
        
        print('in pnlOperations_cancel')
        

    def histogramOperations_req(self):
       
        print('in histogramOperations_req')
     

    def histogramOperations_cancel(self):
        
        print('in histogramOperations_cancel')
 

    def continuousFuturesOperations_req(self):
        
        print('in continuousFuturesOperations_req')
  

    def continuousFuturesOperations_cancel(self):
        
        print('in continuousFuturesOperations_cancel')
   

    @iswrapper
    # ! [managedaccounts]
    def managedAccounts(self, accountsList: str):
        super().managedAccounts(accountsList)
        print("Account list:", accountsList)
        # ! [managedaccounts]

        self.account = accountsList.split(",")[0]
  

    @iswrapper
    # ! [accountsummary]
    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        
        print('in accountSummary')
        
        
    @iswrapper
    # ! [accountsummaryend]
    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        
        print('in accountsummaryend')
        

    @iswrapper
    # ! [updateaccountvalue]
    def updateAccountValue(self, key: str, val: str, currency: str,
                           accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        
        print('in updateAccountValue')
        

    @iswrapper
    # ! [updateportfolio]
    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        super().updatePortfolio(contract, position, marketPrice, marketValue,
                                averageCost, unrealizedPNL, realizedPNL, accountName)
        print('in updatePortfolio')
        

    @iswrapper
    # ! [updateaccounttime]
    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        
        print('in updateAccountTime')
        

    @iswrapper
    # ! [accountdownloadend]
    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        
        print('in accountDownloadEnd')
        

    @iswrapper
    # ! [position]
    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        
        print('in position')
        

    @iswrapper
    # ! [positionend]
    def positionEnd(self):
        super().positionEnd()
        
        print('in positionEnd')
        

    @iswrapper
    # ! [positionmulti]
    def positionMulti(self, reqId: int, account: str, modelCode: str,
                      contract: Contract, pos: float, avgCost: float):
        super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
        
        print('in positionMulti')
        

    @iswrapper
    # ! [positionmultiend]
    def positionMultiEnd(self, reqId: int):
        super().positionMultiEnd(reqId)
        
        print('in positionMultiEnd')
        

    @iswrapper
    # ! [accountupdatemulti]
    def accountUpdateMulti(self, reqId: int, account: str, modelCode: str,
                           key: str, value: str, currency: str):
        super().accountUpdateMulti(reqId, account, modelCode, key, value,
                                   currency)
        
        print('in accountUpdateMulti')
        

    @iswrapper
    # ! [accountupdatemultiend]
    def accountUpdateMultiEnd(self, reqId: int):
        super().accountUpdateMultiEnd(reqId)
        
        print('in accountUpdateMultiEnd')
        

    @iswrapper
    # ! [familyCodes]
    def familyCodes(self, familyCodes: ListOfFamilyCode):
        super().familyCodes(familyCodes)
        
        print('in familyCodes')
        

    @iswrapper
    # ! [pnl]
    def pnl(self, reqId: int, dailyPnL: float,
            unrealizedPnL: float, realizedPnL: float):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        
        print('in pnl')
      

    @iswrapper
    # ! [pnlsingle]
    def pnlSingle(self, reqId: int, pos: int, dailyPnL: float,
                  unrealizedPnL: float, realizedPnL: float, value: float):
        super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
        
        print('in pnlSingle')
        

    def marketDataTypeOperations(self):
        # ! [reqmarketdatatype]
        
        print('in marketDataTypeOperations')
        
        
    @iswrapper
    # ! [marketdatatype]
    def marketDataType(self, reqId: TickerId, marketDataType: int):
        super().marketDataType(reqId, marketDataType)
        
        print('in marketDataType')
        

    @printWhenExecuting
    def tickDataOperations_req(self):
        
        print('in tickDataOperations_req')
        

    @printWhenExecuting
    def tickDataOperations_cancel(self):
        
        print('in tickDataOperations_cancel')
        

    @iswrapper
    # ! [tickprice]
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        
        print('in tickPrice')
        

    @iswrapper
    # ! [ticksize]
    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)
        
        print('in tickSize')
        

    @iswrapper
    # ! [tickgeneric]
    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        super().tickGeneric(reqId, tickType, value)
        
        print('in tickGeneric')
        

    @iswrapper
    # ! [tickstring]
    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        super().tickString(reqId, tickType, value)
        
        print('in tickString')

    @iswrapper
    # ! [ticksnapshotend]
    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        
        print('in tickSnapshotEnd')
        
    @iswrapper
    # ! [rerouteMktDataReq]
    def rerouteMktDataReq(self, reqId: int, conId: int, exchange: str):
        super().rerouteMktDataReq(reqId, conId, exchange)
        
        print('in rerouteMktDataReq')
        

    @iswrapper
    # ! [marketRule]
    def marketRule(self, marketRuleId: int, priceIncrements: ListOfPriceIncrements):
        super().marketRule(marketRuleId, priceIncrements)
        
        print('in marketRule')
        

    @printWhenExecuting
    def tickByTickOperations_req(self):
        
        print('in tickByTickOperations_req')
        

    @printWhenExecuting
    def tickByTickOperations_cancel(self):
        
        print('in tickByTickOperations_cancel')
        
        
    @iswrapper
    # ! [orderbound]
    def orderBound(self, orderId: int, apiClientId: int, apiOrderId: int):
        super().orderBound(orderId, apiClientId, apiOrderId)
        
        print('in orderBound')
        

    @iswrapper
    # ! [tickbytickalllast]
    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float,
                          size: int, tickAtrribLast: TickAttribLast, exchange: str,
                          specialConditions: str):
        super().tickByTickAllLast(reqId, tickType, time, price, size, tickAtrribLast,
                                  exchange, specialConditions)
        
        print('in tickByTickAllLast')
        

    @iswrapper
    # ! [tickbytickbidask]
    def tickByTickBidAsk(self, reqId: int, time: int, bidPrice: float, askPrice: float,
                         bidSize: int, askSize: int, tickAttribBidAsk: TickAttribBidAsk):
        super().tickByTickBidAsk(reqId, time, bidPrice, askPrice, bidSize,
                                 askSize, tickAttribBidAsk)
        
        print('in tickByTickBidAsk')
        

    # ! [tickbytickmidpoint]
    @iswrapper
    def tickByTickMidPoint(self, reqId: int, time: int, midPoint: float):
        super().tickByTickMidPoint(reqId, time, midPoint)
        
        print('in tickByTickMidPoint')
        

    @printWhenExecuting
    def marketDepthOperations_req(self):
        
        print('in marketDepthOperations_req')
        

    @printWhenExecuting
    def marketDepthOperations_cancel(self):
        
        print('in marketDepthOperations_cancel')
        

    @iswrapper
    # ! [updatemktdepth]
    def updateMktDepth(self, reqId: TickerId, position: int, operation: int,
                       side: int, price: float, size: int):
        super().updateMktDepth(reqId, position, operation, side, price, size)
        
        print('in updateMktDepth')
        

    @iswrapper
    # ! [updatemktdepthl2]
    def updateMktDepthL2(self, reqId: TickerId, position: int, marketMaker: str,
                         operation: int, side: int, price: float, size: int, isSmartDepth: bool):
        super().updateMktDepthL2(reqId, position, marketMaker, operation, side,
                                 price, size, isSmartDepth)
        
        print('in updateMktDepthL2')
        

    @iswrapper
    # ! [rerouteMktDepthReq]
    def rerouteMktDepthReq(self, reqId: int, conId: int, exchange: str):
        super().rerouteMktDataReq(reqId, conId, exchange)
        
        print('in rerouteMktDepthReq')


    @printWhenExecuting
    def realTimeBarsOperations_req(self):
        #
        print('in realTimeBarsOperations_req')


    @iswrapper
    # ! [realtimebar]
    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float,
                        volume: int, wap: float, count: int):
        super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        
        print('in realtimeBar')

    @printWhenExecuting
    def realTimeBarsOperations_cancel(self):
        
        print('in realTimeBarsOperations_cancel')
        

    @printWhenExecuting
    def historicalDataOperations_req(self):
        
        print('in historicalDataOperations_req')
        # ! [reqhistoricaldata]

    @printWhenExecuting
    def historicalDataOperations_cancel(self):
       
        print('in historicalDataOperations_cancel')
        
        
    @printWhenExecuting
    def historicalTicksOperations(self):
        # ! [reqhistoricalticks]
        
        print('in historicalTicksOperations')
        

    @iswrapper
    # ! [headTimestamp]
    def headTimestamp(self, reqId:int, headTimestamp:str):
        print("HeadTimestamp. ReqId:", reqId, "HeadTimeStamp:", headTimestamp)
    # ! [headTimestamp]

    @iswrapper
    # ! [histogramData]
    def histogramData(self, reqId:int, items:HistogramDataList):
        print("HistogramData. ReqId:", reqId, "HistogramDataList:", "[%s]" % "; ".join(map(str, items)))
    # ! [histogramData]

    @iswrapper
    # ! [historicaldata]
    def historicalData(self, reqId:int, bar: BarData):
        
        print('in historicalData')
        
        
    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        
        print('in historicalDataEnd')
        
        
    @iswrapper
    # ! [historicalDataUpdate]
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        
        print('in historicalDataUpdate')
        

    @iswrapper
    # ! [historicalticks]
    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTick, done: bool):
        
        print('in historicalTicks')
        

    @iswrapper
    # ! [historicalticksbidask]
    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk,
                              done: bool):
        
        print('in historicalTicksBidAsk')
        

    @iswrapper
    # ! [historicaltickslast]
    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        
        print('in historicalTicksLast')
        

    @printWhenExecuting
    def optionsOperations_req(self):
        
        print('in optionsOperations_req')
        
        
    @printWhenExecuting
    def optionsOperations_cancel(self):
        
        print('in optionsOperations_cancel')
        

    @iswrapper
    # ! [securityDefinitionOptionParameter]
    def securityDefinitionOptionParameter(self, reqId: int, exchange: str,
                                          underlyingConId: int, tradingClass: str, multiplier: str,
                                          expirations: SetOfString, strikes: SetOfFloat):
        super().securityDefinitionOptionParameter(reqId, exchange,
                                                  underlyingConId, tradingClass, multiplier, expirations, strikes)
        
        print('in securityDefinitionOptionParameter')
        
    @iswrapper
    # ! [securityDefinitionOptionParameterEnd]
    def securityDefinitionOptionParameterEnd(self, reqId: int):
        super().securityDefinitionOptionParameterEnd(reqId)
        
        print('in securityDefinitionOptionParameterEnd')
        
    @iswrapper
    # ! [tickoptioncomputation]
    def tickOptionComputation(self, reqId: TickerId, tickType: TickType,
                              impliedVol: float, delta: float, optPrice: float, pvDividend: float,
                              gamma: float, vega: float, theta: float, undPrice: float):
        super().tickOptionComputation(reqId, tickType, impliedVol, delta,
                                      optPrice, pvDividend, gamma, vega, theta, undPrice)
        
        print('in tickOptionComputation')
        

    @printWhenExecuting
    def contractOperations(self):
        
        print('in contractOperations')
      

    @printWhenExecuting
    def newsOperations_req(self):
        
        print('in newsOperations_req')
        

    @printWhenExecuting
    def newsOperations_cancel(self):
        
        print('in newsOperations_cancel')
        
    @iswrapper
    #! [tickNews]
    def tickNews(self, tickerId: int, timeStamp: int, providerCode: str,
                 articleId: str, headline: str, extraData: str):
        
        print('in tickNews')
        

    @iswrapper
    #! [historicalNews]
    def historicalNews(self, reqId: int, time: str, providerCode: str,
                       articleId: str, headline: str):
        
        print('in historicalNews')
        

    @iswrapper
    #! [historicalNewsEnd]
    def historicalNewsEnd(self, reqId:int, hasMore:bool):
        
        print('in historicalNewsEnd')
        
    @iswrapper
    #! [newsProviders]
    def newsProviders(self, newsProviders: ListOfNewsProviders):
        
        print('in newsProviders')
        

    @iswrapper
    #! [newsArticle]
    def newsArticle(self, reqId: int, articleType: int, articleText: str):
        
        print('in newsArticle')
        

    @iswrapper
    # ! [contractdetails]
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        
        print('in contractdetails')
        
    @iswrapper
    # ! [bondcontractdetails]
    def bondContractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().bondContractDetails(reqId, contractDetails)
        
        print('in bondContractDetails')
        
    @iswrapper
    # ! [contractdetailsend]
    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        
        print('in contractDetailsEnd')
        
    @iswrapper
    # ! [symbolSamples]
    def symbolSamples(self, reqId: int,
                      contractDescriptions: ListOfContractDescription):
        super().symbolSamples(reqId, contractDescriptions)
        
        print('in symbolSamples')
        

    @printWhenExecuting
    def marketScannersOperations_req(self):
        
        print('in marketScannersOperations_req')
        

    @printWhenExecuting
    def marketScanners_cancel(self):
        
        print('in marketScanners_cancel')
        

    @iswrapper
    # ! [scannerparameters]
    def scannerParameters(self, xml: str):
        super().scannerParameters(xml)
        
        print('in scannerParameters')
        

    @iswrapper
    # ! [scannerdata]
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails,
                    distance: str, benchmark: str, projection: str, legsStr: str):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark,
                            projection, legsStr)
     
        print('in scannerData')
        

    @iswrapper
    # ! [scannerdataend]
    def scannerDataEnd(self, reqId: int):
        super().scannerDataEnd(reqId)
        
        print('in scannerDataEnd')
        

    @iswrapper
    # ! [smartcomponents]
    def smartComponents(self, reqId:int, smartComponentMap:SmartComponentMap):
        super().smartComponents(reqId, smartComponentMap)
        
        print('in smartComponents')
        

    @iswrapper
    # ! [tickReqParams]
    def tickReqParams(self, tickerId:int, minTick:float,
                      bboExchange:str, snapshotPermissions:int):
        super().tickReqParams(tickerId, minTick, bboExchange, snapshotPermissions)
        
        print('in tickReqParams')
        

    @iswrapper
    # ! [mktDepthExchanges]
    def mktDepthExchanges(self, depthMktDataDescriptions:ListOfDepthExchanges):
        super().mktDepthExchanges(depthMktDataDescriptions)
        
        print('in mktDepthExchanges')
        

    @printWhenExecuting
    def fundamentalsOperations_req(self):
        
        global DB
        
        self.number_active_requests += 1
        
        # Need to work out the frequency type, duration and queryTime from items in the ims_load_todos table 
        # we will only return one request

        todo_list = get_load_todo(database           = DB,
                                  lto_inv_ticker     = '%', 
                                  lto_freq_type      = '%', 
                                  lto_req_type       = 'DIV', 
                                  lto_start_datetime = '%', 
                                  lto_status         = 'RDY',
                                  limit_one          = True
                                  )
        
        if todo_list != []:
            
            for inv_ticker, freq_type, req_type, start_date, end_date in todo_list:
                
                timenow = datetime.datetime.today()
                
                if timenow >= self.stoptime:
                    sys.exit()
                        
                print('****** Starting Dividend DataLoad- ', time.strftime('%l:%M:%S on %b %d, %Y'),' for ',inv_ticker, freq_type, req_type, start_date, end_date)
               
                fmt_start_date           = datetime.datetime.strftime(start_date,"%Y%m%d%H%M%S")
                fmt_end_date             = datetime.datetime.strftime(end_date,"%Y%m%d%H%M%S")
                div_id                   = "%s$%s$%s$%s$%s" % (inv_ticker, freq_type, req_type, start_date, end_date)
               
                inv_ticker_no_exc_symbol = get_ticker(inv_ticker)
                exchange                 = get_exchange(inv_ticker)
                            
                self.req_id += 1
                
                if exchange == "TO":
                             
                    self.reqFundamentalData(self.req_id, ContractSamples.TSXStock(inv_ticker_no_exc_symbol), "ReportsFinSummary", [])
                    new_dict1 = {self.req_id : [div_id, 'DIV', False, self.freq_type]}
                    
                elif exchange == "NYSE":    
                    self.reqFundamentalData(self.req_id, ContractSamples.NYSEStock(inv_ticker_no_exc_symbol), "ReportsFinSummary", [])
                    new_dict1 = {self.req_id : [div_id, 'DIV', False, self.freq_type]}
                else:
                     print("Invalid exchange ", exchange, " for ticker ", inv_ticker, " ignoring record ")
                       
                self.progress_store.update(new_dict1)
                original_tracking_list = self.tracking_store.get(div_id)
                new_list = []
                if original_tracking_list == None:
                    new_list = {div_id : [self.req_id]}
                else:
                    original_tracking_list.append(self.req_id)
                    new_list = {div_id :original_tracking_list} 
                self.tracking_store.update(new_list)
        
                # record this request is now in progress
                oldStatus = '%'
                newStatus = 'WIP'
                result = set_todo_status(database           = DB,
                                         lto_inv_ticker     = inv_ticker, 
                                         lto_freq_type      = freq_type, 
                                         lto_req_type       = req_type, 
                                         lto_start_datetime = start_date, 
                                         old_lto_status     = oldStatus, 
                                         new_lto_status     = newStatus) 
    
        else:
            print('***************************************************************************************************************')
            print('********** Went to load_todos and found no DIV requests waiting to be processed so now we should end **********')
            print('**************************************** Going to wait ',IB_API_SLEEP_BEFORE_EXIT,' then will exit ***********************************')
            print('***************************************************************************************************************')
            
            time.sleep(IB_API_SLEEP_BEFORE_EXIT)
            
            sys.exit()   
        

    @printWhenExecuting
    def fundamentalsOperations_cancel(self):

        self.cancelFundamentalData(8001)
        # ! [cancelfundamentalexamples]

    @iswrapper
    # ! [fundamentaldata]
    def fundamentalData(self, reqId: TickerId, data: str):
        super().fundamentalData(reqId, data)
        #print("FundamentalData. ReqId:", reqId, "Data:", data)
        
        global DB
        
        #initialize all of the variables first
        inv_ticker = ''
        start_datetime = ''
        end_datetime = ''
        freq_type = ''
        
        # from tracking store, identify the request type using the reqId
        this_tracking_rec = self.progress_store.get(reqId)
        
        div_id = this_tracking_rec[0]
        
        # change delimiter to dollar sign rather than period
        #inv_ticker = div_id.split('$')[0]
        #inv_exc_symbol = div_id.split('$')[1]
        
        inv_ticker = div_id.split('$')[0]
        freq_type  = div_id.split('$')[1]
        req_type   = div_id.split('$')[2]
        start_date = div_id.split('$')[3]
        end_date   = div_id.split('$')[4]
                
        #req_type = this_tracking_rec[1]
        #freq_type = this_tracking_rec[3]
        
        # amended this section 22-01-2012 to save ALL dividend types        
        root = ET.fromstring(data)
        for child1 in root:
            
            if child1.tag == 'DividendPerShares':
                
                #number_of_A = 0
                #number_of_TTM = 0
                for child2 in child1:
                    asofDate = child2.get('asofDate')
                    reportType = child2.get('reportType')
                    period = child2.get('period')
                    dividendPerShare = child2.text
                    
                    new_ims_dividend = ImsDividendDB(database        = DB,
                                                     div_inv_ticker  = inv_ticker, 
                                                     div_report_type = reportType, 
                                                     div_as_of_date  = asofDate, 
                                                     div_exdiv_date  = '',
                                                     div_period      = period, 
                                                     div_per_share   = dividendPerShare)
                    new_ims_dividend.insert_DB()
        
        # update the progress store to True to show this reqId was completed
        self.progress_store[reqId][2] = True
        
        if self.number_active_requests > 0:
            self.number_active_requests -= 1
        else:
            print('about to reduce number active requests but already 0')
       
        #ldo_start_date = datetime.datetime.strptime(this_start_date,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        #ldo_end_date = datetime.datetime.strptime(this_end_date,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        date_loaded = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        
        load_done_data = ImsLoadDoneDB(database            = DB,
                                       ldo_inv_ticker      = inv_ticker,
                                       ldo_freq_type       = freq_type,
                                       ldo_req_type        = req_type,
                                       ldo_start_datetime  = start_date,
                                       ldo_end_datetime    = end_date,
                                       ldo_datetime_loaded = date_loaded
                                       )
        
        result = load_done_data.select_DB()
        if result == None:
            load_done_data.insert_DB()
        else:
            load_done_data.update_DB()
   
        # remove the DATALOAD_DATA_TODOS record
        delete_load_todo(database           = DB,
                         lto_inv_ticker     = inv_ticker, 
                         lto_freq_type      = freq_type, 
                         lto_req_type       = req_type, 
                         lto_start_datetime = start_date)

        print('****** Finished writing - ', time.strftime('%l:%M:%S on %b %d, %Y'),' Saved to loaded_data & removed from data_todos for',div_id)       
        print(' ')
        
        # Now re-call to request process to call the next      
        print('Creating a new request after finishing writing')
        self.fundamentalsOperations_req()
        
                            
    # ! [fundamentaldata]

    @printWhenExecuting
    def bulletinsOperations_req(self):
        
        print('in bulletinsOperations_req')
 
 
    @printWhenExecuting
    def bulletinsOperations_cancel(self):
        
        print('in bulletinsOperations_cancel')
        

    @iswrapper
    # ! [updatenewsbulletin]
    def updateNewsBulletin(self, msgId: int, msgType: int, newsMessage: str,
                           originExch: str):
        super().updateNewsBulletin(msgId, msgType, newsMessage, originExch)
        
        print('in updateNewsBulletin')
        
        
    def ocaSample(self):
        
        print('in ocaSample')
        
        
    def conditionSamples(self):
        
        print('in conditionSamples')
        

    def bracketSample(self):
       
        print('in bracketSample')
        

    def hedgeSample(self):
        
        print('in hedgeSample')
        

    def algoSamples(self):
        
        print('in algoSamples')
 
        
    @printWhenExecuting
    def financialAdvisorOperations(self):
        
        print('in financialAdvisorOperations')
        
    @iswrapper
    # ! [receivefa]
    def receiveFA(self, faData: FaDataType, cxml: str):
        super().receiveFA(faData, cxml)
        
        print('in receiveFA')
        

    @iswrapper
    # ! [softDollarTiers]
    def softDollarTiers(self, reqId: int, tiers: list):
        super().softDollarTiers(reqId, tiers)
        
        print('in softDollarTiers')
        

    @printWhenExecuting
    def miscelaneousOperations(self):
        
        print('in miscelaneousOperations')
        
        
    @printWhenExecuting
    def linkingOperations(self):
        
        print('in linkingOperations')
        
        
    @iswrapper
    # ! [displaygrouplist]
    def displayGroupList(self, reqId: int, groups: str):
        super().displayGroupList(reqId, groups)
        
        print('in displayGroupList')
        

    @iswrapper
    # ! [displaygroupupdated]
    def displayGroupUpdated(self, reqId: int, contractInfo: str):
        super().displayGroupUpdated(reqId, contractInfo)
       
        print('in displayGroupUpdated')
        
    @printWhenExecuting
    def whatIfOrderOperations(self):
    
        print('in whatIfOrderOperations')
        

    @printWhenExecuting
    def orderOperations_req(self):
       
        print('in orderOperations_req')
        

    def orderOperations_cancel(self):
        
        print('in orderOperations_cancel')
        

    def rerouteCFDOperations(self):
        
        print('in rerouteCFDOperations')
        

    def marketRuleOperations(self):
        
        print('in marketRuleOperations')
        

    @iswrapper
    # ! [execdetails]
    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        
        print('in execDetails')
        
    @iswrapper
    # ! [execdetailsend]
    def execDetailsEnd(self, reqId: int):
        super().execDetailsEnd(reqId)
        
        print('in execDetailsEnd')
        

    @iswrapper
    # ! [commissionreport]
    def commissionReport(self, commissionReport: CommissionReport):
        super().commissionReport(commissionReport)
        
        print('in commissionreport')
    
        
    @iswrapper
    # ! [currenttime]
    def currentTime(self, time:int):
        super().currentTime(time)
       
        print('in currentTime')
        

    @iswrapper
    # ! [completedorder]
    def completedOrder(self, contract: Contract, order: Order,
                  orderState: OrderState):
        super().completedOrder(contract, order, orderState)
        
        print('in completedOrder')
        

    @iswrapper
    # ! [completedordersend]
    def completedOrdersEnd(self):
        super().completedOrdersEnd()
        
        print('in completedOrdersEnd')
   

def agent_action_dividend_todos():
    SetupLogger()
    logging.debug("now is %s", datetime.datetime.now())
    logging.getLogger().setLevel(logging.ERROR)

    cmdLineParser = argparse.ArgumentParser("api tests")
    # cmdLineParser.add_option("-c", action="store_True", dest="use_cache", default = False, help = "use the cache")
    # cmdLineParser.add_option("-f", action="store", type="string", dest="file", default="", help="the input file")
    
    # SEP 2019 MW changed the port
    
    cmdLineParser.add_argument("-p", "--port", action="store", type=int, 
        dest="port", default = IB_API_PORT, help="The TCP port to use")
    cmdLineParser.add_argument("-C", "--global-cancel", action="store_true",
                               dest="global_cancel", default=False,
                               help="whether to trigger a globalCancel req")
    args = cmdLineParser.parse_args()
    print("Using args", args)
    logging.debug("Using args %s", args)
    # print(args)

    # enable logging when member vars are assigned
    from ibapi import utils
    Order.__setattr__ = utils.setattr_log
    Contract.__setattr__ = utils.setattr_log
    DeltaNeutralContract.__setattr__ = utils.setattr_log
    TagValue.__setattr__ = utils.setattr_log
    TimeCondition.__setattr__ = utils.setattr_log
    ExecutionCondition.__setattr__ = utils.setattr_log
    MarginCondition.__setattr__ = utils.setattr_log
    PriceCondition.__setattr__ = utils.setattr_log
    PercentChangeCondition.__setattr__ = utils.setattr_log
    VolumeCondition.__setattr__ = utils.setattr_log

    try:
        app = TestApp()
        if args.global_cancel:
            app.globalCancelOnly = True
        # ! [connect]
        the_port = IB_API_PORT
        print('about to try ', the_port, ' using ', IB_API_GATEWAY_IP)
        #app.connect(IB_API_GATEWAY_IP, the_port, clientId=0)
        app.connect(IB_API_GATEWAY_IP, the_port, clientId=IB_DIVIDEND_CLIENTID )
        # ! [connect]
        print("serverVersion:%s connectionTime:%s" % (app.serverVersion(),
                                                      app.twsConnectionTime()))

        # ! [clientrun]
        app.run()
        # ! [clientrun]
    except:
        raise
    finally:
        app.dumpTestCoverageSituation()
        app.dumpReqAnsErrSituation()


if __name__ == "__main__":
    
    # SEP 2019 MW database handling
    
    print("Started agent_action_dividend_todos")
    print(" ")
    print("Open db")
    print(" ")
    
    DB = open_db(host        = DB_HOST, 
                 port        = DB_PORT, 
                 tns_service = DB_TNS_SERVICE, 
                 user_name   = DB_USER_NAME, 
                 password    = DB_PASSWORD)

    agent_action_dividend_todos()
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = DB)  
'''
Created on Nov. 17, 2022

@author: dave
'''
