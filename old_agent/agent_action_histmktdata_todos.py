#------------------------------------------------------------
# Filename  : agent_action_histmktdata_todos.py
# Project   : ava
#
# Descr     : This holds routines relating to the IB API to get historic mkt data
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019-09-05   1 MW  Initial write based on TWS Program.py
# 2021-07-15   2 DW  Moved to ILS-ava and renamed to lto_start_datetime and lto_end_datetime
# 2021-08-25 100 DW  Added version 
# 2021-09-03 101 DW  Improved debug messages
# 2022-11-05 200 DW  Reorg
# 2022-11-18 201 DW  Reworked for ava
#------------------------------------------------------------


"""
Copyright (C) 2019 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
 and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""

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

from database.db_objects.ImsExchangeDB import get_ticker, get_exchange
from database.db_objects.ImsHistMktDataDB import insert_ask, insert_bid, insert_trades
from database.db_objects.ImsInvestmentDB import update_latest_price_date
from database.db_objects.ImsLoadDoneDB import ImsLoadDoneDB
from database.db_objects.ImsLoadTodoDB import delete_load_todo, get_load_todo, set_todo_status
from database.db_objects.ImsProcessControlDB import get_process_control_setting

import pandas as pd
from utils.utils_database import close_db, open_db
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD, HISTMKT_IN_PARALLEL
from utils.config import DEBUG, IB_API_PORT, IB_API_GATEWAY_IP, SLACK_BOT_TOKEN, IB_API_STOP_TIME_HR
from utils.config import IB_API_STOP_TIME_MIN, IB_API_STOP_TIME_SEC, IB_API_START_TIME_HR, IB_API_START_TIME_MIN
from utils.config import IB_API_START_TIME_SEC, IB_API_SLEEP_PERIOD, IB_API_SLEEP_BEFORE_EXIT
from utils.config import IB_HISTMKTDATA_CLIENTID

# keep track of number of lost connection errors
LOST_CONNECTION_ERRORS = ConnectionError()
DB                     = None     


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    #time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt  = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'
    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    #logging.basicConfig(filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
    logging.basicConfig(filename = time.strftime("log/TWS_hist_data_api.%y%m%d_%H%M%S.log"),
                        filemode = "w",
                        level    = logging.ERROR,
                        format   = recfmt, 
                        datefmt  = timefmt)
    
    logger  = logging.getLogger()
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
        self.reqMsdId    = reqMsgId
        self.ansMsgId    = ansMsgId
        self.ansEndMsgId = ansEndMsgId
        self.reqId       = reqId


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
        self.clntMeth2reqIdIdx  = collections.defaultdict(lambda: -1)
        self.reqId2nReq         = collections.defaultdict(int)
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
        self.wrapMeth2reqIdIdx  = collections.defaultdict(lambda: -1)
        self.reqId2nAns         = collections.defaultdict(int)
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
        self.nKeybInt         = 0
        self.started          = False
        self.nextValidOrderId = None
        self.permId2ord       = {}
        self.reqId2nErr       = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid   = None

        # SEP 2019 MW Added following for tracking requests and readying inserts to tables
        
        self.req_duration   = ''
        self.progress_store = {}
        self.tracking_store = {}
        self.ims_load_todos = {}
        
        self.hmd_inv_ticker           = ''
        self.hmd_start_datetime       = ''
        self.hmd_end_datetime         = ''
        self.hmd_freq_type            = ''
        self.hmd_start_bid_price      = ''
        self.hmd_highest_bid_price    = ''
        self.hmd_lowest_bid_price     = ''
        self.hmd_last_bid_price       = ''
        self.hmd_start_ask_price      = ''
        self.hmd_highest_ask_price    = ''
        self.hmd_lowest_ask_price     = ''
        self.hmd_last_ask_price       = ''
        self.hmd_first_traded_price   = ''
        self.hmd_highest_traded_price = ''
        self.hmd_lowest_traded_price  = ''
        self.hmd_last_traded_price    = ''
        self.hmd_total_traded_volume  = ''
        
        # 27 Dec 2019 Added so can correctly handle the different types
        self.rectype = ''
        
        self.df_ims_hist_mkt_data = pd.DataFrame()
        
        # initialize ready for the first set
        self.req_id = 0
        
        self.number_active_requests = 0
        print('initialized number active requests to zero')
        
        # work out 8:30pm and 9:30pm as times for comparison
        self.starttime = datetime.datetime.today().replace(hour=IB_API_START_TIME_HR, minute=IB_API_START_TIME_MIN, second=IB_API_START_TIME_SEC, microsecond=0)
        
        self.stoptime = datetime.datetime.today().replace(hour=IB_API_STOP_TIME_HR, minute=IB_API_STOP_TIME_MIN, second=IB_API_STOP_TIME_SEC, microsecond=0)
        
        # SEP 2019 MW End of additions
        
        
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
        if self.started:
            
            return

        self.started = True
        if self.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.reqGlobalCancel()
        else:
            
            print('properly started')
            
            newStatus = 'RDY'
            oldStatus = 'WIP'
            result = set_todo_status(database           = DB,
                                     lto_inv_ticker     = '%', 
                                     lto_freq_type      = '%', 
                                     lto_req_type       = 'HISTMKTDATA_%', 
                                     lto_start_datetime = '%', 
                                     old_lto_status     = oldStatus, 
                                     new_lto_status     = newStatus)
            print(' ')
            print('Set WIP HISTMKTDATA_% ims_load_todos record to RDY')   
                
            # loop for number of parallel requests with 2 second break in between to allow it to get started
            
            for counter in range(HISTMKT_IN_PARALLEL):
                self.historicalDataOperations_req()
                time.sleep(2)
            

    def keyboardInterrupt(self):
        self.nKeybInt += 1
        if self.nKeybInt == 1:
            self.stop()
        else:
            print("Finishing test")
            self.done = True


    def stop(self):
        print("Executing cancels")
        
        self.historicalDataOperations_cancel()
        
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
  
        # changed to ignore some messages that are not really errors
        # 2104 = market data farm connection is OK
        # 2106 = historical data farm connection is OK
        
        if (errorCode != 2104) and (errorCode != 2106) and (errorCode != 2107) and (errorCode != 366):
            
            print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)

            # before incrementing check how long ago the last connection error occurred
            # and if more than 20 minutes ago, reset the count to zero
            # only do the check if there were previously connection errors
                
            # 200 = No security definition has been found for this request
            # 162 = Historical Market Data Service Error  
            # 165 = Historical Market Data Service Error e.g. no such data in IB's database
            #if (errorCode == 200) or (errorCode == 162):
            if (errorCode == 200) or (errorCode == 162) or (errorCode == 165):
                
                # identify primary key of dataload_data_todos table then update this record
                # to set progress_status as 'ERR' in error
                this_rec = self.progress_store[reqId]
                request_type = this_rec[1]
                
                this_hmd_id = self.progress_store[reqId][0]
                error_message = 'Error ' + str(errorCode) + ' for request id ' + str(this_hmd_id)
                print(error_datetime, error_message)
                                
                # get the pk info for this hmd_id
                hmd_inv_ticker      = this_hmd_id.split('$')[0]
                hmd_freq_type       = this_hmd_id.split('$')[1]
                hmd_req_type        = this_hmd_id.split('$')[2]
                this_start_datetime = this_hmd_id.split('$')[3]
                this_end_datetime   = this_hmd_id.split('$')[4]
                hmd_start_datetime  = datetime.datetime.strptime(this_start_datetime,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                hmd_end_datetime    = datetime.datetime.strptime(this_end_datetime,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                
                oldStatus = 'WIP'
                result = set_todo_status(database           = DB,
                                         lto_inv_ticker     = hmd_inv_ticker, 
                                         lto_freq_type      = hmd_freq_type, 
                                         lto_req_type       = hmd_req_type, 
                                         lto_start_datetime = hmd_start_datetime, 
                                         old_lto_status     = oldStatus, 
                                         new_lto_status     = errorCode)
                
                print(error_datetime, 'Set this ims_load_todos record to In Error code ',str(errorCode), hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_datetime)   
                
                print(error_datetime, 'About to cancel reqid ',reqId, ' for ',hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_datetime)
                self.cancelHistoricalData(reqId)
                
                if self.number_active_requests > 0:
                    self.number_active_requests -= 1
                else:
                    print('was already zero - should not have happened')
                    
                print('after cancelling, number of active requests = ',self.number_active_requests)
                
                # update the progress store to True to show this reqId was completed
                self.progress_store[reqId][2] = True
        
                print(error_datetime, 'Cancelled reqid ', reqId, ' for ', hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_datetime)
                
                the_message = '%s * Dataload request cancel for request %3.0f for ticker %s type %s %s for date %s ' % (error_datetime, reqId, hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_datetime)
                
                print(error_datetime, 'Creating a new replacement request (BID)')
                self.historicalDataOperations_req()
                    
            else:
                
                #
                # Ignore certain types of error when incrementing lost connection error count 
                #
                
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
                
        # SEP 2019 MW End of our code
        
    # ! [error] self.reqId2nErr[reqId] += 1


    @iswrapper
    def winError(self, text: str, lastError: int):
        super().winError(text, lastError)


    @iswrapper
    # ! [openorder]
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        print("OpenOrder. PermId: ", order.permId, "ClientId:", order.clientId, " OrderId:", orderId, 
              "Account:", order.account, "Symbol:", contract.symbol, "SecType:", contract.secType,
              "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
              "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty, 
              "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status)

        order.contract = contract
        self.permId2ord[order.permId] = order
    # ! [openorder]


    @iswrapper
    # ! [openorderend]
    def openOrderEnd(self):
        super().openOrderEnd()
        print("OpenOrderEnd")

        logging.debug("Received %d openOrders", len(self.permId2ord))
    # ! [openorderend]


    @iswrapper
    # ! [orderstatus]
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
              lastFillPrice, "ClientId:", clientId, "WhyHeld:",
              whyHeld, "MktCapPrice:", mktCapPrice)
    # ! [orderstatus]


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
        self.reqMarketDataType(MarketDataTypeEnum.DELAYED_FROZEN)
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
        # Requesting historical data

        self.number_active_requests += 1
        # Need to work out the frequency type, duration and queryTime from items in the ims_load_todos table 
        # we will only return one request from get_load_todo

        todo_list = get_load_todo(database           = DB,
                                  lto_inv_ticker     = '%', 
                                  lto_freq_type      = '%', 
                                  lto_req_type       = 'HISTMKTDATA_%', 
                                  lto_start_datetime = '%', 
                                  lto_status         = 'RDY',
                                  limit_one          = True
                                  )
        
        if todo_list != []:
            #print(todo_list)
            for hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_datetime, hmd_end_datetime in todo_list:
    
                timenow = datetime.datetime.today()
                
                if timenow >= self.stoptime:
                    sys.exit()
                        
                print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'),' Starting Historic Market DataLoad for ', hmd_inv_ticker, hmd_freq_type, hmd_req_type, hmd_start_datetime, hmd_end_datetime)
                
                # common stuff for all types
                    
                num_days           = hmd_end_datetime - hmd_start_datetime
                self.req_duration  = str(num_days.days)  + ' D'
                self.hmd_freq_type = hmd_freq_type
                queryTime          = datetime.datetime.strftime(hmd_end_datetime,"%Y%m%d %H:%M:%S")
                    
                fmt_start_datetime       = datetime.datetime.strftime(hmd_start_datetime,"%Y%m%d%H%M%S")
                fmt_end_datetime         = datetime.datetime.strftime(hmd_end_datetime,"%Y%m%d%H%M%S")
                hmd_id                   = "%s$%s$%s$%s$%s" % (hmd_inv_ticker, hmd_freq_type, hmd_req_type, fmt_start_datetime, fmt_end_datetime)
                inv_ticker_no_exc_symbol = get_ticker(hmd_inv_ticker)
                
                # adding chartOptions as final new parameter and changing keepUpToDate value (from None to False) 22-May-2018
                chartOptions = []
                keepUpToDate = False
                        
                # only proceed if nums_days.days is positive
                    
                if num_days.days > 0:
                                
                    self.req_id += 1
                    
                    #
                    # Find the exchange
                    #
                    
                    exchange = get_exchange(hmd_inv_ticker)
                                                                
                    if hmd_req_type == 'HISTMKTDATA_BID':
    
                        if exchange == "TO":
                            self.reqHistoricalData(self.req_id, ContractSamples.TSXStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "BID", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id : [hmd_id, 'HISTMKTDATA_BID', False, self.hmd_freq_type]}
                        
                        elif exchange == "NYSE":
                            self.reqHistoricalData(self.req_id, ContractSamples.NYSEStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "BID", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_BID', False, self.hmd_freq_type]}
                        
                        elif exchange == "LSE":
                            self.reqHistoricalData(self.req_id, ContractSamples.LSEStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "BID", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_BID', False, self.hmd_freq_type]}
                        
                        elif exchange == "ASX":
                            self.reqHistoricalData(self.req_id, ContractSamples.ASXStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "BID", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_BID', False, self.hmd_freq_type]}
                        
                        elif exchange == "TSEJ":
                            self.reqHistoricalData(self.req_id, ContractSamples.TSEJStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "BID", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_BID', False, self.hmd_freq_type]}
                        
                        else:
                            print("Invalid exchange ", exchange, " for ticker ", hmd_inv_ticker, " ignoring record ")
                                                
                    elif hmd_req_type == 'HISTMKTDATA_ASK':
                        
                        if exchange == "TO":
                            self.reqHistoricalData(self.req_id, ContractSamples.TSXStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "ASK", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id : [hmd_id, 'HISTMKTDATA_ASK', False, self.hmd_freq_type]}
                            
                        elif exchange == "NYSE":
                            self.reqHistoricalData(self.req_id, ContractSamples.NYSEStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "ASK", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_ASK', False, self.hmd_freq_type]}

                        elif exchange == "LSE":
                            self.reqHistoricalData(self.req_id, ContractSamples.LSEStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "ASK", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_ASK', False, self.hmd_freq_type]}
                        
                        elif exchange == "ASX":
                            self.reqHistoricalData(self.req_id, ContractSamples.ASXStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "ASK", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_ASK', False, self.hmd_freq_type]}
                        
                        elif exchange == "TSEJ":
                            self.reqHistoricalData(self.req_id, ContractSamples.TSEJStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "ASK", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_ASK', False, self.hmd_freq_type]}
                        else:
                            print("Invalid exchange ", exchange, " for ticker ", hmd_inv_ticker, " ignoring record ")
                        
                    elif hmd_req_type == 'HISTMKTDATA_TRADES':
                        
                        if exchange == "TO":
                            self.reqHistoricalData(self.req_id, ContractSamples.TSXStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "TRADES", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id : [hmd_id, 'HISTMKTDATA_TRADES', False, self.hmd_freq_type]}
 
                        elif exchange == "NYSE":
                            self.reqHistoricalData(self.req_id, ContractSamples.NYSEStock(inv_ticker_no_exc_symbol), queryTime, 
                                              self.req_duration, self.hmd_freq_type, "TRADES", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_TRADES', False, self.hmd_freq_type]}

                        elif exchange == "LSE":
                            self.reqHistoricalData(self.req_id, ContractSamples.LSEStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "TRADES", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_TRADES', False, self.hmd_freq_type]}
                        
                        elif exchange == "ASX":
                            self.reqHistoricalData(self.req_id, ContractSamples.ASXStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "TRADES", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_TRADES', False, self.hmd_freq_type]}
                        
                        elif exchange == "TSEJ":
                            self.reqHistoricalData(self.req_id, ContractSamples.TSEJStock(inv_ticker_no_exc_symbol), queryTime, 
                                                   self.req_duration, self.hmd_freq_type, "TRADES", 1, 1, keepUpToDate, chartOptions)
                            new_dict1 = {self.req_id :  [hmd_id, 'HISTMKTDATA_TRADES', False, self.hmd_freq_type]}
                        
                        else:
                            print("Invalid exchange ", exchange, " for ticker ", hmd_inv_ticker, " ignoring record ")
                   
                    else:
                        print('some other type', hmd_req_type)    
                        
                    self.progress_store.update(new_dict1)
                    
                    original_tracking_list = self.tracking_store.get(hmd_id)
                    new_list               = []
                    
                    if original_tracking_list == None:
                        new_list = {hmd_id : [self.req_id]}
                    else:
                        original_tracking_list.append(self.req_id)
                        new_list = {hmd_id :original_tracking_list} 
                    self.tracking_store.update(new_list)
                         
                    # record this request is now in progress
                    
                    oldStatus = '%'
                    newStatus = 'WIP'
                    result = set_todo_status(database           = DB,
                                             lto_inv_ticker     = hmd_inv_ticker, 
                                             lto_freq_type      = hmd_freq_type, 
                                             lto_req_type       = hmd_req_type, 
                                             lto_start_datetime = hmd_start_datetime, 
                                             old_lto_status     = oldStatus, 
                                             new_lto_status     = newStatus) 
    
        else:
            print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), 'Went to load_todos and found no HISTMKTDATA_% requests waiting to be processed')
            print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), 'Checking process control to see if we should completely stop or wait and re-try')
            
            next_step = get_process_control_setting(database = DB,
                                                    pcn_process_name = 'AGENT_ACTION_HISTMKTDATA_TODOS', 
                                                    pcn_server_name  = DB_HOST)[0]
            
            #print(next_step)
            
            if next_step == 'RUN':
                
                print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), ' Going to wait ',IB_API_SLEEP_BEFORE_EXIT,'seconds then will try again')
                time.sleep(IB_API_SLEEP_BEFORE_EXIT)
                self.historicalDataOperations_req()
                
            else:
               
                print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), ' Going to stop immediately ')
            
                time.sleep(IB_API_SLEEP_BEFORE_EXIT)
                sys.exit()
            
        
        # ! [reqhistoricaldata]

    @printWhenExecuting
    def historicalDataOperations_cancel(self):
       print('in historicalDataOperations_cancel')

    @printWhenExecuting
    def historicalTicksOperations(self):
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
        
        #initialize all of the variables first
        hmd_inv_ticker = ''
        hmd_start_datetime = ''
        hmd_end_datetime = ''
        hmd_freq_type = ''
        hmd_start_bid_price = ''
        hmd_highest_bid_price = ''
        hmd_lowest_bid_price = ''
        hmd_last_bid_price = ''
        hmd_start_ask_price = ''
        hmd_highest_ask_price = ''
        hmd_lowest_ask_price = ''
        hmd_last_ask_price = ''
        hmd_first_traded_price = ''
        hmd_highest_traded_price = ''
        hmd_lowest_traded_price = ''
        hmd_last_traded_price = ''        
        hmd_total_traded_volume = ''
        rectype = ''
        
        # from tracking store, identify the request type using the reqId
        this_tracking_rec = self.progress_store.get(reqId)
        
        hmd_id = this_tracking_rec[0]
        
        # change delimiter to dollar sign rather than period
        hmd_inv_ticker = hmd_id.split('$')[0]
        hmd_inv_exc_symbol = hmd_id.split('$')[1]
        
        hmd_start_datetime = (datetime.datetime.strptime(bar.date,"%Y%m%d  %H:%M:%S")).strftime("%Y-%m-%d %H:%M:%S")
        index_start_dt = (datetime.datetime.strptime(bar.date,"%Y%m%d  %H:%M:%S")).strftime("%Y%m%d%H%M")
        
        if self.hmd_freq_type == '1 min':
            hmd_end_datetime = (datetime.datetime.strptime(bar.date,"%Y%m%d %H:%M:%S") + datetime.timedelta(seconds=59)).strftime("%Y-%m-%d %H:%M:%S")
            index_end_dt = (datetime.datetime.strptime(bar.date,"%Y%m%d %H:%M:%S") + datetime.timedelta(seconds=59)).strftime("%Y%m%d%H%M")
        elif self.hmd_freq_type == '5 mins':
            hmd_end_datetime = (datetime.datetime.strptime(bar.date,"%Y%m%d %H:%M:%S") + datetime.timedelta(seconds=59)).strftime("%Y-%m-%d %H:%M:%S")
            index_end_dt = (datetime.datetime.strptime(date,"%Y%m%d %H:%M:%S") + datetime.timedelta(seconds=299)).strftime("%Y%m%d%H%M")
        elif self.hmd_freq_type == '15 mins':
            hmd_end_datetime = (datetime.datetime.strptime(bar.date,"%Y%m%d %H:%M:%S") + datetime.timedelta(seconds=59)).strftime("%Y-%m-%d %H:%M:%S")
            index_end_dt = (datetime.datetime.strptime(date,"%Y%m%d %H:%M:%S") + datetime.timedelta(seconds=899)).strftime("%Y%m%d%H%M")
              
        rectype = this_tracking_rec[1]
        hmd_freq_type = this_tracking_rec[3]
        
        df_temp = pd.DataFrame()
        
        # change delimiter to dollar sign rather than period
        full_index_col = hmd_inv_ticker + '$' + index_start_dt + '$' + index_end_dt
        
        if rectype == 'HISTMKTDATA_BID':
            hmd_start_bid_price   = bar.open
            hmd_highest_bid_price = bar.high
            hmd_lowest_bid_price  = bar.low
            hmd_last_bid_price    = bar.close
            
            # Don't know whether already exists in data frame or not so check first
            
            if len(self.df_ims_hist_mkt_data.index) > 0:
                
                if full_index_col in self.df_ims_hist_mkt_data.index:
                    
                    # already exists so just update 
                    
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_start_bid_price']   = hmd_start_bid_price   
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_highest_bid_price'] = hmd_highest_bid_price 
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_lowest_bid_price']  = hmd_lowest_bid_price 
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_last_bid_price']    = hmd_last_bid_price      
                    self.df_ims_hist_mkt_data.at[full_index_col,'rectype']               = rectype                  
            
                else:
                    
                    # doesn't exist so insert
                    this_dict = {'full_index_col':       [full_index_col],
                             'hmd_id':                   [hmd_id],
                             'hmd_inv_ticker':           [hmd_inv_ticker], 
                             'hmd_inv_exc_symbol':       [hmd_inv_exc_symbol], 
                             'hmd_start_datetime':       [hmd_start_datetime], 
                             'hmd_end_datetime':         [hmd_end_datetime], 
                             'hmd_freq_type':            [hmd_freq_type], 
                             'hmd_start_bid_price':      [hmd_start_bid_price], 
                             'hmd_highest_bid_price':    [hmd_highest_bid_price], 
                             'hmd_lowest_bid_price':     [hmd_lowest_bid_price], 
                             'hmd_last_bid_price':       [hmd_last_bid_price], 
                             'hmd_start_ask_price':      [0], 
                             'hmd_highest_ask_price':    [0], 
                             'hmd_lowest_ask_price':     [0], 
                             'hmd_last_ask_price':       [0], 
                             'hmd_first_traded_price':   [0], 
                             'hmd_highest_traded_price': [0], 
                             'hmd_lowest_traded_price':  [0], 
                             'hmd_last_traded_price':    [0],        
                             'hmd_total_traded_volume':  [0],
                             'rectype':                  [rectype]
                             }
                
                    df_temp = pd.DataFrame.from_dict(this_dict)
                    df_temp.set_index("full_index_col", inplace=True)
                    self.df_ims_hist_mkt_data = pd.concat([self.df_ims_hist_mkt_data, df_temp], axis = 0, sort=False)
                    
            else:
        
                # doesn't exist so insert
                    
                this_dict = {'full_index_col':           [full_index_col],
                             'hmd_id':                   [hmd_id],
                             'hmd_inv_ticker':           [hmd_inv_ticker], 
                             'hmd_inv_exc_symbol':       [hmd_inv_exc_symbol], 
                             'hmd_start_datetime':       [hmd_start_datetime], 
                             'hmd_end_datetime':         [hmd_end_datetime], 
                             'hmd_freq_type':            [hmd_freq_type], 
                             'hmd_start_bid_price':      [hmd_start_bid_price], 
                             'hmd_highest_bid_price':    [hmd_highest_bid_price], 
                             'hmd_lowest_bid_price':     [hmd_lowest_bid_price], 
                             'hmd_last_bid_price':       [hmd_last_bid_price], 
                             'hmd_start_ask_price':      [0], 
                             'hmd_highest_ask_price':    [0], 
                             'hmd_lowest_ask_price':     [0], 
                             'hmd_last_ask_price':       [0], 
                             'hmd_first_traded_price':   [0], 
                             'hmd_highest_traded_price': [0], 
                             'hmd_lowest_traded_price':  [0], 
                             'hmd_last_traded_price':    [0],        
                             'hmd_total_traded_volume':  [0],
                             'rectype':                  [rectype]
                             }
                
                df_temp = pd.DataFrame.from_dict(this_dict) 
                df_temp.set_index("full_index_col", inplace=True)
                self.df_ims_hist_mkt_data = pd.concat([self.df_ims_hist_mkt_data, df_temp], axis =0, sort=False)
                
        elif rectype == 'HISTMKTDATA_ASK':
            
            hmd_start_ask_price   = bar.open
            hmd_highest_ask_price = bar.high
            hmd_lowest_ask_price  = bar.low
            hmd_last_ask_price    = bar.close
            
            # Don't know whether already exists in data frame or not so check first
            
            if self.df_ims_hist_mkt_data.shape[0] > 0:
                
                if full_index_col in self.df_ims_hist_mkt_data.index:
                    
                    # already exists so just update
                    # replacing set_value with .at as set_value is deprecated
                    
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_start_ask_price']   = hmd_start_ask_price  
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_highest_ask_price'] = hmd_highest_ask_price
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_lowest_ask_price']  = hmd_lowest_ask_price
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_last_ask_price']    = hmd_last_ask_price
                    self.df_ims_hist_mkt_data.at[full_index_col,'rectype']               = rectype        
                else:
                    
                    # doesn't exist so insert
                    this_dict = {'full_index_col':       [full_index_col],
                             'hmd_id':                   [hmd_id],
                             'hmd_inv_ticker':           [hmd_inv_ticker], 
                             'hmd_inv_exc_symbol':       [hmd_inv_exc_symbol], 
                             'hmd_start_datetime':       [hmd_start_datetime], 
                             'hmd_end_datetime':         [hmd_end_datetime], 
                             'hmd_freq_type':            [hmd_freq_type], 
                             'hmd_start_bid_price':      [0], 
                             'hmd_highest_bid_price':    [0], 
                             'hmd_lowest_bid_price':     [0], 
                             'hmd_last_bid_price':       [0], 
                             'hmd_start_ask_price':      [hmd_start_ask_price], 
                             'hmd_highest_ask_price':    [hmd_highest_ask_price], 
                             'hmd_lowest_ask_price':     [hmd_lowest_ask_price], 
                             'hmd_last_ask_price':       [hmd_last_ask_price], 
                             'hmd_first_traded_price':   [0], 
                             'hmd_highest_traded_price': [0], 
                             'hmd_lowest_traded_price':  [0], 
                             'hmd_last_traded_price':    [0],        
                             'hmd_total_traded_volume':  [0],
                             'rectype':                  [rectype]
                             }
                    df_temp = pd.DataFrame.from_dict(this_dict) 
                    df_temp.set_index("full_index_col", inplace=True)
                    self.df_ims_hist_mkt_data = pd.concat([self.df_ims_hist_mkt_data, df_temp], axis = 0, sort=False)
                                        
            else:
            
                # doesn't exist so insert
                
                this_dict = {'full_index_col':           [full_index_col],
                             'hmd_id':                   [hmd_id],
                             'hmd_inv_ticker':           [hmd_inv_ticker], 
                             'hmd_inv_exc_symbol':       [hmd_inv_exc_symbol], 
                             'hmd_start_datetime':       [hmd_start_datetime], 
                             'hmd_end_datetime':         [hmd_end_datetime], 
                             'hmd_freq_type':            [hmd_freq_type], 
                             'hmd_start_bid_price':      [0], 
                             'hmd_highest_bid_price':    [0], 
                             'hmd_lowest_bid_price':     [0], 
                             'hmd_last_bid_price':       [0], 
                             'hmd_start_ask_price':      [hmd_start_ask_price], 
                             'hmd_highest_ask_price':    [hmd_highest_ask_price], 
                             'hmd_lowest_ask_price':     [hmd_lowest_ask_price], 
                             'hmd_last_ask_price':       [hmd_last_ask_price], 
                             'hmd_first_traded_price':   [0], 
                             'hmd_highest_traded_price': [0], 
                             'hmd_lowest_traded_price':  [0], 
                             'hmd_last_traded_price':    [0],        
                             'hmd_total_traded_volume':  [0],
                             'rectype':                  [rectype]
                             }   
                df_temp = pd.DataFrame.from_dict(this_dict) 
                df_temp.set_index("full_index_col", inplace=True)
                self.df_ims_hist_mkt_data = pd.concat([self.df_ims_hist_mkt_data, df_temp], axis = 0, sort=False)
                        
        elif rectype == 'HISTMKTDATA_TRADES': 
            
            hmd_first_traded_price   = bar.open
            hmd_highest_traded_price = bar.high
            hmd_lowest_traded_price  = bar.low
            hmd_last_traded_price    = bar.close        
            hmd_total_traded_volume  = bar.volume
            
            # Don't know whether already exists in data frame or not so check first
            
            if self.df_ims_hist_mkt_data.shape[0] > 0:
                
                if full_index_col in self.df_ims_hist_mkt_data.index:
                    
                    # already exists so just update
                    # replacing set_value with .at as set_value is deprecated
                    
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_first_traded_price']   = hmd_first_traded_price  
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_highest_traded_price'] = hmd_highest_traded_price
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_lowest_traded_price']  = hmd_lowest_traded_price
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_last_traded_price']    = hmd_last_traded_price 
                    self.df_ims_hist_mkt_data.at[full_index_col,'hmd_total_traded_volume']  = hmd_total_traded_volume 
                    self.df_ims_hist_mkt_data.at[full_index_col,'rectype']                  = rectype  
                            
                else:
                    # doesn't exist so insert
                    
                    this_dict = {'full_index_col':       [full_index_col],
                             'hmd_id':                   [hmd_id],
                             'hmd_inv_ticker':           [hmd_inv_ticker], 
                             'hmd_inv_exc_symbol':       [hmd_inv_exc_symbol], 
                             'hmd_start_datetime':       [hmd_start_datetime], 
                             'hmd_end_datetime':         [hmd_end_datetime], 
                             'hmd_freq_type':            [hmd_freq_type], 
                             'hmd_start_bid_price':      [0], 
                             'hmd_highest_bid_price':    [0], 
                             'hmd_lowest_bid_price':     [0], 
                             'hmd_last_bid_price':       [0], 
                             'hmd_start_ask_price':      [0], 
                             'hmd_highest_ask_price':    [0], 
                             'hmd_lowest_ask_price':     [0], 
                             'hmd_last_ask_price':       [0], 
                             'hmd_first_traded_price':   [hmd_first_traded_price], 
                             'hmd_highest_traded_price': [hmd_highest_traded_price], 
                             'hmd_lowest_traded_price':  [hmd_lowest_traded_price], 
                             'hmd_last_traded_price':    [hmd_last_traded_price],        
                             'hmd_total_traded_volume':  [hmd_total_traded_volume],
                             'rectype':                  [rectype]
                             }   
                    df_temp = pd.DataFrame.from_dict(this_dict)
                    df_temp.set_index("full_index_col", inplace=True)
                    self.df_ims_hist_mkt_data = pd.concat([self.df_ims_hist_mkt_data, df_temp], axis = 0, sort=False)
                    
                    
            else:
                # doesn't exist so insert
                
                this_dict = {'full_index_col':           [full_index_col],
                             'hmd_id':                   [hmd_id],
                             'hmd_inv_ticker':           [hmd_inv_ticker], 
                             'hmd_inv_exc_symbol':       [hmd_inv_exc_symbol], 
                             'hmd_start_datetime':       [hmd_start_datetime], 
                             'hmd_end_datetime':         [hmd_end_datetime], 
                             'hmd_freq_type':            [hmd_freq_type], 
                             'hmd_start_bid_price':      [0], 
                             'hmd_highest_bid_price':    [0], 
                             'hmd_lowest_bid_price':     [0], 
                             'hmd_last_bid_price':       [0], 
                             'hmd_start_ask_price':      [0], 
                             'hmd_highest_ask_price':    [0], 
                             'hmd_lowest_ask_price':     [0], 
                             'hmd_last_ask_price':       [0], 
                             'hmd_first_traded_price':   [hmd_first_traded_price], 
                             'hmd_highest_traded_price': [hmd_highest_traded_price], 
                             'hmd_lowest_traded_price':  [hmd_lowest_traded_price], 
                             'hmd_last_traded_price':    [hmd_last_traded_price],        
                             'hmd_total_traded_volume':  [hmd_total_traded_volume],
                             'rectype':                  [rectype]
                             }   
                df_temp = pd.DataFrame.from_dict(this_dict) 
                df_temp.set_index("full_index_col", inplace=True)
                self.df_ims_hist_mkt_data = pd.concat([self.df_ims_hist_mkt_data, df_temp], axis = 0, sort=False)
                

        else:
            print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), ' SOMETHING ELSE ???')
        
    # ! [historicaldata]

    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        
         # find out which ticker and exchange symbol this reqId is for
        this_hmd_id = self.progress_store[reqId][0]
        
        # update the progress store to True to show this reqId was completed
        self.progress_store[reqId][2] = True
        
        # decrement the count
        # increment the number of requests here
        
        # amended 8th December 2018 - only reduce if positive
        if self.number_active_requests > 0:
            self.number_active_requests -= 1
        else:
            print('about to reduce number active requests but already 0')
        
        
        # find out if all requests for this ticker and exchange symbol have been completed
        these_requests = self.tracking_store[this_hmd_id]
        
        any_requests_os = False
        
        for each_request in these_requests:
            if self.progress_store[each_request][2] == False:
                any_requests_os = True
                   
        
        # if all requests for ths ticker and exchange symbol have completed then can save to the database
        
        if any_requests_os == False:
             
            print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), ' About to save - ', time.strftime('%l:%M:%S on %b %d, %Y'),' for ', this_hmd_id)   
            
            for index, thisrow in self.df_ims_hist_mkt_data.iterrows():
                
                # only prepare the record if it matches the ticker/exchange symbol - ignore the others
                df_hmd_id = thisrow.hmd_id
                
                if df_hmd_id == this_hmd_id:
                    
                    this_rectype = thisrow.rectype
                    
                    if this_rectype == 'HISTMKTDATA_ASK':
                        insert_ask(database              = DB,
                                   hmd_inv_ticker        = thisrow.hmd_inv_ticker, 
                                   hmd_start_datetime    = thisrow.hmd_start_datetime, 
                                   hmd_end_datetime      = thisrow.hmd_end_datetime, 
                                   hmd_freq_type         = thisrow.hmd_freq_type, 
                                   hmd_start_ask_price   = thisrow.hmd_start_ask_price, 
                                   hmd_highest_ask_price = thisrow.hmd_highest_ask_price, 
                                   hmd_lowest_ask_price  = thisrow.hmd_lowest_ask_price, 
                                   hmd_last_ask_price    = thisrow.hmd_last_ask_price)
                        
                        
                    elif this_rectype == 'HISTMKTDATA_BID':
                        insert_bid(database              = DB,
                                   hmd_inv_ticker        = thisrow.hmd_inv_ticker, 
                                   hmd_start_datetime    = thisrow.hmd_start_datetime, 
                                   hmd_end_datetime      = thisrow.hmd_end_datetime, 
                                   hmd_freq_type         = thisrow.hmd_freq_type, 
                                   hmd_start_bid_price   = thisrow.hmd_start_bid_price, 
                                   hmd_highest_bid_price = thisrow.hmd_highest_bid_price, 
                                   hmd_lowest_bid_price  = thisrow.hmd_lowest_bid_price, 
                                   hmd_last_bid_price    = thisrow.hmd_last_bid_price)
                        
                        
                    elif this_rectype == 'HISTMKTDATA_TRADES':
                        insert_trades(database                 = DB,
                                      hmd_inv_ticker           = thisrow.hmd_inv_ticker, 
                                      hmd_start_datetime       = thisrow.hmd_start_datetime, 
                                      hmd_end_datetime         = thisrow.hmd_end_datetime, 
                                      hmd_freq_type            = thisrow.hmd_freq_type, 
                                      hmd_first_traded_price   = thisrow.hmd_first_traded_price, 
                                      hmd_highest_traded_price = thisrow.hmd_highest_traded_price, 
                                      hmd_lowest_traded_price  = thisrow.hmd_lowest_traded_price, 
                                      hmd_last_traded_price    = thisrow.hmd_last_traded_price, 
                                      hmd_total_traded_volume  = thisrow.hmd_total_traded_volume)                        
                          
                    # now remove from the data frame rows for this ticker/exchange symbol
                    self.df_ims_hist_mkt_data.drop(index, inplace = True) 
                
                    
            # get the pk info for this ddt_id
            ldo_inv_ticker      = this_hmd_id.split('$')[0]
            ldo_freq_type       = this_hmd_id.split('$')[1]
            ldo_req_type        = this_hmd_id.split('$')[2]
            this_start_datetime = this_hmd_id.split('$')[3]
            this_end_datetime   = this_hmd_id.split('$')[4]
            
            ldo_start_datetime  = datetime.datetime.strptime(this_start_datetime,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
            ldo_end_datetime    = datetime.datetime.strptime(this_end_datetime,"%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
            ldo_datetime_loaded = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            
            load_done_data = ImsLoadDoneDB(database            = DB,
                                           ldo_inv_ticker      = ldo_inv_ticker,
                                           ldo_freq_type       = ldo_freq_type,
                                           ldo_req_type        = ldo_req_type,
                                           ldo_start_datetime  = ldo_start_datetime,
                                           ldo_end_datetime    = ldo_end_datetime,
                                           ldo_datetime_loaded = ldo_datetime_loaded
                                           )
            
            result = load_done_data.select_DB()
            
            if result == None:
                load_done_data.insert_DB()
            else:
                load_done_data.update_DB()
            
            
            # remove the DATALOAD_DATA_TODOS record
            delete_load_todo(database           = DB,
                             lto_inv_ticker     = ldo_inv_ticker, 
                             lto_freq_type      = ldo_freq_type, 
                             lto_req_type       = ldo_req_type, 
                             lto_start_datetime = ldo_start_datetime)
            
            
            # now update the IMS_investment record to record just updated this ticker's prices
                    
            update_latest_price_date(database                  = DB,
                                     inv_ticker                = ldo_inv_ticker, 
                                     inv_latest_price_datetime = ldo_end_datetime)
            
            print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), time.strftime('%l:%M:%S on %b %d, %Y'),' Saved to loaded_data & removed from data_todos for ',this_hmd_id)       
        
            # Now re-call to request process to call the next      
            print('agent_action_histmktdata_todos.py - ', time.strftime('%l:%M:%S on %b %d, %Y'), ' Creating a new request after finishing writing')
            self.historicalDataOperations_req()

    # ! [historicaldataend]

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
        print('in newsOperations_req')

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
        print('in fundamentalsOperations_req')

    @printWhenExecuting
    def fundamentalsOperations_cancel(self):
        print('in fundamentalsOperations_cancel')

    @iswrapper
    # ! [fundamentaldata]
    def fundamentalData(self, reqId: TickerId, data: str):
        super().fundamentalData(reqId, data)
        print('in fundamentalData')

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
        print('in commissionReport')

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


def agent_action_histmktdata_todos():
    SetupLogger()
    logging.debug("now is %s", datetime.datetime.now())
    logging.getLogger().setLevel(logging.ERROR)

    cmdLineParser = argparse.ArgumentParser("api tests")
    
    cmdLineParser.add_argument("-p", "--port", action="store", type=int, 
        dest="port", default = IB_API_PORT, help="The TCP port to use")
    cmdLineParser.add_argument("-C", "--global-cancel", action="store_true",
                               dest="global_cancel", default=False,
                               help="whether to trigger a globalCancel req")
    args = cmdLineParser.parse_args()
    print("Using args", args)
    logging.debug("Using args %s", args)
    

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
        app.connect(IB_API_GATEWAY_IP, the_port, clientId=IB_HISTMKTDATA_CLIENTID)
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
    
    print("Started agent_action_histmktdata_todos")
    print(" ")
    print("Open db")
    print(" ")
   
    DB = open_db(host        = DB_HOST, 
                 port        = DB_PORT, 
                 tns_service = DB_TNS_SERVICE, 
                 user_name   = DB_USER_NAME, 
                 password    = DB_PASSWORD)
    
    agent_action_histmktdata_todos()
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = DB)  
