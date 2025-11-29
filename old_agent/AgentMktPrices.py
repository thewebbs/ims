#------------------------------------------------------------
# Filename  : AgentMktPrices.py
# Project   : ava
#
# Descr     : This holds routines relating to the Market Prices Agent class
#   
# Params    : None
#         
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-09-17   1 DW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import IB_API_PORT, IB_API_GATEWAY_IP

from ibapi.client   import EClient
from ibapi.wrapper  import EWrapper
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum


#
# IB API class
#

class IBApiApp(EWrapper, EClient):
    
    #
    # override the methods we want to use
    #
    
    def __init__(self):
        
        EClient.__init__(self, self)
    
        return 
    
     
    def error(self, reqId, errorCode, errorString) :
        
        print("Error: ", reqId, " ", errorCode, " ", errorString)
        
        return 
    
    
    def tickPrice(self, reqId, tickType, price, attrib):
        
        print ("Tick Price. Ticker ID:", reqId, "tickType:", TickTypeEnum.to_str(tickType), "Price:", price, end=' ')
        
        return


    def tickSize(self, reqId, tickType, size):
        
        print("Tick Size. Ticker ID:", reqId, "tickType:", TickTypeEnum.to_str(tickType), "Size:", size)
        
        return 
  
#
# Class definition
#

class AgentMktPrices:
    
    def __init__(self, mp_uid, mp_ticker, mp_exchange, mp_cty_symbol, mp_frequency):
        
        self.mp_uid        = mp_uid
        self.mp_ticker     = mp_ticker
        self.mp_exchange   = mp_exchange
        self.mp_cty_symbol = mp_cty_symbol
        self.mp_frequency  = mp_frequency
        
        return 
    
    #
    # Get methods
    #
    
    # Generic methods that should exist for all agents
    
    def get_uid(self):
        
        #
        # 
        result = self.mp_uid
        
        return result
    
    
    # Specific methods for AgentMktPrices

    def get_mp_uid(self):
        
        result = self.mp_uid
        
        return result
    
    
    def get_mp_ticker(self):
        
        result = self.mp_ticker
        
        return result

    
    def get_mp_exchange(self):
        
        result = self.mp_exchange
        
        return result


    def get_mp_cty_symbol(self):
        
        result = self.mp_cty_symbol
        
        return result


    def get_mp_frequency(self):
        
        result = self.mp_frequency
        
        return result
 
   
    #
    # Set methods
    #
    
    # Generic methods that should exist for all agents
    
    def set_uid(self, uid):
        
        self.mp_uid = uid
        
        return
 

    # Specific methods for AgentMktPrices
 
    def set_mp_uid(self, mp_uid):
        
        self.hp_uid = mp_uid
        
        return


    def set_mp_ticker(self, mp_ticker):
        
        self.mp_ticker = mp_ticker
        
        return

   
    def set_mp_exchange(self, mp_exchange):
        
        self.mp_exchange = mp_exchange
        
        return


    def set_mp_cty_symbol(self, mp_cty_symbol):
        
        self.mp_cty_symbol = mp_cty_symbol
        
        return
    

    def set_mp_frequency(self, mp_frequency):
        
        self.mp_frequency = mp_frequency
        
        return

    
    #
    # Other methods
    #
    '''
    def start(self):
        
        app = IBApiApp()
        app.connect(IB_API_GATEWAY_IP, IB_API_PORT, 0)
    
        contract                 = Contract()
        contract.symbol          = "GOOG"
        contract.secType         = "STK"
        contract.exchange        = "SMART"
        contract.currency        = "USD"
        contract.primaryExchange = "NASDAQ" 
        
        app.reqMarketDataType(4) # Switch to delayed-frozen data if live not available
        app.reqMktData(1, contract, "", False, False, [])
        
        app.run()
        
        return
    '''

    def display(self):
        
        #
        # Display the state of the agent
        #
    
        print('AgentMktPrices : ', self.mp_uid)
        print('---------------')
        print('Ticker         : ', self.mp_ticker)
        print('Exchange       : ', self.mp_exchange)
        print('Currency       : ', self.mp_cty_symbol)
        print('Frequency      : ', self.mp_frequency)
            
        return

    
    def __repr__(self):
        
        return self.mp_uid
    
    
    def __str__(self): 
        
        return self.mp_uid
   

    def __unicode__(self): 
        
        return self.mp_uid
    
    
def main():
    
    app = IBApiApp()
    app.connect(IB_API_GATEWAY_IP, IB_API_PORT, 0)
    
    contract                 = Contract()
    contract.symbol          = "GOOG"
    contract.secType         = "STK"
    contract.exchange        = "SMART"
    contract.currency        = "USD"
    contract.primaryExchange = "NASDAQ" 
    
    app.reqMarketDataType(4) # Switch to delayed-frozen data if live not available
    app.reqMktData(100, contract, "", False, False, [])
    
    contract2                 = Contract()
    contract2.symbol          = "AAPL"
    contract2.secType         = "STK"
    contract2.exchange        = "SMART"
    contract2.currency        = "USD"
    contract2.primaryExchange = "NASDAQ" 

    app.reqMktData(200, contract2, "", False, False, [])
    
    app.run()
    
    
if __name__ == "__main__":
    main()
    