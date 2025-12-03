#------------------------------------------------------------
# Filename  : ava.py
# Project   : ava
#
# Descr     : Top Level Control Program to instantiate the IB API object, 
#             instantiate the Trade agent object and begin to trade 
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

import time
from ava_agents.agent_ib_api import cleanup_environment, get_IB_stockprice_data, setup_environment
from ava_agents.agent_trade import AgentTrade, show_holding
import asyncio

async def check_status(tradeagent):
    await asyncio.sleep(10)
    show_holding(tradeagent)
    
async def ava():
  
    units_held = 0
    average_price = 0
    target_profit = 30
    
    # set up the environment we need  
    app = setup_environment()
    
    print('creating tradeagent1')
    tradeagent1 = AgentTrade(at_ticker        = 'CM', 
                            at_units_held     = units_held, 
                            at_average_price  = average_price, 
                            at_target_profit  = target_profit,
                            at_last_bid_price = 0,
                            at_last_ask_price = 0)
    
    print('get_IB_stockprice_data for tradeagent1')
    get_IB_stockprice_data(app=app, 
                           inv_ticker='CM', 
                           inv_exc_symbol='NYSE',
                           tradeagent = tradeagent1)
    
    
    '''
    print('creating tradeagent2')
    tradeagent2 = AgentTrade(at_ticker        = 'RY', 
                            at_units_held     = units_held, 
                            at_average_price  = average_price, 
                            at_target_profit  = target_profit,
                            at_last_bid_price = 0,
                            at_last_ask_price = 0)
    
    print('get_IB_stockprice_data for tradeagent2')
    get_IB_stockprice_data(app=app, 
                           inv_ticker='RY', 
                           inv_exc_symbol='NYSE',
                           tradeagent = tradeagent2)
    
    '''
    
    for counter in range(10):
        #await asyncio.gather(check_status(tradeagent1), check_status(tradeagent2))
        await asyncio.gather(check_status(tradeagent1))
        
    
    
    # final status
    print("===========")
    print("at very end")    
    show_holding(tradeagent1)
    #show_holding(tradeagent2)
    print("cleaning up")
    print("===========")
    
    # clean up
    cleanup_environment(app)
       
    
    return



if __name__ == '__main__':

    print ("Starting ava")    
    print(' ')
    
    asyncio.run(ava())
    
