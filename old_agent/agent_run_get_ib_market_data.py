#------------------------------------------------------------
# Filename  : agent_run_get_ib_market_data.py
# Project   : ava
#
# Descr     : Program to request realtime pricing data from IB 
#             This is a demo program. Obviously need to re-work the architecture
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-05-07   1 DW  Initial write
# ...
# 2021-12-19 100 DW  Added version 
# 2022-11-05 200 DW  Reorg
# 2022-12-17 201 DW  chane TO to TSE
#------------------------------------------------------------


from time import sleep
from agents.agent_get_ib_market_data import cleanup_environment, get_IB_stockprice_data, setup_environment


def agent_run_get_ib_market_data():
  
    # set up the environment we need  
    
    app = setup_environment()
    
    get_IB_stockprice_data(app=app, inv_ticker='CM', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='RY', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='TD', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='BNS', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='ABX', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='BNP', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='CSH.UN', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='ECI', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='EDV', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='FR', inv_exc_symbol='TSE')
    
    get_IB_stockprice_data(app=app, inv_ticker='HR.UN', inv_exc_symbol='TSE')
    
    
    #
    # Pause for a bit
    #
        
    sleep(100000000)
    
    # clean up
    cleanup_environment(app)
       
    
    return



if __name__ == '__main__':

    print ("Starting agent_run_get_ib_market_data")    
    print(' ')
    
    agent_run_get_ib_market_data()
    print ("agent_run_get_ib_market_data")
    
