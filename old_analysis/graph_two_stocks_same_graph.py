#------------------------------------------------------------
# Filename  : graph_two_stocks_same_graph.py
# Project   : ava
#
# Descr     : This holds routine to graph prices for a given day 
#             for two stocks
#
# Params    : database
#             inv_ticker1
#             inv_ticker2
#             start_datetime
#             end_datetime
#             freq_type
#             display_to_screen
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-08-04   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from database.db_objects.ImsPriceComparisonDB import ImsPriceComparisonDB
from database.db_objects.ImsHistMktDataDB import get_hist_mkt_data_in_range
import matplotlib.pyplot as plt
import pandas as pd

from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.config import VISUAL_LEGEND_BBOX_TO_ANCHOR, REPORT_FOLDER_OSX
from utils.config import VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS, VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR
from utils.config import VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE, VISUAL_RED_LINE_COLOR
from utils.config import VISUAL_XAXIS_COLOR, VISUAL_XAXIS_ROTATION

from utils.utils_database import close_db, open_db
from utils.utils_dataframes import fixit_df, normalize_df


def graph_two_stocks_same_graph(database, inv_ticker1, inv_ticker2, start_datetime, end_datetime, freq_type, display_to_screen):
       
    prices_df1 = get_price_lists(database, inv_ticker1, start_datetime, end_datetime, freq_type)
    prices_df2 = get_price_lists(database, inv_ticker2, start_datetime, end_datetime, freq_type)
    
    prices1_changedindex_df = prices_df1.rename(columns={inv_ticker1: 'INV'})
    prices2_changedindex_df = prices_df2.rename(columns={inv_ticker2: 'INV'})
    
    new_prices_df1 = normalize_df(prices_df1)
    new_prices_df2 = normalize_df(prices_df2)
    
    new_prices1_changedindex_df = new_prices_df1.rename(columns={inv_ticker1: 'INV'})
    new_prices2_changedindex_df = new_prices_df2.rename(columns={inv_ticker2: 'INV'})
     
    # calculate the differences between the two dataframes to include on graph
    diff_df = new_prices1_changedindex_df - new_prices2_changedindex_df
    
    # now change the index to DIFF for displaying on the graph
    diff_changedindex_df = diff_df.rename(columns={'INV': 'DIFF'})
    
    # now graph the dataframes before changed the names plus the difference
    graph_results(new_prices_df1, new_prices_df2, diff_changedindex_df, display_to_screen)   

    # write this data to the database table
    create_price_comparison_rec(database, inv_ticker1, inv_ticker2, new_prices1_changedindex_df, new_prices2_changedindex_df)
                                
    
    return


def create_price_comparison_rec(database, inv_ticker1, inv_ticker2, prices_df1, prices_df2):
    
    #loop through the two dataframes
    for r in prices_df1.index:
        for c in prices_df1.columns:
            print('inv_ticker1', inv_ticker1)
            print('inv_ticker2', inv_ticker2)
            this_datetime = r
            print('this_datetime', this_datetime)
            price1 = prices_df1.at[r, c]
            print('price1', price1)
            price2 = prices_df2.at[r, c]
            print('price2', price2)
    
            # write this record to the database
            
            new_ims_price_comparison = ImsPriceComparisonDB(database         = database,
                                                            pco_inv_ticker1 = inv_ticker1,
                                                            pco_inv_ticker2 = inv_ticker2,
                                                            pco_datetime    = this_datetime,
                                                            pco_price1      = price1,
                                                            pco_price2      = price2)
            new_ims_price_comparison.insert_DB()
    
    return 

                
def get_price_lists(database, inv_ticker, start_datetime, end_datetime, freq_type):

    # get midpoint price data and build into price and date lists for processing

    hist_mkt_prices = []
    
    prices_df = pd.DataFrame()

    # note that in the database inv_ticker includes the exchange symbol

    hist_mkt_prices[:] = get_hist_mkt_data_in_range(database           = database,
                                                    hmd_inv_ticker     = inv_ticker,
                                                    hmd_start_datetime = start_datetime,
                                                    hmd_end_datetime   = end_datetime,
                                                    hmd_freq_type      = freq_type)

    for (start_datetime, bid_price, ask_price) in hist_mkt_prices:
        
        if bid_price == None:
            if ask_price == None:
                bid_price = 0
            else:
                bid_price = ask_price

        if ask_price == None:
            ask_price = 0
        
        # record price record in the prices dataframe which contains all records

        index_val = inv_ticker 
        format_date = start_datetime.strftime('%y-%m-%d %H:%M')
        temp_df = pd.DataFrame({format_date: (float(ask_price))}, index=[index_val] )

        
        prices_df = pd.concat([prices_df, temp_df], sort=False)

    prices_df = prices_df.groupby(prices_df.index).sum()
    if len(prices_df.index) > 0:
        prices_df = fixit_df(prices_df)

    return prices_df


def graph_results(prices1_df, prices2_df, diff_df, display_to_screen):

    # temporarily set the indexes the same so can subtract
    # graph the prices dataframe then save the figure

    plt.style.use(VISUAL_STYLE_TO_USE)

    fig = plt.figure(figsize=(20,10))
    ax1 = fig.add_subplot(111)

    plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
    plt.ylabel('Price', color=VISUAL_XAXIS_COLOR)       # this sets the axes title colour only

    # change title to use dollar threshold rather than streak percent
    
    maintitle = "Ask Prices for %s and %s between %s and %s" % (inv_ticker1, inv_ticker2, start_datetime, end_datetime)
    plt.suptitle(maintitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
    
    prices1_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
    prices2_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_RED_LINE_COLOR)
    diff_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = '#3359FF')

    
    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width * VISUAL_LEGEND_BBOX_WIDTH, box.height])
    ax1.legend(loc='center left', bbox_to_anchor=VISUAL_LEGEND_BBOX_TO_ANCHOR, ncol=VISUAL_LEGEND_COLS, fancybox=VISUAL_LEGEND_FANCYBOX, fontsize=VISUAL_LEGEND_FONT_SIZE)

    plt.grid(linestyle="dotted", color='grey', linewidth=0.5)

    # now save the graphs to the specified pathname
    
    disp_start_date = start_datetime[0:10]
    disp_end_date = end_datetime[0:10]
    
    output_file_name = "%sAsk_prices_%s_and_%s_between_%s_and_%s.png" % (REPORT_FOLDER_OSX, inv_ticker1, inv_ticker2, disp_start_date, disp_end_date )
    print(output_file_name)
    plt.savefig(output_file_name)

    # Only actually display if boolean says so

    if display_to_screen:
        plt.show()

    return



if __name__ == "__main__":

    print("Open db")
    print(" ")
 
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)

    
    start_datetime_list =  ('2020-05-04 00:00:00', '2020-05-06 00:00:00', '2020-05-07 00:00:00', '2020-05-27 00:00:00')
    end_datetime_list   =  ('2020-05-05 00:00:00', '2020-05-07 00:00:00', '2020-05-08 00:00:00', '2020-05-28 00:00:00')
    
    inv_ticker1_list = ('CM.TO',   'TD.TO',   'L.TO')
    inv_ticker2_list = ('CM.NYSE', 'TD.NYSE', 'L.NYSE')
    
    for ticker_count in range(len(inv_ticker1_list)):
        inv_ticker1 = inv_ticker1_list[ticker_count]
        inv_ticker2 = inv_ticker2_list[ticker_count]
        
        for datetime_count in range(len(start_datetime_list)):
            start_datetime = start_datetime_list[datetime_count]
            end_datetime   = end_datetime_list[datetime_count]
            
            graph_two_stocks_same_graph(database = database,
                                        inv_ticker1 = inv_ticker1,
                                        inv_ticker2 = inv_ticker2, 
                                        start_datetime = start_datetime,
                                        end_datetime = end_datetime,
                                        freq_type = '1 min',
                                        display_to_screen = False)
            
    
    
    print(" ")
    print("Close db")
    print(" ")
    
    close_db(database = database) 
    

