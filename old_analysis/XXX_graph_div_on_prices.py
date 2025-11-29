#------------------------------------------------------------
# Filename  : graph_div_on_prices.py
# Project   : ava
#
# Descr     : This file contains code to graph DividendDB dates on top of a graph of prices
#
# Params    : database
#             inv_ticker
#             inv_exc_symbol
#             start_date
#             end_date
#             produce_graph
#             display_to_screen
#             output_path_name
#             price_type_1
#             price_type_2
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2018-06-20   1 MW  Initial write 
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from database.db_objects.CandlestickDB import get_end_of_day_candlestick

from utils.config import DEBUG, STREAK_LOOKBACK_PERIOD, STREAK_LOOKBACK_THRESHOLD, VISUAL_COLOR_LIST, VISUAL_GRAPH_KIND, VISUAL_LEGEND_BBOX_TO_ANCHOR
from utils.config import VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS, VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR, VISUAL_MARKER_EDGE_COLOR
from utils.config import VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_MARKER_KIND, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE
from utils.config import VISUAL_2ND_TITLE_FONT_SIZE, VISUAL_XAXIS_COLOR, VISUAL_XAXIS_ROTATION, REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db
from decimal import Decimal
from database.db_objects.ImsDividendDB import get_dividends
import matplotlib.pyplot as plt
import pandas as pd
from utils.utils_dataframes import fixit_df


number_ups   = 0
number_downs = 0
number_flats = 0
profit       = 0


# Get methods

def build_price_df(database, inv_ticker, inv_exc_symbol, start_date, end_date, price_type):
   
    # get  price data and build into dataframe
        
    prices    = []
    freq_type = '1 min'
    prices_df = pd.DataFrame()
    
    # get different data depending on price_type
    
    prices[:] = candlestick_data = get_end_of_day_candlestick(database       = database,
                                                              inv_ticker     = inv_ticker, 
                                                              inv_exc_symbol = inv_exc_symbol, 
                                                              start_date     = start_date, 
                                                              end_date       = end_date, 
                                                              freq_type      = freq_type)
            
    # changed the parameters returned 
    
    for (pri_datetime, 
         startbid, highestbid, lowestbid, lastbid, 
         startask, highestask, lowestask, lastask,
         firsttraded, highesttraded, lowesttraded, lasttraded,
         totalvol) in candlestick_data:
        
        if startbid == None:
            startbid = Decimal(0)
        if highestbid == None:
            highestbid = Decimal(0)
        if lowestbid == None:
            lowestbid = Decimal(0)
        if lastbid == None:
            lastbid = Decimal(0)
        if startask == None:
            startask = Decimal(0)
        if highestask == None:
            highestask = Decimal(0)
        if lowestask == None:
            lowestask = Decimal(0)
        if lastask == None:
            lastask = Decimal(0)
        if firsttraded == None:
            firsttraded = Decimal(0)
        if highesttraded == None:
            highesttraded = Decimal(0)
        if lowesttraded == None:
            lowesttraded = Decimal(0)
        if lasttraded == None:
            lasttraded = Decimal(0)
        if totalvol == None:
            totalvol = Decimal(0)
       
        index_val = inv_ticker + "-" + inv_exc_symbol + "-" + price_type
        
        format_date = pri_datetime.strftime('%Y/%m/%d %H:%M')
        
        # we return different types of data depending on price type
        
        if price_type == 'LASTMID':
            price_to_graph = lastbid + (lastask - lastbid)
            
        if price_type == 'STARTMID':
            price_to_graph = startbid + (startask - startbid)
        
        if price_type == 'HIGHESTMID':
            price_to_graph = highestbid + (highestask - highestbid)
            
        if price_type == 'LOWESTMID':
            price_to_graph = lowestbid + (lowestask - lowestbid)
                
        temp_df = pd.DataFrame({format_date: (float(price_to_graph))}, index=[index_val] )
        
        prices_df = pd.concat([prices_df, temp_df]) 
            
    prices_df = prices_df.groupby(prices_df.index).sum()
    
    if len(prices_df.index) > 0:  
        prices_df = fixit_df(prices_df)
         
    return prices_df


def build_div_df(database, inv_ticker, inv_exc_symbol, start_date, end_date):
   
    divs_df = pd.DataFrame()
    
    # get the dividends within this date range
    
    the_divs = get_dividends(database       = database,
                             inv_ticker     = inv_ticker, 
                             inv_exc_symbol = inv_exc_symbol, 
                             start_date     = start_date, 
                             end_date       = end_date)

    for div_inv_ticker, div_inv_exc_symbol, div_start_date, div_end_date, div_freq, div_exdiv_date, div_pay_date, div_per_share, div_yield, div_cty_symbol, div_record_date, div_declaration_date in the_divs:
        
        if div_per_share == None:
            div_per_share = Decimal(0)
       
        index_val = inv_ticker + "-" + inv_exc_symbol + "-" + "Div"
        
        format_date = div_start_date.strftime('%Y/%m/%d') + ' 12:59'
        
        temp_df = pd.DataFrame({format_date: (float(div_per_share))}, index=[index_val] )
        divs_df = pd.concat([divs_df, temp_df]) 
            
    divs_df = divs_df.groupby(divs_df.index).sum()
    
    if len(divs_df.index) > 0:  
        divs_df = fixit_df(divs_df)
        
    return divs_df


def graph_div_on_prices(database, inv_ticker, inv_exc_symbol, start_date, end_date, produce_graph, display_to_screen, output_path_name, price_type_1, price_type_2):
         
    print('Processing for ', inv_ticker, 'between ', start_date, 'and', end_date)
    
    # build a dataframe of prices ready for graphing
    # will build two if two price types are passed in
    
    # graph the prices dataframe then save the figure

    plt.style.use(VISUAL_STYLE_TO_USE) 
    
    fig = plt.figure(figsize=(20,10))
    ax1 = fig.add_subplot(111)
        
    plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
    plt.ylabel('price', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
   
    # change title to use dollar threshold rather than streak percent
   
    maintitle = "%s.%s Prices between %s and %s With Markers For Ex-Div Dates" % \
        (inv_ticker, inv_exc_symbol, start_date, end_date)
    plt.suptitle(maintitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
    
    prices1_df = build_price_df(inv_ticker, inv_exc_symbol, start_date, end_date, price_type_1)
    
    line_colour = 'grey'
    graph_style = 'LINE'
    
    graph_results(database, inv_ticker, inv_exc_symbol, start_date, end_date, prices1_df, output_path_name, display_to_screen, ax1, line_colour, graph_style)
    
    if price_type_2 != '':
        prices2_df = build_price_df(inv_ticker, inv_exc_symbol, start_date, end_date, price_type_2)
        line_colour = 'blue'
        graph_style = 'LINE'
        graph_results(inv_ticker, inv_exc_symbol, start_date, end_date, prices2_df, output_path_name, display_to_screen, ax1, line_colour, graph_style)
    
    divs1_df = build_div_df(inv_ticker, inv_exc_symbol, start_date, end_date)
    
    # put the divs into a dataframe that has same dimensions as the prices one so can graph on the same graph
    new_divs_df = pd.DataFrame(index=prices1_df.index)   
           
    # next line then puts the streak data frame this TransactionDB onto the same dataframe
    graph_divs_df1 = pd.concat([divs1_df, new_divs_df], axis = 1)
    
    # now need to temporarily combine the dataframes to pull the value out of prices1_df to put into 
    # graph_divs_df1 where the graph_divs_df1 value is not NaN
    
    prices_col = inv_ticker + "-" + inv_exc_symbol + "-" + price_type_1
    divs_col = inv_ticker + "-" + inv_exc_symbol + "-" + "Div"
    
    combined_df = prices1_df.join(graph_divs_df1, how = 'outer')
    for index, row in combined_df.iterrows():
        if row[divs_col] > 0:
            row[divs_col] = row[prices_col]
   
    divs_df1 = combined_df[divs_col]
    
    line_colour = 'red'
    graph_style = 'MARKER'
    graph_results(inv_ticker, inv_exc_symbol, start_date, end_date, divs_df1, output_path_name, display_to_screen, ax1, line_colour, graph_style)
    
    
    
    # change filename to include dollar threshold not percentage
    # now save the graphs to the specified pathname
    
    plt.savefig(output_file_name)

    # Only actually display if boolean says so
    
    if display_to_screen:
        plt.show()

    print("Close database")
    close_db()   
    
    print("Finished graph_div_on_prices") 
    
    return 


def graph_results(inv_ticker, inv_exc_symbol, start_date, end_date, data_df, output_path_name, display_to_screen, ax1, display_colour, graph_style):

    #prices_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
    
    if graph_style == 'LINE':
        data_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = display_colour)
    
    if graph_style == 'MARKER':
        data_df.plot(ax=ax1, kind=VISUAL_GRAPH_KIND, marker=VISUAL_MARKER_KIND, markeredgecolor=VISUAL_MARKER_EDGE_COLOR, color=display_colour)
             
   
    # way do plotting depends on how many plots handling at once - one or many
    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width * VISUAL_LEGEND_BBOX_WIDTH, box.height])
    ax1.legend(loc='center left', bbox_to_anchor=VISUAL_LEGEND_BBOX_TO_ANCHOR, ncol=VISUAL_LEGEND_COLS, fancybox=VISUAL_LEGEND_FANCYBOX, fontsize=VISUAL_LEGEND_FONT_SIZE)
    
    # use every 20th records for the xticks
    ticks_to_use = data_df.index[::5]           
    
    ax1.set_xticklabels(ticks_to_use, rotation=VISUAL_XAXIS_ROTATION)
    plt.grid(linestyle="dotted", color='grey', linewidth=0.5)

    return

      
if __name__ == "__main__":
    
    #
    # set up parameters
    #
    
    inv_ticker        = 'CM'
    inv_exc_symbol    = 'TSE'
    start_date        = '2016-01-01 06:00:00'
    end_date          = '2018-06-30 23:00:0'
    produce_graph     = True
    display_to_screen = True
    output_path_name  = REPORT_FOLDER_OSX
    disp_start_date   = start_date[0:10]
    disp_end_date     = end_date[0:10]
    output_file_name  = "%s_DIVS_ON_PRICES_%s_%s_%s_%s.png" % \
        ( output_path_name, inv_ticker, inv_exc_symbol, disp_start_date, disp_end_date )

    # printing two price types on the same graph
    price_type_1 = 'LASTMID'        
    price_type_2 = ''
       
    print("Started graph_div_on_prices")
    
    print(" ")
    print("Open Database")
    print("-------------------------------")
     
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
    
    results = graph_div_on_prices(database          = database,
                                  inv_ticker        = inv_ticker,
                                  inv_exc_symbol    = inv_exc_symbol, 
                                  start_date        = start_date, 
                                  end_date          = end_date, 
                                  produce_graph     = produce_graph, 
                                  display_to_screen = display_to_screen, 
                                  output_path_name  = output_path_name, 
                                  price_type_1      = price_type_1, 
                                  price_type_2      = price_type_2)

        
    