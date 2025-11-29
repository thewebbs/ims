#------------------------------------------------------------
# Filename  : produce_candlestick_plot.py
# Project   : ava
#
# Descr     : This file contains code to graph pricing data
#             using the seaborn package
#
# Params    : inv_ticker
#             inv_exc_symbol
#             start_date
#             end_date
#             display_to_screen
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- --------
# 2018-07-17   2 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from database.db_objects.CandlestickDB import get_ohlc
from utils.config import REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db 
from decimal import Decimal
from mpl_finance import candlestick_ohlc
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import matplotlib.dates
from pygments.unistring import combine
from matplotlib.pyplot import tight_layout


def produce_candlestick_plot(inv_ticker, inv_exc_symbol, start_date, end_date, display_to_screen): 
    
    print(" ")
    print("Processing for ", inv_ticker,".", inv_exc_symbol, "between ",start_date,"and",end_date)
    
    freq_type = '1 min'
    
    candlesticks    = []
    candlesticks[:] = get_ohlc(inv_ticker, inv_exc_symbol, start_date, end_date, freq_type)
    
    date_data  = []
    open_data  = []
    high_data  = []
    low_data   = []
    close_data = []
       
    for (can_date, open, high, low, close) in candlesticks:
    
        if open == None:
            open = Decimal(0)
        if high == None:
            high = Decimal(0)
        if low == None:
            low = Decimal(0)
        if close == None:
            close = Decimal(0)
        
        format_date = date2num(can_date)
        date_data.append(format_date)
        open_data.append(float(open))
        high_data.append(float(high))
        low_data.append(float(low))
        close_data.append(float(close))
    
    # now combine these into one data structure for plotting    
    
    quotes = [tuple([date_data[i],
                     open_data[i],
                     high_data[i],
                     low_data[i],
                     close_data[i]]) for i in range(len(date_data))] 
    
    # sets color scheme to have a black background with white axes lettering
    
    plt.style.use('dark_background')      
    
    # sets the size and shape of the diagram 
    
    fig, ax = plt.subplots(num=None, figsize=(30,10), dpi=80)
    
    # formatting the x axis as a date, and setting the amount of space beneath
    
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M'))
    
    # sets the linestyle and line weight of both the major and minor gridlines
    
    ax.grid(True)
    ax.grid(color='w', which='major', linestyle='-.', linewidth=0.5)
    ax.grid(color='w', which='minor', linestyle='-.', linewidth=0.25)
    
    # but the minor tick lines won't show unless we turn them on
    
    plt.minorticks_on()
    
    # finally, now everthing's set up we can print the candlestick 
    # specifying in width is the size of candlestick rectangles
    # alpha seems to be how transparent the candlesticks are so we want solid
    
    candlestick_ohlc(ax, quotes, width=0.0006, colorup='green', colordown='red', alpha=1.0)
    
    
    # ---------------------------------------------------------
    
    output_path_name = REPORT_FOLDER_OSX
    output_file_name = "%sCANDLESTICK_%s.png" % (output_path_name, inv_ticker)
    plt.savefig(output_file_name)
    print('Output saved to', output_file_name)
    
    if display_to_screen:
        plt.show()
    
    return


if __name__ == "__main__":
    
    print("Started produce_candlestick_plot")
    print(" ")
    
    print("Open Database")

    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   
    
    produce_candlestick_plot(inv_ticker        = 'CM', 
                             inv_exc_symbol    = 'TSE', 
                             start_date        = '2018-07-12 06:00:00', 
                             end_date          = '2018-07-13 14:00:00', 
                             display_to_screen = True)
    
    print("Close database")
    close_db(database = database)   
    
    print(" ")
    print("Finished produce_candlestick_plot") 
    print(" ")
