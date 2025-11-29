#------------------------------------------------------------
# Filename  : report_histmktdata.py
# Project   : ava
#
# Descr     : This file contains code to produce the daily 
#             histmktdata reports. Based on report_daily but
#             broken down into constituent parts.
#
# Params    : ticker (or %)
#             display_report Y/N
#             filename excluding path
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2019-11-09   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from datetime import datetime
from database.db_objects.ImsAnalysisResultDB import get_daily_earlyam_rows, get_daily_overnight_rows
from database.db_objects.ImsHistMktDataDB import get_latest_start_datetime_before_date_for_ticker, get_midpoint_for_date
from database.db_objects.ImsStreakSummarieDB import get_daily_streak_rows
from utils.config import REPORT_FOLDER_OSX
from utils.utils_database import open_db, close_db
from utils.utils_HTML import create_doc, finish_doc
from utils.utils_sys import get_sys_params
import webbrowser

def report_histmktdata(database, inv_ticker, str_run_date, streak_filename, earlyam_filename, overnight_filename, display_report):
    
    print("Started report_histmktdata")
    print(" ")
    print("Producing daily streak report")   
    
    produce_daily_streak_reports(database        = database, 
                                 inv_ticker      = inv_ticker, 
                                 str_run_date    = str_run_date, 
                                 streak_filename = streak_filename, 
                                 display_report  = display_report)
    
    print("Finished daily streak report") 

    print("Producing daily early morning report")   
    
    produce_daily_earlyam_reports(database         = database, 
                                  inv_ticker       = inv_ticker, 
                                  str_run_date     = str_run_date, 
                                  earlyam_filename = earlyam_filename, 
                                  display_report   = display_report)
    
    print("Finished daily early morning report") 

    print("Producing daily overnight report")   
    
    produce_daily_overnight_reports(database         = database, 
                                  inv_ticker         = inv_ticker, 
                                  str_run_date       = str_run_date, 
                                  overnight_filename = overnight_filename, 
                                  display_report     = display_report)
    
    print("Finished daily overnight report")   
    
    return
    
    
def produce_daily_streak_reports(database, inv_ticker, str_run_date, streak_filename, display_report):
    
    streak_rows = get_daily_streak_rows(database   = database, 
                                        inv_ticker = inv_ticker)
    
    latest_daily_streak_report(streak_rows     = streak_rows, 
                               inv_ticker      = inv_ticker, 
                               str_run_date    = str_run_date, 
                               streak_filename = streak_filename, 
                               display_report  = display_report)
    
    return 


def produce_daily_earlyam_reports(database, inv_ticker, str_run_date, earlyam_filename, display_report):
    
    earlyam_rows = get_daily_earlyam_rows(database   = database, 
                                          inv_ticker = inv_ticker)
    
    latest_daily_earlyam_report(earlyam_rows     = earlyam_rows, 
                                inv_ticker       = inv_ticker, 
                                str_run_date     = str_run_date, 
                                earlyam_filename = earlyam_filename, 
                                display_report   = display_report)
    
    return 


def produce_daily_overnight_reports(database, inv_ticker, str_run_date, overnight_filename, display_report):
    
    overnight_rows = get_daily_overnight_rows(database   = database, 
                                              inv_ticker = inv_ticker)
    
    latest_daily_overnight_report(overnight_rows     = overnight_rows, 
                                  inv_ticker         = inv_ticker, 
                                  str_run_date       = str_run_date, 
                                  overnight_filename = overnight_filename, 
                                  display_report     = display_report)
    
    return 


def latest_daily_streak_report(streak_rows, inv_ticker, str_run_date, streak_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Streaks Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> Daily Streaks Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(streak_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True
    total_profit = 0
    total_ups = 0
    total_downs = 0
    for streak_rec in streak_rows:
        this_ticker = streak_rec[0]
        this_date = streak_rec[1]
        this_id = streak_rec[2]
        this_avg_spread = streak_rec[3]
        this_total_profit = streak_rec[4]
        this_total_ups = streak_rec[5]
        this_total_downs = streak_rec[6]

        total_profit += this_total_profit
        total_ups += this_total_ups
        total_downs += this_total_downs
        
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Date </td>"
            html += "<td class=\"smallvalue\"> Daily Profit </td> " 
            html += "<td class=\"smallvalue\"> Daily Ups </td> " 
            html += "<td class=\"smallvalue\"> Daily Downs </td> "
            html += "<td class=\"smallvalue\"> Daily Avg Spread </td> </tr>"
            
        html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"               % (this_date)
        html += "<td class=\"smallvalue\"> %.2f </td>"        % (this_total_profit) 
        html += "<td class=\"smallvalue\"> %.0f </td> "       % (this_total_ups)
        html += "<td class=\"smallvalue\"> %.0f </td> "       % (this_total_downs)
        html += "<td class=\"smallvalue\"> %.2f </td> </tr>"  % (this_avg_spread)
          
    html += "<tr> <td class=\"descr\"> TOTALS </td>" 
    html += "<td class=\"descr\"> </td> "
    html += "<td class=\"smallvalue\"> %.2f </td>"            % (total_profit) 
    html += "<td class=\"smallvalue\"> %.0f </td> "           % (total_ups)
    html += "<td class=\"smallvalue\"> %.0f </td> "           % (total_downs)
    html += "<td class=\"smallvalue\">  </td> </tr>" 
              
    html += "</table><br>"
    
    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (streak_filename)
        webbrowser.open(html_link)

    return


def latest_daily_earlyam_report(earlyam_rows, inv_ticker, str_run_date, earlyam_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Early Morning Stats Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> DDaily Early Morning Stats Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(earlyam_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True

    for earlyam_rec in earlyam_rows:
        this_ticker                    = earlyam_rec[0]
        this_trading_date              = earlyam_rec[1]
        this_early_change_bid          = earlyam_rec[2]
        this_early_change_ask          = earlyam_rec[3]
        this_perc_early_change_bid     = earlyam_rec[4]
        this_perc_early_change_ask     = earlyam_rec[5]
        
        
        if this_early_change_bid == None:
            this_early_change_bid = 0
        if this_early_change_ask == None:
            this_early_change_ask = 0
        if this_perc_early_change_bid == None:
            this_perc_early_change_bid = 0
        if this_perc_early_change_ask == None:
            this_perc_early_change_ask = 0
            
            
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Date </td>"
            html += "<td class=\"smallvalue\"> Early Change Bid </td> "
            html += "<td class=\"smallvalue\"> % Early Change Bid </td> "
            html += "<td class=\"smallvalue\"> Early Change Ask </td> "
            html += "<td class=\"smallvalue\"> % Early Change Ask </td> </tr>"
            
        html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"               % (this_trading_date)
        html += "<td class=\"smallvalue\"> %.2f </td> "       % (this_early_change_bid)
        html += "<td class=\"smallvalue\"> %.2f </td> "       % (this_perc_early_change_bid)
        html += "<td class=\"smallvalue\"> %.2f </td> "       % (this_early_change_ask)
        html += "<td class=\"smallvalue\"> %.2f </td> </tr>"  % (this_perc_early_change_ask)
                    
    html += "</table><br>"

    
    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (earlyam_filename)
        webbrowser.open(html_link)

    return


def latest_daily_overnight_report(overnight_rows, inv_ticker, str_run_date, overnight_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Overnight Stats Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> DDaily Overnight Stats Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(overnight_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True

    for overnight_rec in overnight_rows:
        this_ticker                    = overnight_rec[0]
        this_trading_date              = overnight_rec[1]
        this_overnight_change_bid      = overnight_rec[2]
        this_overnight_change_ask      = overnight_rec[3]
        this_perc_overnight_change_bid = overnight_rec[4]
        this_perc_overnight_change_ask = overnight_rec[5]
        
        if this_overnight_change_bid == None:
            this_overnight_change_bid = 0
        if this_overnight_change_ask == None:
            this_overnight_change_ask = 0
        if this_perc_overnight_change_bid == None:
            this_perc_overnight_change_bid = 0
        if this_perc_overnight_change_ask == None:
            this_perc_overnight_change_ask = 0
            
            
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Date </td>"
            html += "<td class=\"smallvalue\"> Overnight Change Bid </td> " 
            html += "<td class=\"smallvalue\"> % Overnight Change Bid </td> " 
            html += "<td class=\"smallvalue\"> Overnight Change Ask </td> " 
            html += "<td class=\"smallvalue\"> % Overnight Change Ask </td> </tr>" 
        
            
        html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"               % (this_trading_date)
        html += "<td class=\"smallvalue\"> %.2f </td>"        % (this_overnight_change_bid) 
        html += "<td class=\"smallvalue\"> %.2f </td>"        % (this_perc_overnight_change_bid) 
        html += "<td class=\"smallvalue\"> %.2f </td> "       % (this_overnight_change_ask)
        html += "<td class=\"smallvalue\"> %.2f </tr> </td> " % (this_perc_overnight_change_ask)
                     
    html += "</table><br>"

    
    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (overnight_filename)
        webbrowser.open(html_link)

    return



if __name__ == "__main__":
    
    (num_args, args_list) = get_sys_params()
    
    if num_args == 0:
        inv_ticker            = '%'
        display_report        = 'Y'
        streak_filename       = REPORT_FOLDER_OSX + 'report_daily_streaks.html'
        earlyam_filename      = REPORT_FOLDER_OSX + 'report_daily_earlyam.html'
        overnight_filename    = REPORT_FOLDER_OSX + 'report_daily_overnight.html'        
        
    else:
        if num_args == 5:
            
            removed_brackets = args_list.strip("[]")
            split_args_list = removed_brackets.split(",")
            
            inv_ticker            = split_args_list[0].strip("'")
            display_report        = split_args_list[1].strip(" ").strip("'")
            streak_filename       = split_args_list[2].strip(" ").strip("'")
            earlyam_filename      = split_args_list[3].strip(" ").strip("'") 
            overnight_filename    = split_args_list[4].strip(" ").strip("'") 
            
            print(' ')          
            print('parameters received', inv_ticker, display_report, streak_filename, earlyam_filename, overnight_filename)
            print(' ')
            
    if (num_args != 5 and num_args != 0):
        print("ERROR report_histmktdata.py - wrong number of args provided expected 5")
        print("Number args: ", num_args)
        print("Arg list: ", args_list)
        
    else:
        
        print("report_histmktdata - Open Database")
            
        database = open_db(host        = DB_HOST, 
                           port        = DB_PORT, 
                           tns_service = DB_TNS_SERVICE, 
                           user_name   = DB_USER_NAME, 
                           password    = DB_PASSWORD)  
             
        # Create report for last business day
        
        today_day_of_week = datetime.today().weekday()
        
        if today_day_of_week == 6 or today_day_of_week == 0:
            # if Sunday or Monday go back 3 days
            num_days = 3
        else: 
            # any other day go back  day
            num_days = 1
        
        str_run_date = datetime.strftime(datetime.now(), '%y-%m-%d')
        
        report_histmktdata(database              = database, 
                     inv_ticker            = inv_ticker, 
                     str_run_date          = str_run_date, 
                     streak_filename       = streak_filename, 
                     earlyam_filename      = earlyam_filename, 
                     overnight_filename    = overnight_filename,
                     display_report        = display_report
                     )
    
        print("report_histmktdata - Close Database")
            
        close_db(database = database)   

    
 
