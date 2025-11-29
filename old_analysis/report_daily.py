#------------------------------------------------------------
# Filename  : report_daily.py
# Project   : ava
#
# Descr     : This file contains code to produce the daily reports 
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
# 2022-01-05 102 DW  Partial rework to add div_exdiv_date NB probably not complete
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from datetime import datetime
from database.db_objects.ImsAnalysisResultDB import get_daily_earlyam_rows, get_daily_overnight_rows
from database.db_objects.ImsDividendDB import get_daily_div_rows, get_dividends_in_next_period
from database.db_objects.ImsEarningDB import get_earnings_in_next_period
from database.db_objects.ImsHistMktDataDB import get_latest_start_datetime_before_date_for_ticker, get_midpoint_for_date
from database.db_objects.ImsStreakSummarieDB import get_daily_streak_rows
from utils.config import REPORT_FOLDER_OSX
from utils.utils_database import open_db, close_db
from utils.utils_HTML import create_doc, finish_doc
from utils.utils_sys import get_sys_params
import webbrowser

def report_daily(database, inv_ticker, str_run_date, streak_filename, earlyam_filename, overnight_filename, div_filename, upcoming_filename, display_report, upcoming_period, days_urgent):
    
    print("Started report_daily")
    print(" ")
    print("Producing daily streak report")   
    
    produce_daily_streak_reports(database        = database, 
                                 inv_ticker      = inv_ticker, 
                                 str_run_date    = str_run_date, 
                                 streak_filename = streak_filename, 
                                 display_report  = display_report)
    
    print("Producing daily early morning report")   
    
    produce_daily_earlyam_reports(database         = database, 
                                  inv_ticker       = inv_ticker, 
                                  str_run_date     = str_run_date, 
                                  earlyam_filename = earlyam_filename, 
                                  display_report   = display_report)
    
    print("Producing daily overnight report")   
    
    produce_daily_overnight_reports(database         = database, 
                                  inv_ticker         = inv_ticker, 
                                  str_run_date       = str_run_date, 
                                  overnight_filename = overnight_filename, 
                                  display_report     = display_report)
    
    print("Producing daily dividend report")   
    
    produce_daily_div_reports(database       = database, 
                              inv_ticker     = inv_ticker, 
                              str_run_date   = str_run_date, 
                              div_filename   = div_filename, 
                              display_report = display_report)
    
    print("Producing upcoming report")   
    
    produce_upcoming_reports(database          = database, 
                             inv_ticker        = inv_ticker, 
                             str_run_date      = str_run_date, 
                             upcoming_filename = upcoming_filename, 
                             display_report    = display_report, 
                             upcoming_period   = upcoming_period, 
                             days_urgent       = days_urgent)
    
    print("Finished upcoming report") 
    
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


def produce_daily_div_reports(database, inv_ticker, str_run_date, div_filename, display_report):
    
    div_rows = get_daily_div_rows(database   = database, 
                                  inv_ticker = inv_ticker)
    
    new_div_rows = []
    
    for div_rec in div_rows:
        this_ticker            = div_rec[0]
        this_as_of_date        = div_rec[1]
        this_div_per_share     = div_rec[2]
        this_div_exdiv_date    = div_rec[3]
        this_sec_name          = div_rec[4]
        
        str_this_as_of_date = this_as_of_date.strftime("%Y-%m-%d") + ' 13:59:59'
        # get the last start_datetime before this_as_of_date
        last_datetime = get_latest_start_datetime_before_date_for_ticker(database           = database,
                                                                         hmd_inv_ticker     = this_ticker, 
                                                                         hmd_start_datetime = str_this_as_of_date)
        
        if last_datetime != None:
            # get the midpoint price closest to this_as_of_date
            midpoint_price = get_midpoint_for_date(database           = database,
                                                   hmd_inv_ticker     = this_ticker, 
                                                   hmd_start_datetime = last_datetime, 
                                                   hmd_freq_type      = '1 min')[0]
        
            if midpoint_price != None:
                
                percent_div = 100 * (this_div_per_share / midpoint_price)
            
            
                new_div_rec = (this_ticker, this_as_of_date, this_div_per_share, this_sec_name, midpoint_price, percent_div)
                new_div_rows.append(new_div_rec)
    
    sorted_div_rows = sorted(new_div_rows, key=lambda tup: tup[5], reverse=True)
    
    latest_daily_div_report(div_rows        = sorted_div_rows, 
                            inv_ticker      = inv_ticker, 
                            str_run_date    = str_run_date, 
                            div_filename    = div_filename, 
                            display_report  = display_report)
    
    
    return 


def produce_upcoming_reports(database, inv_ticker, str_run_date, upcoming_filename, display_report, upcoming_period, days_urgent):
    
    upcoming_div_rows = get_dividends_in_next_period(database        = database, 
                                                     inv_ticker      = inv_ticker, 
                                                     upcoming_period = upcoming_period, 
                                                     days_urgent     = days_urgent)
    upcoming_ear_rows = get_earnings_in_next_period(database        = database, 
                                                    inv_ticker      = inv_ticker, 
                                                    upcoming_period = upcoming_period, 
                                                    days_urgent     = days_urgent)
    
    latest_upcoming_report(upcoming_div_rows = upcoming_div_rows, 
                           upcoming_ear_rows = upcoming_ear_rows, 
                           inv_ticker        = inv_ticker, 
                           str_run_date      = str_run_date, 
                           upcoming_filename = upcoming_filename, 
                           display_report    = display_report)
    
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


def latest_daily_div_report(div_rows, inv_ticker, str_run_date, div_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Dividend Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> Daily Dividend Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(div_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True
    
    for div_rec in div_rows:
        this_ticker         = div_rec[0]
        this_as_of_date     = div_rec[1]
        this_div_per_share  = div_rec[2]
        this_exdiv_date     = div_rec[3]
        this_sec_name       = div_rec[4]
        this_midpoint_price = div_rec[5]
        this_percent_div    = div_rec[6]
        
        if this_div_per_share == None:
            this_div_per_share = 0
        if this_midpoint_price == None:
            this_midpoint_price = 0
        if this_percent_div == None:
            this_percent_div = 0
        
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Sector </td>"
            html += "<td class=\"descr\"> Date </td>"
            html += "<td class=\"smallvalue\"> Div Per Share </td> " 
            html += "<td class=\"smallvalue\"> Midpoint </td> " 
            html += "<td class=\"smallvalue\"> % Div </td> "
            html += "</tr>" 
            
        html += "<tr> <td class=\"descr\"> %s </td>"     % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"          % (this_sec_name)
        html += "<td class=\"descr\"> %s </td>"          % (this_as_of_date)
        html += "<td class=\"descr\"> %s </td>"          % (this_exdiv_date)
        html += "<td class=\"smallvalue\"> %.2f </td>"   % (this_div_per_share) 
        html += "<td class=\"smallvalue\"> %.2f </td>"   % (this_midpoint_price) 
        html += "<td class=\"smallvalue\"> %.2f </td> "  % (this_percent_div)
        html += "</tr>" 
                        
    html += "</table><br>"

    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (div_filename)
        webbrowser.open(html_link)

    return


def latest_upcoming_report(upcoming_div_rows, upcoming_ear_rows, inv_ticker, str_run_date, upcoming_filename, display_report):
    
    if inv_ticker == '%':
        html = "<h1> Daily Upcoming Report for All Tickers on %s </h1>" % (str_run_date)
    else:
        html = "<h1> Daily Upcoming Report for %s for %s </h1>" % (inv_ticker, str_run_date) 
        
    f = open(upcoming_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    first_time = True

    html += "<h2> Upcoming Dividend Dates <h2>"
    for upcoming_div_rec in upcoming_div_rows:
        this_ticker                    = upcoming_div_rec[0]
        this_as_of_date                = upcoming_div_rec[1]
        this_div_period                = upcoming_div_rec[2]
        this_div_per_share             = upcoming_div_rec[3]
        this_next_as_of_date           = upcoming_div_rec[4]
        this_urgency                   = upcoming_div_rec[5]
            
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Last As Of Date </td>"
            html += "<td class=\"descr\"> Dividend Period </td>"
            html += "<td class=\"descr\"> Dividends Per Share </td> " 
            html += "<td class=\"descr\"> Next As Of Date </td>"
            html += "</tr>"
            
        html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"               % (this_as_of_date)
        html += "<td class=\"descr\"> %s </td>"               % (this_div_period)
        html += "<td class=\"descr\"> %.2f </td>"             % (this_div_per_share) 
        if this_urgency == 'Urgent':
            html += "<td class=\"lossfont\"> %s </td>"        % (this_next_as_of_date)
        else:
            html += "<td class=\"descr\"> %s </td>"           % (this_next_as_of_date)
            
                    
    html += "</table><br>"

    first_time = True
    html += "<h2> Upcoming Earnings Dates <h2>"
    
    for upcoming_ear_rec in upcoming_ear_rows:
        this_ticker                    = upcoming_ear_rec[0]
        this_as_of_date                = upcoming_ear_rec[1]
        this_ear_period                = upcoming_ear_rec[2]
        this_ear_per_share             = upcoming_ear_rec[3]
        this_next_as_of_date           = upcoming_ear_rec[4]
        this_urgency                   = upcoming_ear_rec[5]
            
        if first_time:
            
            first_time = False       
            html += "</table><br>"    
            html += "<table class=\"font10\"> <tr class=\"head\"> "
            html += "<td class=\"descr\"> Ticker </td>"
            html += "<td class=\"descr\"> Last As Of Date </td>"
            html += "<td class=\"descr\"> Earnings Period </td>"
            html += "<td class=\"descr\"> Earnings Per Share </td> " 
            html += "<td class=\"descr\"> Next As Of Date </td>"
            html += "</tr>"
            
        html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
        html += "<td class=\"descr\"> %s </td>"               % (this_as_of_date)
        html += "<td class=\"descr\"> %s </td>"               % (this_ear_period)
        html += "<td class=\"descr\"> %.2f </td>"             % (this_ear_per_share) 
        if this_urgency == 'Urgent':
            html += "<td class=\"lossfont\"> %s </td>"        % (this_next_as_of_date)
        else:
            html += "<td class=\"descr\"> %s </td>"           % (this_next_as_of_date)
                    
    html += "</table><br>"
    
    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (upcoming_filename)
        webbrowser.open(html_link)

    return


if __name__ == "__main__":
    
    (num_args, args_list) = get_sys_params()
    
    if num_args == 0:
        #inv_ticker = 'CM.TO'
        inv_ticker            = '%'
        display_report        = 'Y'
        streak_filename       = REPORT_FOLDER_OSX + 'report_daily_streaks.html'
        earlyam_filename      = REPORT_FOLDER_OSX + 'report_daily_earlyam.html'
        overnight_filename    = REPORT_FOLDER_OSX + 'report_daily_overnight.html'
        div_filename          = REPORT_FOLDER_OSX + 'report_daily_div.html'
        upcoming_div_filename = REPORT_FOLDER_OSX + 'report_upcoming_divs.html'
        upcoming_period_number = "'7'"
        upcoming_period_duration = 'DAY'
        upcoming_period = upcoming_period_number + ' ' + upcoming_period_duration
        days_urgent           = 3
        
        
    else:
        if num_args == 10:
            
            removed_brackets = args_list.strip("[]")
            split_args_list = removed_brackets.split(",")
            
            inv_ticker            = split_args_list[0].strip("'")
            display_report        = split_args_list[1].strip(" ").strip("'")
            streak_filename       = split_args_list[2].strip(" ").strip("'")
            earlyam_filename      = split_args_list[3].strip(" ").strip("'") 
            overnight_filename    = split_args_list[4].strip(" ").strip("'") 
            div_filename          = split_args_list[5].strip(" ").strip("'")   
            upcoming_div_filename = split_args_list[6].strip(" ").strip("'")
            days_urgent           = int(split_args_list[7].strip(" ").strip("'"))
            
            # Note - yes I really do mean to be stripping off double but not single quotes here. Please leave alone
            upcoming_period_number  = split_args_list[8].strip(" ").strip('"') 
            upcoming_period_duration = split_args_list[9].strip(" ").strip("'")
            upcoming_period = upcoming_period_number + ' ' + upcoming_period_duration
            
            print(' ')          
            print('parameters received', inv_ticker, display_report, streak_filename, earlyam_filename, overnight_filename, div_filename, upcoming_div_filename, days_urgent, upcoming_period)
            print(' ')
            
    if (num_args != 10 and num_args != 0):
        print("ERROR report_daily.py - wrong number of args provided expected 10")
        print("Number args: ", num_args)
        print("Arg list: ", args_list)
        
    else:
        
        print("report_daily - Open Database")
            
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
        
        report_daily(database              = database, 
                     inv_ticker            = inv_ticker, 
                     str_run_date          = str_run_date, 
                     streak_filename       = streak_filename, 
                     earlyam_filename      = earlyam_filename, 
                     overnight_filename    = overnight_filename,
                     div_filename          = div_filename, 
                     upcoming_filename     = upcoming_div_filename, 
                     display_report        = display_report, 
                     days_urgent           = days_urgent,
                     upcoming_period       = upcoming_period 
                     )
    
        print("report_daily - Close Database")
            
        close_db(database = database)   

    
 
