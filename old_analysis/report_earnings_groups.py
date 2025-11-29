#------------------------------------------------------------
# Filename  : report_earnings_groups.py
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
# 2020-02-04   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from database.db_objects.ImsEarningDB import get_earnings_in_next_period
from database.db_objects.ImsGroupingDB import get_tickers_for_grouping_type
from datetime import datetime
from infrastructure.blackboard.load_todos_manager import load_todos_manager
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD, REPORT_FOLDER_OSX
from utils.utils_database import open_db, close_db
from utils.utils_HTML import create_doc, finish_doc
from utils.utils_sys import get_sys_params
import webbrowser


def report_earnings_groups(database, inv_ticker, str_run_date, report_filename, display_report, upcoming_period, days_urgent, grp_type):
    
    # first start building the html header
    html = latest_daily_earnings_group_header(str_run_date = str_run_date, 
                                              grp_type     = grp_type)
   
    # first find the tickers with earnings coming up that are in the group
    grouped_tickers = get_tickers_for_grouping_type(database = database, 
                                                    grp_type = grp_type)
        
    for this_ticker in grouped_tickers:
        
        this_ticker = this_ticker[0]
        report_rows = get_earnings_in_next_period(database        = database, 
                                                  this_ticker     = this_ticker, 
                                                  upcoming_period = upcoming_period, 
                                                  days_urgent     = days_urgent)
        
        for report_rec in report_rows:
            html = latest_daily_earnings_group_details(html       = html, 
                                                       report_rec = report_rec)   
                
    latest_daily_earnings_group_complete(html            = html, 
                                         report_filename = report_filename, 
                                         display_report  = display_report)
    
    return 


def latest_daily_earnings_group_header(str_run_date, grp_type):
    
    html = "<h1> Daily Earnings Group Report for Group Type %s on %s </h1>" % (grp_type, str_run_date)
    html += "</table><br>"    
    html += "<table class=\"font10\"> <tr class=\"head\"> "
    html += "<td class=\"descr\"> Ticker </td>"
    html += "<td class=\"descr\"> Last As Of Date </td>"
    html += "<td class=\"descr\"> Earnings Period </td>"
    html += "<td class=\"descr\"> Earnings Per Share </td> " 
    html += "<td class=\"descr\"> Next As Of Date </td>"
    html += "</tr>"
            
    return html


def latest_daily_earnings_group_details(html, report_rec):
    
    this_ticker                    = report_rec[0]
    this_as_of_date                = report_rec[1]
    this_ear_period                = report_rec[2]
    this_ear_per_share             = report_rec[3]
    this_next_as_of_date           = report_rec[4]
    this_urgency                   = report_rec[5]
    
    html += "<tr> <td class=\"descr\"> %s </td>"          % (this_ticker)
    html += "<td class=\"descr\"> %s </td>"               % (this_as_of_date)
    html += "<td class=\"descr\"> %s </td>"               % (this_ear_period)
    html += "<td class=\"descr\"> %.2f </td>"             % (this_ear_per_share) 
    if this_urgency == 'Urgent':
        html += "<td class=\"lossfont\"> %s </td>"        % (this_next_as_of_date)
    else:
        html += "<td class=\"descr\"> %s </td>"           % (this_next_as_of_date)
    html += "</tr>"
    
    return html


def latest_daily_earnings_group_complete(html, report_filename, display_report):                
    
    html += "</table><br>"
    
    f = open(report_filename,'w')
    font_size = 10
    create_doc(f, font_size)

    f.write(html)
    finish_doc(f)
    f.close()
    
    if display_report == 'Y':
        html_link = 'file://%s' % (report_filename)
        webbrowser.open(html_link)

    return


if __name__ == "__main__":

    print("Started report_earnings_groups")
    print(" ")
        
    (num_args, args_list) = get_sys_params()
    
    if num_args == 0:
        #inv_ticker = 'CM.TO'
        inv_ticker            = '%'
        display_report        = 'Y'
        report_filename       = REPORT_FOLDER_OSX + 'report_earnings_groups.html'
        upcoming_period       = '1 WEEK'
        days_urgent           = 3
        grp_type              = 'BANK_EARN'
        
    else:
        if num_args == 6:
            inv_ticker            = args_list[0]
            display_report        = args_list[1]
            report_filename       = args_list[2] 
            upcoming_period       = args_list[3]
            days_urgent           = args_list[4]
            grp_type              = args_list[5]
        
    if (num_args != 6 and num_args != 0):
        print("ERROR report_earnings_groups.py - wrong number of args provided expected 6")
        print("Number args: ", num_args)
        print("Arg list: ", args_list)
        
    else:
        
        # Create report for last business day
        
        today_day_of_week = datetime.today().weekday()
        
        if today_day_of_week == 6 or today_day_of_week == 0:
            # if Sunday or Monday go back 3 days
            num_days = 3
        else: 
            # any other day go back  day
            num_days = 1
        
        str_run_date = datetime.strftime(datetime.now(), '%y-%m-%d')
        
        print("Open db")
        print(" ")
        
        database = open_db(host        = DB_HOST, 
                           port        = DB_PORT, 
                           tns_service = DB_TNS_SERVICE, 
                           user_name   = DB_USER_NAME, 
                           password    = DB_PASSWORD)
    
        report_earnings_groups(database        = database, 
                               inv_ticker      = inv_ticker, 
                               str_run_date    = str_run_date, 
                               report_filename = report_filename, 
                               display_report  = display_report, 
                               upcoming_period = upcoming_period, 
                               days_urgent     = days_urgent, 
                               grp_type        = grp_type)
    
    print("Close database")
    
    close_db(database = database)   

    print("Finished report_earnings_groups") 
        
 
