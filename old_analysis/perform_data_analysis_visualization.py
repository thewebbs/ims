#------------------------------------------------------------
# Filename  : perform_data_analysis_visualization.py
# Project   : ava
#
# Descr     : This file contains code to graph pricing data
#             using the seaborn package
#
# Params    : database,
#             mode
#             start_date
#             end_date
#             display_to_screen
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2018-07-08   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db
from apis.dataload.load_drive_table import load_drive_table
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from utils.utils_dataframes import format_drive_table_df, format_drive_table_dow_df


def change_str_to_int(x):
    newx = int(x) - 180300
    return newx


def perform_data_analysis_visualization(database, mode, start_date, end_date, display_to_screen): 
    
   
    #
    #=============================================================================
    #
    # drive graph from the drive_table but populate it first with specific investments
    
    run_name  = 'SPECIFIED'
    load_type = 'INVESTMENT'
    
    if (mode == 'MATPLOTLIB'):
        delete_first = True
        load_drive_table(run_name, load_type, 'CM', 'TSE', delete_first, start_date, end_date)
        delete_first = False
        load_drive_table(run_name, load_type, 'BMO', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'TD', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'XIU', 'TSE', delete_first, start_date, end_date)
        dataframe = format_drive_table_df(run_name, start_date, end_date)
        graph_from_dataframe(dataframe, 1, mode, start_date, end_date, display_to_screen)
       
    
    if (mode == 'SEABORN_TS_PLOT') or (mode == 'SEABORN_BOX_PLOT') :
        
        delete_first = True
        load_drive_table(run_name, load_type, 'CM', 'TSE', delete_first, start_date, end_date)
        delete_first = False
        load_drive_table(run_name, load_type, 'BMO', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'TD', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'XIU', 'TSE', delete_first, start_date, end_date)
        dataframe = format_drive_table_df(run_name, start_date, end_date)
        
        # Seaborn expects dataframes to be in the tidy longform so we need to re-organize a bit
        dataframe['Timepoint'] = dataframe.index
        longform_dataframe=pd.melt(dataframe, id_vars="Timepoint", var_name="Ticker", value_name="Price")
        longform_dataframe = longform_dataframe.reset_index()
        
        graph_from_dataframe(longform_dataframe, 1, mode, start_date, end_date, display_to_screen)
    
    
    if (mode == 'SEABORN_LM_PLOT') or (mode == 'SEABORN_JOINT_PLOT') or (mode == 'SEABORN_HEXBIN_PLOT') \
        or (mode == 'SEABORN_KDE_PLOT') or (mode == 'SEABORN_KDE_GRID_PLOT') :
        
        delete_first = True
        load_drive_table(run_name, load_type, 'CM', 'TSE', delete_first, start_date, end_date)
        delete_first = False
        load_drive_table(run_name, load_type, 'BMO', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'TD', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'XIU', 'TSE', delete_first, start_date, end_date)
        dataframe = format_drive_table_df(run_name, start_date, end_date)
        
        # Seaborn expects dataframes to be in the tidy longform so we need to re-organize a bit
        dataframe['Timepoint'] = dataframe.index
        
        # need integer for calculations in lmplot so adding new column then removing original
        dataframe['Timeindex'] = dataframe['Timepoint'].map(change_str_to_int)
        newdataframe = dataframe.drop(['Timepoint'], axis=1)
        
        longform_dataframe=pd.melt(newdataframe, id_vars="Timeindex", var_name="Ticker", value_name="Price")
        longform_dataframe = longform_dataframe.reset_index()
        graph_from_dataframe(longform_dataframe, 1, mode, start_date, end_date, display_to_screen)
    '''
    if (mode == 'SEABORN_PAIR_PLOT') or (mode == 'SEABORN_PAIR_KDE_PLOT'):
        
        delete_first = True
        load_drive_table(run_name, load_type, 'CM', 'TSE', delete_first, start_date, end_date)
        delete_first = False
        load_drive_table(run_name, load_type, 'BMO', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'TD', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'XIU', 'TSE', delete_first, start_date, end_date)
        dataframe = format_drive_table_df(run_name, start_date, end_date)
        
        # Seaborn expects dataframes to be in the tidy longform so we need to re-organize a bit
        dataframe['Timepoint'] = dataframe.index
        
        # need integer for calculations in lmplot so adding new column then removing original
        dataframe['Timeindex'] = dataframe['Timepoint'].map(change_str_to_int)
        newdataframe = dataframe.drop(['Timepoint'], axis=1)
        
        longform_dataframe=pd.melt(newdataframe, id_vars="Timeindex", var_name="Ticker", value_name="Price")
        longform_dataframe = longform_dataframe.reset_index()
        graph_from_dataframe(longform_dataframe, 1, mode, start_date, end_date, display_to_screen)
    '''
    if (mode == 'SEABORN_STRIP_PLOT') or (mode == 'SEABORN_PAIR_PLOT') or (mode == 'SEABORN_PAIR_KDE_PLOT'):
        
        delete_first = True
        load_drive_table(run_name, load_type, 'CM', 'TSE', delete_first, start_date, end_date)
        delete_first = False
        load_drive_table(run_name, load_type, 'BMO', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'TD', 'TSE', delete_first, start_date, end_date)
        load_drive_table(run_name, load_type, 'XIU', 'TSE', delete_first, start_date, end_date)
        
        prices_df = format_drive_table_df(run_name, start_date, end_date)
        prices_df['Timepoint'] = prices_df.index
        prices_df['Timeindex'] = prices_df['Timepoint'].map(change_str_to_int)
        prices_df = prices_df.drop(['Timepoint'], axis=1)
        
        longform_prices_df=pd.melt(prices_df, id_vars="Timeindex", var_name="Ticker", value_name="Price")
        longform_prices_df = longform_prices_df.reset_index()
        
        days_of_week_df = format_drive_table_dow_df(run_name, start_date, end_date)
        days_of_week_df['Timepoint'] = days_of_week_df.index
        days_of_week_df['Timeindex'] = days_of_week_df['Timepoint'].map(change_str_to_int)
        days_of_week_df = days_of_week_df.drop(['Timepoint'], axis=1)
  
        combined_df = pd.merge(longform_prices_df, days_of_week_df, how='inner', on='Timeindex')
        graph_from_dataframe(combined_df, 1, mode, start_date, end_date, display_to_screen)
        
    return


def graph_from_dataframe(dataframe, figure_number, mode, start_date, end_date, display_to_screen):
  
    #Function:    Plots a graph based on investments in the drive table, for prices within the date range.
    #Input:       Start Date, End Date, Title of graph, whether to show legend or not
    #Output:      None
    
    if DEBUG:
        print("in graph_from_dataframe")
   
    output_path_name = REPORT_FOLDER_OSX
        
    if mode == 'MATPLOTLIB':   
        fig, (ax1) = plt.subplots(figure_number, sharex=True, sharey=True)
        dataframe.plot(ax=ax1)
        ttitle = 'Matplotlib from ' + start_date + ' to ' + end_date
        output_file_name = "%sMATPLOTLIB.png" % (output_path_name)
        ax1.set_title(ttitle)
        
        # legend to right hand side outside of box
        
        box = ax1.get_position()
        ax1.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax1.legend(loc='center left', bbox_to_anchor=(1, 0.5), ncol=1, fancybox=True, fontsize='small')
        
    if mode == 'SEABORN_TS_PLOT':
        fig, (ax1) = plt.subplots(figure_number, sharex=True, sharey=True)
        
        # Note: TSPLOT goes weird if cross the month end
        
        sns.set(style="darkgrid")
        ax1 = sns.tsplot(data=dataframe, time= "Timepoint", unit="Ticker", condition="Ticker", value="Price" )
        ttitle = 'Seaborn Tsplot from ' + start_date + ' to ' + end_date 
        output_file_name = "%sSEABORN_TSPLOT.png" % (output_path_name)
        ax1.set_title(ttitle)
        
        # legend to right hand side outside of box
        
        box = ax1.get_position()
        ax1.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax1.legend(loc='center left', bbox_to_anchor=(1, 0.5), ncol=1, fancybox=True, fontsize='small')
    
    if mode == 'SEABORN_LM_PLOT':
        sns.set(style="darkgrid")
        sns.lmplot(x="Timeindex", y="Price" ,data=dataframe, hue="Ticker", truncate=True)
        ax1 = plt.gca()
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)  # shrink fig so cbar is visible
        ttitle = 'Seaborn Tsplot from ' + start_date + ' to ' + end_date 
        ax1.set_title(ttitle)
        output_file_name = "%sSEABORN_LMPLOT.png" % (output_path_name)
     
    if mode == 'SEABORN_BOX_PLOT':
        fig, (ax1) = plt.subplots(figure_number, sharex=True, sharey=True)
        sns.set(style="darkgrid")
        sns.boxplot(x="Timepoint", y="Price", data=dataframe)
        ax1 = plt.gca()
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)  # shrink fig so cbar is visible
        ttitle = 'Seaborn Boxplot from ' + start_date + ' to ' + end_date
        ax1.set_title(ttitle)
        output_file_name = "%sSEABORN_BOXPLOT.png" % (output_path_name)
    
    if mode == 'SEABORN_JOINT_PLOT':
        sns.set(style="darkgrid")
        grid = sns.jointplot(x="Timeindex", y="Price" ,data=dataframe)
        ax1 = plt.gca()
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)  # shrink fig so cbar is visible
        ttitle = 'Seaborn Jointplot from ' + start_date + ' to ' + end_date 
        plt.suptitle(ttitle, fontsize=14)
        output_file_name = "%sSEABORN_JOINTPLOT.png" % (output_path_name)
    
    if mode == 'SEABORN_HEXBIN_PLOT':
        sns.set(style="white")
        grid = sns.jointplot(x="Timeindex", y="Price" ,data=dataframe, kind="hex", color="k")
        ax1 = plt.gca()
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)  # shrink fig so cbar is visible
        ttitle = 'Seaborn Jointplot from ' + start_date + ' to ' + end_date 
        plt.suptitle(ttitle, fontsize=14)
        output_file_name = "%sSEABORN_JOINTPLOT.png" % (output_path_name)
    
    if mode == 'SEABORN_KDE_PLOT':
        sns.set(style="white")
        grid = sns.jointplot(x="Timeindex", y="Price" ,data=dataframe, kind="kde", color="k")
        ax1 = plt.gca()
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)  # shrink fig so cbar is visible
        ttitle = 'Seaborn Kernel Density Plot from ' + start_date + ' to ' + end_date 
        plt.suptitle(ttitle, fontsize=14)
        output_file_name = "%sSEABORN_KDEPLOT.png" % (output_path_name)
    
    if mode == 'SEABORN_KDE_GRID_PLOT':
        grid = sns.jointplot(x="Timeindex", y="Price" ,data=dataframe, kind="kde", color="m")
        grid.plot_joint(plt.scatter, c="w", s=30, linewidth=1, marker="+")
        
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)  # shrink fig so cbar is visible
        ttitle = 'Seaborn KDE Joint Grid Plot from ' + start_date + ' to ' + end_date 
        plt.suptitle(ttitle, fontsize=14)
        output_file_name = "%sSEABORN_KDE_GRID_PLOT.png" % (output_path_name)
    
    if mode == 'SEABORN_PAIR_PLOT':
        grid = sns.pairplot(dataframe)
        output_file_name = "%sSEABORN_PAIR_PLOT.png" % (output_path_name)
    
    if mode == 'SEABORN_PAIR_KDE_PLOT':
        grid = sns.pairplot(dataframe)
        grid.map_diag(sns.kdeplot)
        grid.map_offdiag(sns.kdeplot, cmap="Blues_d", n_levels=6);
        output_file_name = "%sSEABORN_PAIR_KDE_PLOT.png" % (output_path_name)
    
    if mode == 'SEABORN_STRIP_PLOT':
        sns.stripplot(x="Timeindex", y="Price", hue="Ticker", data=dataframe)
        output_file_name = "%sSEABORN_STRIP_PLOT.png" % (output_path_name)
  
    #common stuff
    
    plt.savefig(output_file_name)

    if display_to_screen:
        plt.show()
    
    return


if __name__ == "__main__":
    
    print("Started perform_data_analysis_visualization - mode is ", mode, " from ", start_date, " to ", end_date)
    print(" ")
    print("Open Database")
     
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)  
    
    start_date = '2018-03-01'
    end_date   = '2018-03-31'
    
    # Matplotlib basic line graph
    
    display_to_screen = True
    mode              = 'MATPLOTLIB'
    
    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn time series plot
    # Note: TSPLOT goes weird if cross the month end
    
    display_to_screen = True
    mode              = 'SEABORN_TS_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)

    # Seaborn im plot
    
    display_to_screen = True
    mode              = 'SEABORN_LM_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
 
    # Seaborn box plot
    
    display_to_screen = True
    mode              = 'SEABORN_BOX_PLOT'
    
    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn joint plot
    
    display_to_screen = True
    mode              = 'SEABORN_JOINT_PLOT'
    
    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn hexbin plot
    
    display_to_screen = True
    mode              = 'SEABORN_HEXBIN_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn kde plot
    
    display_to_screen = True
    mode              = 'SEABORN_KDE_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn kde joint grid plot
    
    display_to_screen = True
    mode              = 'SEABORN_KDE_GRID_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn pair wise plot
    
    display_to_screen = True
    mode              = 'SEABORN_PAIR_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn pair wise plot with KDE on top
    
    display_to_screen = True
    mode              = 'SEABORN_PAIR_KDE_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    # Seaborn stripplot
    
    display_to_screen = True
    mode              = 'SEABORN_STRIP_PLOT'

    perform_data_analysis_visualization(database          = database, 
                                        mode              = mode, 
                                        start_date        = start_date, 
                                        end_date          = end_date, 
                                        display_to_screen = display_to_screen)
    
    print("Close database")
    close_db(database = database)   
    
    print(" ")
    print("Finished perform_data_analysis_visualization") 
    print(" ")
    