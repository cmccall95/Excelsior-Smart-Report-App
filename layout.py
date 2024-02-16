import dash
from dash import Dash, html, dcc
from dash_extensions.javascript import Namespace
import dash_tabulator
#from dash import html, dcc#, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
import os
from dotenv import load_dotenv
import logging


#Project file imports
from connect_qb import get_headers, get_tpsl_row_data, get_table_data, transform_data, get_job_setup, get_continuity
from hierarchy import select_df_columns, dataframe_to_tree, generate_columns
from rt_report_lib import rt_clicked, rt_option_clicked, get_gap_report_type

#Enable the current logger
logger = logging.getLogger(__name__)

def initialize_store_data(exclude_keys=None):
    # Define all keys that will be in store_data
    keys = ["job_setup_table", "filtered_df", "trans_df", "tree_data", 
            "by_welder_lot_df", "rt_welder_tree_data", "rt_welder_columns_config",
            "rt_welder_option_tree_data_filtered", "rt_welder_option_columns_config_df",
            "rt_welder_option_field_mapping", "by_welder_option_unfiltered_df",
            "by_welder_option_filtered_df", "rt_welder_option_columns_config_filtered",
            "df_table", "weld_log_field_mapping", "rt_welder_field_mapping",
            "gap_data", "gap_columns_config", "pt_data", "pt_columns_config",
            "continuity_data", "continuity_field_mapping"]

    # Initialize all keys with empty strings, except those in exclude_keys
    store_data = {key: '' for key in keys if key not in exclude_keys}
    return store_data


def layout(record_id):#, job_val=None, pkg_val=None):
    #Get dictionary values
    if record_id is None:
        return html.Div([
                #dcc.Location(id='url', refresh=False),
                html.H1("Record ID is 'NONE'. Nothing to display.", style={"color": "red"}),
                html.H2("", style={"color": "red"}),
                html.H3(f"Verify you have sufficient permissions to access the requested content at 'Quickbase' or you have passed a record id"),
            ])
    else:
        store_data = run_functions(record_id)

        #If the dataframe returned is empty, display None
        if store_data is None:
            logger.warning("Your data is empty and no tables will be a available...")

        #print("STORE DATA: \n", store_data)

        try:
            job_val, pkg_val, option_val, job_type, tpsl_full_df, tpsl_filtered_df, tpsl_field_mapping = store_data['tpsl_data']
        except:
            job_val = None
            pkg_val = None
            job_type = None
    
        #if job_type == "Construction":
        if job_type == "Construction" and (job_val is None or pkg_val is None):
            #if job_val is None or pkg_val is None:
            job_val = job_val
            pkg_val = ""
            return html.Div([
                #dcc.Location(id='url', refresh=False),
                html.H1("Nothing to display.", style={"color": "red"}),
                html.H2(f"Job: {job_val if job_val != 'none' else 'None'}, Test Package: {pkg_val if pkg_val != 'none' else 'None'}", style={"color": "red"}),
                html.H2(f"Test Package: {pkg_val if pkg_val != 'none' else 'None'}", style={"color": "red"}),
                html.H2("", style={"color": "red"}),
                html.H3(f"Verify you have sufficient permissions to access the requested content at 'Quickbase Record ID': {record_id}", style={"color": "red"}),
            ])

        else:

            # Create an empty DataFrame
            df_empty = pd.DataFrame()

            # Initialize DashTabulator components with the empty DataFrame
            standard_table = dash_tabulator.DashTabulator(id='standard-table', data=df_empty.to_dict('records'))
            hierarchy_table = dash_tabulator.DashTabulator(id='hierarchy-table', data=df_empty.to_dict('records'))
            rt_hierarchy_table = dash_tabulator.DashTabulator(id='rt-hierarchy-table', data=df_empty.to_dict('records'))
            rt_option_table = dash_tabulator.DashTabulator(id='rt-option-table', data=df_empty.to_dict('records'))

            return html.Div([  
                #dcc.Location(id='url', refresh=False),
                # Title
                html.Div([
                    html.H1(f"JOB #: {job_val}", style={"textAlign": "center"}),
                    html.H2(f"Test Package '{pkg_val}' Reports", style={"textAlign": "center"})
                ], style={"padding": "10px"}),

                #Notification element for download
                dbc.Toast(
                [
                    html.P("Click the link below to download:"),
                    html.A(id='download-link', children='Downloads', href='', download='your_filename.pdf', 
                           target='_blank', style={"display": "flex", "alignItems": "center"})
                ],
                id="notification",
                header="Download Ready",
                is_open=False,
                dismissable=True,
                icon="success",
                # top: 66 positions the toast below the navbar
                style={"position": "fixed", "top": 20, "right": 10, "width": 350},
            ),

                # Main content
                html.Div([
                    # Left panel (sidebar)
                    html.Div([
                        dcc.Tabs(id="sidebar-tabs", value='tab-1', children=[
                            dcc.Tab(label='Package Reports', value='tab-1', children=[
                                html.P("Test Package Reports", style={"fontWeight": "bold"}),
                                # Tree structure for Package Reports
                                html.Ul([
                                    html.Li([
                                        f"'{pkg_val}' Weld Log",
                                        html.Ul([
                                            html.Li([
                                                html.Div([
                                                    html.A(f"Weld Log", id="standard-link", href="#", style={"color": "blue", "display": "inline-block"}),
                                                    html.Img(id="weld-error-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})
                                                ], style={"display": "flex", "alignItems": "center"})
                                            ]),
                                        ]),
                                    ]),
                                    html.Li([
                                        f"'{pkg_val}' NDE Reports",
                                        html.Ul([
                                            # RT-By Welder
                                            html.Li([
                                                html.Div([
                                                    html.A("RT-By Welder", id="rt-tree-link", href="#", style={"color": "blue", "display": "inline-block"}),
                                                    html.Img(id="rt-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})  # Initially hidden
                                                ], style={"display": "flex", "alignItems": "center"}),
                                            ]),
                                            # RT-By Option
                                            html.Li([
                                                html.Div([
                                                    html.A("RT-By Option", id="rt-option-link", href="#", style={"color": "blue", "display": "inline-block"}),
                                                    html.Img(id="rt-option-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})  # Initially hidden
                                                ], style={"display": "flex", "alignItems": "center"}),
                                            ]),
                                            # RT-By Welder/Option
                                            html.Li([
                                                html.Div([
                                                    html.A("RT-By Welder/Option", id="rt-welder-option-filtered-link", href="#", style={"color": "blue", "display": "inline-block"}),
                                                    html.Img(id="rt-welder-option-filtered-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})  # Initially hidden
                                                ], style={"display": "flex", "alignItems": "center"}),
                                            ]),
                                            # GAP Report
                                            html.Li([
                                                html.Div([
                                                    html.A("GAP Report", id="gap-tree-link", href="#", style={"color": "blue", "display": "inline-block"}),
                                                    html.Img(id="gap-error-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})  # Initially hidden
                                                ], style={"display": "flex", "alignItems": "center"})
                                            ]),
                                            # PT/MT Report
                                            html.Li([
                                                html.Div([
                                                    html.A("PT/MT Report", id="pt-tree-link", href="#", style={"color": "blue", "display": "inline-block"}),
                                                    html.Img(id="pt-error-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})  # Initially hidden
                                                ], style={"display": "flex", "alignItems": "center"})
                                            ]),                                            
                                        ]),
                                    ]),
                                ]),
                            ]),
                            dcc.Tab(label='Manager Review', value='tab-2',children=[
                                html.P("For Review", style={"fontWeight": "bold"}),
                                html.Ul([
                                    html.Li([
                                        html.Div([
                                            html.A("Weld Log", id="tree-link", href="#", style={"color": "blue"}),
                                            html.Img(id="weld-error-icon-1", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})
                                        ], style={"display": "flex", "alignItems": "center"})
                                    ]),
                                    html.Li([   # New list item for "Exception Report"
                                        html.Div([
                                            html.A("Exception Report", id="exception-report-link", href="#", style={"color": "blue"}), 
                                            # Assuming you want an error icon for this link too
                                            html.Img(id="exception-error-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})
                                        ], style={"display": "flex", "alignItems": "center"})
                                    ]),
                                    html.Li([   # New list item for "Welder Continuity"
                                        html.Div([
                                            html.A("Welder Continuity", id="welder-continuity-link", href="#", style={"color": "blue"}),
                                            # Assuming you want an error icon for this link too
                                            html.Img(id="welder-continuity-error-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})
                                        ], style={"display": "flex", "alignItems": "center"})
                                    ]),
                                    html.Li([   # New list item for "Welder Continuity"
                                        html.Div([
                                            html.A("RT - Full Job", id="job-rt-link", href="#", style={"color": "blue"}),
                                            # Assuming you want an error icon for this link too
                                            html.Img(id="job-rt-icon", src="assets/warning_icon.svg", style={"height": "15px", "width": "15px", "display": "none"})
                                        ], style={"display": "flex", "alignItems": "center"})
                                    ])
                                ])  # This ensures that the list uses bullet points
                            ])  # Closing 'Manager Review' tab
                        ]),  # Closing dcc.Tabs
                    ], className="left-panel"),

                    # Right panel (table container)
                    html.Div([
                        html.Div([
                            html.Div([  # New flex container for title and icon
                            html.Button(
                                html.Img(id="icon-img", src='assets/print_icon.svg', style={"height": "20px", "width": "20px"}),
                                id="print-icon", 
                                style={"position": "absolute", "left": "0", "background": "none", "border": "none"}  # Added CSS to remove button styling
                            ),  
                            html.Div(id="table-title", children="", style={"textAlign": "center", "fontSize": "20px"}),  
                        ], style={"position": "relative"}),

                            # Your modal with an ID for the ModalBody
                            dbc.Modal(
                                [
                                    dbc.ModalHeader("Message"),
                                    dbc.ModalBody("Export to PDF is currently disabled at this time. Please try again.",
                                                id="modal-body"),  # Assigning an ID here
                                    dbc.ModalFooter(
                                        dbc.Button("Close", id="close-modal", className="ml-auto")
                                    )
                                ],
                                id="modal",
                                is_open=False,  # Starts out hidden
                            ),

                            html.Div(className="loader-wrapper", children=[
                                dcc.Loading(
                                    id="loading",
                                    type="cube",
                                    fullscreen=True,
                                    color="#3A96D8",
                                    children=[
                                        html.Div(id="table-container", children=[
                                        ]),
                                    ],
                                    className="custom-loading"
                                )
                            ]),
                            html.P("Sit tight while we process your data...", id='loading-text', className='loading-text', style={'textAlign': 'center', 'display': 'none'}),
                        ], id='loading-container', className="right-panel")
                    ], className="content-container"),
                ], className="main-container", style={"display": "flex", "flexDirection": "row"}),


                # Adding a dcc.Store to hold real-time data
                dcc.Store(id='tpsl-values', data=store_data.get("tpsl_data", {})),

                #Weld Log Data
                dcc.Store(id='weld-data-store', data=store_data.get("trans_df", {})),
                dcc.Store(id='weld-data-tree-store', data=store_data.get("tree_data", {})),
                dcc.Store(id='weld-log-field-mapping', data=store_data.get("weld_log_field_mapping", {})),
                #RT By Welder Data Store
                dcc.Store(id='rt-data-store', data=store_data.get("rt_welder_tree_data", {})), #By Welder
                dcc.Store(id='rt-data-store-flat', data=store_data.get("by_welder_lot_df", {})), #By Welder
                dcc.Store(id='rt-welder-columns-config', data=store_data.get("rt_welder_columns_config", {})),
                dcc.Store(id='rt-welder-field-mapping', data=store_data.get("rt_welder_field_mapping", {})),

                #dcc.Store(id='rt-option-data-store', data=store_data.get("rt_welder_option_tree_data_df", {})), #RT-By Option data
                #dcc.Store(id='rt-option-field-mapping'), 
            
                #dcc.Store(id='rt-option-columns-config', data=store_data.get("rt_welder_option_columns_config_df", {})), 

                #RT By WElder/Option Data Store
                dcc.Store(id='rt-welder-option-data-filtered-store', data=store_data.get("rt_welder_option_tree_data_filtered", {})), #RT By Welder/Option filtered for only LOTs and Welders in the particular test package
                dcc.Store(id='rt-welder-option-data-filtered-store-flat', data=store_data.get("by_welder_option_filtered_df", {})),
                dcc.Store(id='rt-welder-option-columns-config', data=store_data.get("rt_welder_option_columns_config_df", {})),
                dcc.Store(id='rt-welder-option-field-mapping', data=store_data.get("rt_welder_option_field_mapping", {})),
                

                dcc.Store(id='rt-welder-option-data-unfiltered-store'), #RT By Welder/Option not filtered. Includes all LOTs if the welder is in that test package

                #RT Gap Data Store
                dcc.Store(id='gap-store', data=store_data.get("gap_data", {})),
                dcc.Store(id='gap-columns-config', data=store_data.get("gap_columns_config", {})),
                
                #PT/MT Data Store
                dcc.Store(id='pt-mt-store', data=store_data.get("pt_data", {})),
                dcc.Store(id='pt-mt-columns-config', data=store_data.get("pt_columns_config", {})),
                
                #Continuity Data Store
                dcc.Store(id='continuity-data-store', data=store_data.get("continuity_data", {})),
                dcc.Store(id='continuity-field-mapping', data=store_data.get("continuity_field_mapping", {})),
            
                #Other Data Store
                dcc.Store(id='current-table-id'),
                dcc.Store(id='weld-log-highlight-welds'),
                dcc.Store(id='exception-report'),
                html.Div(id='job-type', style={"height": "20px", "width": "20px"}),
                html.Div(id='dummy-output', style={'display': 'none'}),
                html.Div(id='client-side-dummy-output', style={'display': 'none'}),
                html.Div(id='hidden-error-log-1', style={'display': 'none'}),
                html.Div(id='hidden-error-log-2', style={'display': 'none'}),
                html.Div(id='hidden-error-log-3', style={'display': 'none'}),
                html.Div(id='hidden-seen-errors-1', style={'display': 'none'}),
                html.Div(id='hidden-seen-errors-2', style={'display': 'none'}),
                html.Div(id='hidden-seen-errors-3', style={'display': 'none'}),

            ])  # This closing bracket is for the main 'html.Div' under the 'else' condition
        


                # Initialize DashTabulator components with the empty DataFrame
                #dash_tabulator.DashTabulator(id='standard-table', data=df_empty.to_dict('records')), #Unhide standard table and hierarchy table if errors
                #dash_tabulator.DashTabulator(id='hierarchy-table', data=df_empty.to_dict('records')),

                #dash_tabulator.DashTabulator(id='rt-hierarchy-table', data=df_empty.to_dict('records'))
                #dash_tabulator.DashTabulator(id='rt-option-table', data=df_empty.to_dict('records'))

            #])  # This closing bracket is for the main 'html.Div' under the 'else' condition


def run_functions(record_id):

    load_dotenv()  # take environment variables from .env.

    QB_REALM_HOSTNAME = os.getenv('QB_HOSTNAME')
    QB_TOKEN = os.getenv('QB_TOKEN')
    QB_USER_AGENT = os.getenv('QB_USER_AGENT')
    
    APP_ID = os.getenv('APP_ID')
    WELD_LOG_ID = os.getenv('WELD_LOG_ID')
    JOB_STENCIL_LOTS_ID = os.getenv('JOB_STENCIL_LOTS_ID')
    JOB_STENCIL_OPTION_LOTS_ID = os.getenv('JOB_STENCIL_OPTION_LOTS_ID')
    TPSL_ID = os.getenv('TPSL_ID')
    JOB_SETUP_TABLE = os.getenv('JOB_SETUP_TABLE')
    WELDER_CONT_TABLE = os.getenv('WELDER_CONTINUITY_TABLE')

    logger.info("-- Python-dotenv loaded. Variables: ")

    logger.info("Record ID: " + str(record_id))

    test_pkg_parent_keys = ['6', '11', '8']
    test_pkg_child_keys = ['15']

    show_columns = ['6','76','14','11','15','10','8','16','9','17','20','21','49','22',
                            '23','18','19','43','28','30','31','32','219','207','36','37','38','206','52',
                            '24','90', '95','97','140','141','208','209','210','211','213','212','202', '203', 
                            '78', '77', '3', '225', '227', '228', '229','230','_error'] #'202', '203', '78', '77'] Lot info '35'(PT Final RPT #) replaced by 219

    drop_columns = ['14', '95', '97', '208', '209', '210', '211', '212', '213', '140', '141', '3', '_error']  #'76', '77', '78' Lot Option Selection
    #'140', '141',

    # Specify the name of the hierarchical column
    tree_column_name = ""

    # Register the custom namespace
    ns = Namespace("myNamespace", "tabulator")

    #Call functions
    print("\n 1. Getting Headers \n")
    headers = get_headers()

    print("\n 2A.Get TPLS INFO \n")
    if record_id:
        tpsl_data = get_tpsl_row_data(headers, APP_ID, TPSL_ID, record_id)
        job_val, pkg_val, option_val, job_type, tpsl_df, tpsl_filtered_df, tpsl_field_mapping = tpsl_data
        #print("Getting TPSL DATA:", tpsl_data)
        logger.info("\n\nFetched TPSL DATA:","\nJob:", job_val, "\nPKG:", pkg_val, "\nOPTION:", option_val, "\nJOB TYPE:", job_type)
    else:
        print('\n TPSL ROW REQUEST FAILED\n')
        tpsl_data = None, None, None, None, None, None, None

    if job_type == 'Construction':
        query_columns = ["3","6","14","8","9","10","11","76","15","16","17","18","19","20","21","49","52","22","23","24","28","30","43", 
                    "31","32","33","34","35","219","207","206","36","37","38","212","213","214","90","208","209","210","211","95", 
                        "97","140","141","71","39","45","47","81","83","55","56","91","40","46","48","85","87","62","63","92", "202",
                        "203", "77", "78", "225", "227", "228", "229", "230"], 
        check_error = False
    elif job_type == 'Fab Shop':
        pkg_val = ""
        check_error = False
    elif job_type == 'Maintenance':  
        check_error = True
    else:
        check_error = True

    logger.info(f"Job Type Setting: {job_type}")

    print("\n 2B. If TPSL Data is not None, resume. Else, cancel function calls \n")

    if job_val is not None and pkg_val is not None:
        #print("\n 3. Get table data using values TPSL Data \n")
        filtered_df, weld_log_field_mapping = get_table_data(headers,APP_ID, WELD_LOG_ID, tpsl_data)
        
        qb_request_rows = len(filtered_df)

        #Check if dataframe has any rows
        logger.info(f"QuickBase Query returned {qb_request_rows} rows.")

        if qb_request_rows > 0:

            #print("\n 4. Insert a new row for every R1 and R2 \n")
            trans_df, repair_hierarchy = transform_data(filtered_df, weld_log_field_mapping)

            print("\n 7. Select columns for displayed table and drop blank columns \n")
            trans_df = select_df_columns(trans_df, show_columns)

            #Format "<br>" to new line
            trans_df['219'] = trans_df['219'].str.replace('<br>', '\n')

            # Generate column configurations //No longer in use
            #test_pkg_columns_config = generate_columns(weld_log_field_mapping, trans_df, tree_column_name, test_pkg_parent_keys, test_pkg_child_keys)

            # Convert DataFrame to tree structure
            tree_data = dataframe_to_tree(trans_df, test_pkg_parent_keys, test_pkg_child_keys, repair_hierarchy)

            #Check dataframe is not blank before proceeding
            if trans_df.shape[0] > 0:
                #print("TRANS DF HAS ROWS:", trans_df)

                #Get RT by Welder Data
                by_welder_lot_df, rt_welder_tree_data, rt_welder_columns_config, rt_welder_field_mapping = rt_clicked(trans_df, tpsl_data)

                #print("\n\n>> Run Functions: \nBy Welder DF:", by_welder_lot_df, "\n\n RT Welder_tree_data: \n", rt_welder_tree_data, "\n\nOption Val: ", option_val)

                #Check if Option is selected
                if option_val is not None or option_val != "":
                    #Get Welder/Option RT Data Unfiltered/Filtered
                    by_welder_option_unfiltered_df, rt_welder_option_tree_data_df, rt_welder_option_columns_config_df, rt_option_field_mapping_df = rt_option_clicked(trans_df, tpsl_data, "BW", "RT", filtered=False)
                    by_welder_option_filtered_df, rt_welder_option_tree_data_filtered, rt_welder_option_columns_config_filtered, rt_option_field_mapping_filtered = rt_option_clicked(trans_df, tpsl_data, "BW", "RT", filtered=True)
                    
                    # Check if returned dataframes are None or empty
                    if by_welder_option_filtered_df is not None and by_welder_option_filtered_df.empty:
                        by_welder_option_filtered_df = None
                        rt_welder_option_tree_data_filtered = None

                    #... and so on for other DataFrames

                    #print("\n\nWELDER/OPTION:", by_welder_option_filtered_df)

                df_table = trans_df #field_id_to_field_name(trans_df, weld_log_field_mapping)

                #Assign unique id column to dataframe
                df_table.reset_index(drop=True, inplace=True)

                #>>>>If SW in test package, calculate on full data

                #gap_data = get_gap_report_type(df_table)

                #Get Filtered Gap Report
                gap_filtered_df, gap_data_filtered, gap_columns_config_filtered, gap_mapping_df = rt_option_clicked(trans_df, tpsl_data, "SW", "Gap", filtered=True)
                
                #Get Filtered PT Report
                pt_filtered_df, pt_data_filtered, pt_columns_config_filtered, pt_mapping_df = rt_option_clicked(trans_df, tpsl_data, "SW", "PT/MT", filtered=True)

                #print("\n\nGAP DATA:", gap_filtered_df)

                #Get Welder Continuity Table
                df_cont, cont_field_mapping = get_continuity(headers, APP_ID, WELDER_CONT_TABLE, tpsl_data)

                #Reassign the tpsl_data variable to remove the dataframes before storing
                sep_tpsl_data = job_val, pkg_val, option_val, job_type

                store_data = {
                    #"headers": headers,
                    "tpsl_data": sep_tpsl_data,
                    "full_tpsl": tpsl_df.to_dict(orient="records") if tpsl_df is not None else None,
                    "filtered_tpsl": tpsl_filtered_df.to_dict(orient="records") if tpsl_filtered_df is not None else None,
                    "job_setup_table": '',
                    "filtered_df": filtered_df.to_dict(orient="records") if filtered_df is not None else None,
                    "trans_df": trans_df.to_json(orient="records") if trans_df is not None else None,
                    #"test_pkg_columns_config": test_pkg_columns_config,
                    "tree_data": tree_data,
                    "by_welder_lot_df": by_welder_lot_df.to_dict(orient="records") if by_welder_lot_df is not None else None,

                    "rt_welder_tree_data": rt_welder_tree_data,
                    "rt_welder_columns_config": rt_welder_columns_config,

                    "rt_welder_option_tree_data_filtered": rt_welder_option_tree_data_filtered,
                    "rt_welder_option_columns_config_df": rt_welder_option_columns_config_df,
                    "rt_welder_option_field_mapping": rt_option_field_mapping_df,


                    #Possible Delete >>>>
                    "by_welder_option_unfiltered_df": by_welder_option_unfiltered_df.to_dict(orient="records") if by_welder_option_unfiltered_df is not None else None,
                    "by_welder_option_filtered_df": by_welder_option_filtered_df.to_dict(orient="records") if by_welder_option_filtered_df is not None else None,
                    #<<<<<


                #"rt_welder_option_tree_data_filtered": rt_welder_option_tree_data_filtered,
                    "rt_welder_option_columns_config_filtered": rt_welder_option_columns_config_filtered,
                    "df_table": df_table.to_dict(orient="records") if df_table is not None else None,
                    "weld_log_field_mapping": weld_log_field_mapping,
                    "rt_welder_field_mapping": rt_welder_field_mapping,

                    "gap_data": gap_data_filtered,
                    "gap_columns_config": gap_columns_config_filtered, 
                    
                    "pt_data": pt_data_filtered,
                    "pt_columns_config": pt_columns_config_filtered,

                    "continuity_data": df_cont.to_json(orient="records") if df_cont is not None else None,
                    "continuity_field_mapping": cont_field_mapping
                }

                return store_data
        else:
            logger.warning(f"Job '{job_val}' has no weld data for Test Package '{pkg_val}'...")
            #Update Store Data Setting all values to blank except TPSL
            store_data = initialize_store_data(exclude_keys=["tpsl_data"])
            #print(store_data)
            store_data["tpsl_data"] = tpsl_data  # Update with non-empty value

            return store_data

    else:
        print("No TPLS data. Do nothing.")
        return None
