#main : Creates webapp
import logging
import platform
import dash_bootstrap_components as dbc
import dash
from flask import Flask
from dash import Dash, html, dcc #, Output, Input
from dash.dependencies import Output, Input, State, ALL
from dash.dash_table.Format import Group
from dash import clientside_callback, ClientsideFunction, Input, Output
from dash_tabulator import DashTabulator
from dash_extensions.javascript import Namespace 
import webbrowser
from threading import Timer
import json
import pandas as pd
import urllib.parse as parse
from waitress import serve
from dotenv import load_dotenv
import os
import base64
#from flask_cors import CORS #<< Simulate Cors

#Project file imports
from layout import layout
from connect_qb import get_headers, get_tpsl_row_data, get_table_data, transform_data, get_rt_report, get_job_setup
from hierarchy import build_hierarchy_data, select_df_columns, remove_selected_columns, field_id_to_field_name, field_id_to_field_name_for_tabulator, dataframe_to_tree, generate_columns
from error_lib import check_weld_repairs, check_rt_status, process_error_log, transform_exceptions
from rt_report_lib import flatten_data, rt_clicked, rt_option_clicked, create_gap_report, flatten_data
from create_pdfs import create_print_dataframe, table_to_pdf, create_groups, create_groups_from_paths

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

#http://localhost:8080/?parentRid=34

# Enable Logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logging.info("-- Python Logging enabled")

# Register the custom namespace
ns = Namespace("myNamespace", "tabulator")

#Arguments to be passed
record_id = None

#print(record_id)

def test_layout(record_id):
    # assets_path = './assets'  # Adjust the path if necessary
    # try:
    #     file_list = os.listdir(assets_path)
    #     files_str = ', '.join(file_list)
    # except FileNotFoundError:
    #     files_str = "Assets directory not found."
        
    # print(files_str)
            
    if record_id is None:
            return html.Div([
                    #dcc.Location(id='url', refresh=False),
                    html.H1("Record ID is 'NONE'. Nothing to display.", style={"color": "red"}),
                    html.H2("", style={"color": "red"}),
                    html.H3(f"Verify you have sufficient permissions to access the requested content at 'Quickbase' or you have passed a record id"),
                ])
    else:
        return html.Div([
                    #dcc.Location(id='url', refresh=False),
                    html.H1(f"Record ID is '{record_id}'. "),
                    html.H3(f"Function is working"),
                ])
        
def tree_to_dataframe(tree, parent_data=None): #Converts tree to dataframe (works but displays "name" for the lot field because the field id is not carried over to the data)
    rows = []
    if parent_data is None:
        parent_data = {}

    for item in tree:
        current_data = parent_data.copy()  # Copy the parent_data to ensure it's not overwritten

        # Current node data
        for key, value in item.items():
            if key != "_children":
                current_data[key] = value

        if "_children" in item:
            rows.extend(tree_to_dataframe(item["_children"], current_data))
        else:
            rows.append(current_data)

    return rows

def bytesio_to_base64(bytes_io):
    """Converts a BytesIO object into a base64 encoded string."""
    return base64.b64encode(bytes_io.read()).decode('utf-8')

def construct_tabulator_table(data, columns, options, downloadButtonType=None, table_id="table-id"):
    if not data or len(data) < 1:  # checks if data is None or empty
        return "No data to display"

    return DashTabulator(
        id=table_id,
        data=data,
        columns=columns,
        options=options,
        downloadButtonType=downloadButtonType,
    )

external_scripts=[
    'https://unpkg.com/tabulator-tables@5.5.0/dist/js/tabulator.min.js', 
    #'/assets/printingFunc.js',
    'https://printjs-4de6.kxcdn.com/print.min.js',
    'https://oss.sheetjs.com/sheetjs/xlsx.full.min.js',
    #'https://cdnjs.cloudflare.com/ajax/libs/jspdf/1.5.3/jspdf.min.js',
    'https://cdnjs.cloudflare.com/ajax/libs/jspdf/1.5.3/jspdf.debug.js',
    'https://cdnjs.cloudflare.com/ajax/libs/jspdf-autotable/3.0.5/jspdf.plugin.autotable.js'
    
]
          
external_stylesheets = [dbc.themes.BOOTSTRAP, '/assets/styles.css'] 

#Initialize
flask_server = Flask(__name__)
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, external_scripts=external_scripts, server= flask_server )

# Keep the layout structure static, but the content dynamic
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='dynamic-content'),  # This div will contain your dynamic layout
])

# Define callback to update the displayed record ID based on URL
@app.callback(
    Output('dynamic-content', 'children'), #<<<Fix
    [Input('url', 'search')]
)
def update_record_id(search):
    # Extract record_id from the query string
    record_id = None
    if search:
        query_params = {k: v[0] for k, v in parse.parse_qs(search.lstrip('?')).items()}
        record_id = query_params.get('parentRid')

    # Generate your dynamic layout based on record_id Uncomment for production use
    #dynamic_layout = test_layout(record_id) #'34'
    # Generate your dynamic layout based on record_id
    dynamic_layout = layout(record_id) #'34'
    
    return dynamic_layout

@app.callback(
    [Output('table-container', 'children'),
     Output('current-table-id', 'data'),
     Output('table-title', 'children'),
     Output("notification", "is_open"),
     Output('download-link', 'href'),
     Output('download-link', 'download'),
     Output('download-link', 'children'),
     Output('modal-body', 'children'),
     Output('modal', 'is_open')
     ],
    [
        Input('print-icon', 'n_clicks'),
        Input('standard-link', 'n_clicks'),
        Input('tree-link', 'n_clicks'),
        Input('rt-tree-link', 'n_clicks'),
        Input('rt-welder-option-filtered-link', 'n_clicks'),
        Input('gap-tree-link', 'n_clicks'),
        Input('exception-report-link', 'n_clicks'),
        Input('welder-continuity-link', 'n_clicks'),
        Input('pt-tree-link', 'n_clicks'),
        Input('close-modal', 'n_clicks'),
        
        
    ],
    [
        State('weld-data-store', 'data'),
        State('rt-data-store', 'data'),
        State('rt-welder-option-data-filtered-store', 'data'),
        State('gap-store', 'data'),
        State('gap-columns-config', 'data'),
        State('exception-report', 'data'),
        State('current-table-id', 'data'),
        State('weld-log-field-mapping', 'data'),
        State('weld-data-tree-store', 'data'),
        State('rt-welder-field-mapping', 'data'),
        State('rt-welder-columns-config', 'data'),
        State('rt-welder-option-columns-config', 'data'),
        State('rt-welder-option-field-mapping', 'data'),
        State('tpsl-values', 'data'),
        State('continuity-data-store', 'data'),
        State('continuity-field-mapping', 'data'),
        State('pt-mt-store', 'data'),
        State('pt-mt-columns-config', 'data'),
        ]
)
def combined_callback(print_btn_clicks, standard_clicks, tree_clicks, rt_tree_clicks,
                      rt_option_filtered, gap_clicks, exception_clicks, continuity_clicks, pt_clicks, close_modal, weld_data, rt_data, rt_option_data_filtered, 
                      gap_store, gap_columns_config, exception_data, current_table_id, weld_log_field_mapping, tree_data, rt_welder_field_mapping,
                     rt_welder_columns_config, rt_welder_option_columns_config, rt_option_mapping, tpsl_data, cont_data, cont_field_mapping, pt_data, pt_columns_config):

    ctx = dash.callback_context
    drop_columns = ['76', '77', '78' ,'14', '95', '97', '208', '209', '210', '211', '212', '213', '140', '141', '3', '_error'] 
     #'140', '141',
    #print("tpsl_data: ", tpsl_data)
    job_val, pkg_val, option_val, job_type = tpsl_data

    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger_id == 'print-icon':
        msg_body = [
            html.P([
                html.Em("Export to PDF functionality is currently disabled at this time. Please try again later.")
                 ]),
            html.P([
                html.Strong("error_description:"),  # Bold text
                html.Em(f" User requests access to {current_table_id}.")
            ]),
            html.P([
                "For help, please contact the developer at: ",
                html.A('cmccall@excelusa.com', href='mailto:cmccall@excelusa.com')

            ])
        ]


        #--------------------------------------------------
            ###Function Disabled
        #--------------------------------------------------
        print(msg_body)
        return(dash.no_update, dash.no_update, dash.no_update, dash.no_update, 
               dash.no_update, dash.no_update, dash.no_update, msg_body, True)
        #--------------------------------------------------
            #Function Disabled
        #--------------------------------------------------

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        
        #Initialize blank dataframe
        df_print = pd.DataFrame()

        if current_table_id == 'standard-table':
            tbl_data = json.loads(weld_data)
            df_print = pd.DataFrame(tbl_data)
            #Drop Columns
            df_print = remove_selected_columns(df_print, drop_columns)
            df_print = field_id_to_field_name(df_print, weld_log_field_mapping)

            #Document properties
            print_title = f"Weld Log - {pkg_val}"
            file_name = f"Report_Weld Log - {job_val}  {pkg_val}.pdf"
            doc_size = 'tabloid'
            doc_gridlines = True

        elif current_table_id == 'hierarchy-table':
            pparent_keys = ['6', '11', '8']

            tbl_data = json.loads(weld_data)
            df_temp =  pd.DataFrame(tbl_data)
            #print("Original Dataframe:\n\n", df_print)
            #Drop Columns
            df_temp = remove_selected_columns(df_temp, drop_columns)

            # Ensure column '15' is in the second position
            cols = df_temp.columns.tolist()
            cols.remove('15')
            cols = [cols[0], '15'] + cols[1:]
            df_temp = df_temp[cols]

            df_print = field_id_to_field_name(df_temp, rt_option_mapping)

            #Create tree pdf data
            df_print = create_groups(pparent_keys, df_temp)
            #Convert field ids to field names
            df_print = field_id_to_field_name(df_print, weld_log_field_mapping)

            #Document properties
            print_title = f"Weld Log - {pkg_val}"
            file_name = f"Report_Weld Log_Tree - {job_val}  {pkg_val}.pdf"
            doc_size = 'tabloid'
            doc_gridlines = False

        elif current_table_id == 'rt-hierarchy-table':
            #print(f"CURRENT TABLE ID: {current_table_id}")
            
            pparent_keys = ['6','7']
            
            df_temp = flatten_data(rt_data)

            #print("\n\nPRINT DF TEMP (HIER1)\n", df_temp)
            df_print = create_groups(pparent_keys, df_temp)
                        
            df_print = field_id_to_field_name(df_print, rt_welder_field_mapping)

            #Doc properties
            print_title = "RT Report - Welder"
            file_name = f"Report_RT Welder - {job_val}  {pkg_val}.pdf"
            doc_size = 'letter'
            doc_gridlines = False

        #elif current_table_id == 'rt-option-unfiltered-table':
        #    pparent_keys = ['6', '8', '7']
        #    tbl_data = json.loads(rt_option_data_unfiltered)
        #    df_temp = pd.DataFrame(tbl_data)
        #    df_temp = df_temp.drop(columns=['21'])
        #    df_print = create_groups(pparent_keys, df_temp)
        #    df_print = field_id_to_field_name(df_print, rt_option_field_mapping_uf)

            ##Doc properties
            #print_title = "RT Report - Welder/Option"
            #file_name = f"Report_RT Welder_Option_unfiltered - {job_val}  {pkg_val}.pdf"
            #doc_size = 'letter'
            #doc_gridlines = False

        elif current_table_id == 'rt-option-filtered-table':
            pparent_keys = ['6', '8', '7']
            pparent_keys = ['6', '8', '7', '15']

            #df_temp = pd.DataFrame(tbl_data)
            df_temp = flatten_data(rt_option_data_filtered)
            
            #print("\n\nRT OPTION FILTERED DATAFRAME 2: \n", df_temp)

            #Create Hierarchy Groups
            #df_print = create_groups(pparent_keys, df_temp)
            df_print = create_groups_from_paths(df_temp)
            
            # Check if '9' or 'name' exists and move it to the second position
            column_to_move = '9' if '9' in df_print.columns else ('name' if 'name' in df_print.columns else None)
            if column_to_move:
                cols = df_print.columns.tolist()
                cols.remove(column_to_move)
                cols.insert(1, column_to_move)
                df_print = df_print[cols]

                # Rename the column to 'Lot'
                df_print = df_print.rename(columns={column_to_move: 'Lot'})
                
            #Remove 'path' (created in flattening function)    
            # List of columns to check and drop
            columns_to_drop = ['path', '21']

            # Loop through each column and drop it if it exists
            for column in columns_to_drop:
                if column in df_print.columns:
                    df_print = df_print.drop(columns=[column])

            df_print = field_id_to_field_name(df_print, rt_option_mapping)
            
            #print("\n\nRT OPTION FILTERED DATAFRAME 3: \n", df_print)

            #Doc properties
            print_title = "RT Report - Welder/Option"
            file_name = f"Report_RT Welder_Option_filtered - {job_val} - {pkg_val}.pdf"
            doc_size = 'letter'
            doc_gridlines = False
            #print(f"CURRENT TABLE ID: {current_table_id}")
        else:
            print(f"ERROR: Table {current_table_id} not registering click")

        if not df_print.empty:
            #Function to drop columns that are not needed for printing

            #Output table to PDF

            #print("PRINTING DATAFRAME: \n\n", df_print)
            pdf_bytes = table_to_pdf(print_title, df_print, file_name, doc_size, doc_gridlines)
            #print(f"CURRENT TABLE ID: {current_table_id}")

            # Convert the BytesIO object to a base64 encoded string for download
            encoded_pdf = bytesio_to_base64(pdf_bytes)
            href_data = f"data:application/pdf;base64,{encoded_pdf}"

            #print(href_data)

            return (dash.no_update, dash.no_update, dash.no_update, True, 
                    href_data, file_name, file_name, dash.no_update, False)
        else:
            print(f"{current_table_id} data is empty")
            return(dash.no_update, dash.no_update, dash.no_update, dash.no_update, 
                   dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

    if trigger_id == 'standard-link':
        #Define table title and table_id
        table_title = "Test Package - Standard Weld Log"
        table_id = 'standard-table'

        # Convert the JSON string to a Python data structure
        if len(weld_data) > 0:
            json_data = json.loads(weld_data)
   
            # Extract column names from the first dictionary
            column_names = list(json_data[0].keys()) if json_data else []
            table_columns = field_id_to_field_name_for_tabulator(column_names, weld_log_field_mapping)

            downloadButtonType = {"css": "btn btn-standard", "text": "Export", "type": "xlsx"}
            options={'rowFormatter': ns("weldLogRowFormatter"), 'layout': 'fitDataFill',
                        'selectable': True, 'virtualDom': True, 'pagination': 'local', 'paginationSize': 150}#, 'height': '100%', "minHeight": 400}, 
            #Freeze first 5 columns
            for i in range(5):
                table_columns[i]['frozen'] = True
        else:
           json_data, table_columns, options, downloadButtonType, table_id = None, None, None, None, None 

        #Construct tabulator table
        table = construct_tabulator_table(json_data, table_columns, options, downloadButtonType, table_id)

        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

    elif trigger_id == 'tree-link':
        #Define table title and id
        table_title = "Test Package Weld Log - View: Review"
        table_id = 'hierarchy-table'
        
        #Create the dataframe
        if len(weld_data) > 0:
            data = json.loads(weld_data)
            df = pd.DataFrame(data)

            #Set parameters for the tree
            tree_column_name = ""
            test_pkg_parent_keys = ['6', '8'] #Construction ('6: Job', '11: PKG #', '8: Drawing')
            test_pkg_child_keys = ['15'] #('15: Weld')

            #show_columns = ['6','76','14','11','15','10','8','16','9','17','20','21','49','22',
            #                    '23','18','19','43','28','30','31','32','219','207','36','37','38','206','52',
            #                    '24','90', '95','97','140','141','208','209','210','211','213','212','202', '203', '78', '77', '3', '_error'] 
                                #'202', '203', '78', '77'] Lot info '35'(PT Final RPT #) replaced by 219

            # Generate column configurations
            table_columns = generate_columns(weld_log_field_mapping, df, tree_column_name, test_pkg_parent_keys, test_pkg_child_keys)

            #Set additional options for the tree
            
            table_columns[0]['frozen'] = True
            downloadButtonType = {"css": "btn btn-primary", "text": "Export", "type": "xlsx"}
            options={'rowFormatter': ns("weldLogRowFormatter"), 'dataTree': True, 'dataTreeStartExpanded': True, 
                        'selectable': True, 'layout': 'fitData', 'virtualDom': True, 'pagination': 'local', 'paginationSize': 75}
        
        else:
            json_data, table_columns, options, downloadButtonType, table_id = None, None, None, None, None 

        #Construct tabulator table
        table = construct_tabulator_table(tree_data, table_columns, options, downloadButtonType, table_id)

        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

    elif trigger_id == 'rt-tree-link':
        #Define table parameters
        table_title = "RT Report - By Welder"
        table_id = 'rt-hierarchy-table'
        downloadButtonType = {"css": "btn btn-primary", "text": "Export", "type": "xlsx"}
        options={'rowFormatter': ns("weldLogRowFormatter"), 'dataTree': True, 'dataTreeStartExpanded': True, 
                 'selectable': True, 'layout': 'fitData'}

        #print("RT - By Welder (Data): \n", rt_data )

        #Construct tabulator table
        table = construct_tabulator_table(rt_data, rt_welder_columns_config, options, downloadButtonType, table_id)

        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

    elif trigger_id == 'rt-welder-option-filtered-link':
        #Define table parameters
        table_title = "RT Report Filtered - By Welder/Option"
        table_id = 'rt-option-filtered-table'
        downloadButtonType = {"css": "btn btn-primary", "text": "Export", "type": "xlsx"}
        options={'rowFormatter': ns("rtRowFormatter"), 'dataTree': True, 'dataTreeStartExpanded': True, 
                 'selectable': True, 'layout': 'fitData'}

        # print("RT - By Welder/Option (Data Length): \n", len(rt_option_data_filtered))
        
        # print("\n\nRT Table Columns: ", rt_welder_option_columns_config)
        # print("\n\nRT Data: ", rt_option_data_filtered,)

        #Construct tabulator table
        table = construct_tabulator_table(rt_option_data_filtered, rt_welder_option_columns_config, options, downloadButtonType, table_id)

        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)
       
    elif trigger_id == 'gap-tree-link':
        #Extract values from the store_data dictionary
        #Set parameters and option for the table
        table_title = "GAP Report"
        table_id = "gap-table"

        if gap_store is None:
            return ("No data to display", table_id, table_title, dash.no_update, 
                    dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

        data = gap_store #gap_store["gap_data"]
        table_columns = gap_columns_config#gap_store["column_definitions"]
        #title = gap_store["title"]
        table_type = 'tree' #'tabular' #gap_store["table_type"]
        
        # print("\n\nGAP Table Columns: ", table_columns)
        # print("\n\nGAP Data: ", data)

        downloadButtonType = {"css": "btn btn-standard", "text": "Export", "type": "xlsx"}
        
        options={'rowFormatter': ns("rtRowFormatter"), 'dataTree': True, 'dataTreeStartExpanded': True, 
                 'selectable': True, 'layout': 'fitData'}
        
        #check type and build options
        if table_type == 'tabular':
            options={'layout': 'fitData', 
                     'selectable': True, 'height': '100%', "minHeight": 500}
        
        elif table_type == 'tree':
            options={'rowFormatter': ns("rtRowFormatter"), 'dataTree': True, 'dataTreeStartExpanded': True, 
                 'selectable': True, 'layout': 'fitData'}
            
        #return in no condition met
        else:
            return (dash.no_update, dash.no_update, dash.no_update, dash.no_update, 
                    dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)
        
        #Construct tabulator table
        table = construct_tabulator_table(data, table_columns, options, downloadButtonType, table_id)
        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)
    
    elif trigger_id == 'pt-tree-link':
        #Extract values from the store_data dictionary
        #Set parameters and option for the table
        table_title = "PT/MT Report"
        table_id = "pt-table"

        if pt_data is None:
            return ("No data to display", table_id, table_title, dash.no_update, 
                    dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

        data = pt_data #gap_store["gap_data"]
        table_columns = pt_columns_config #gap_store["column_definitions"]
        #title = gap_store["title"]
        table_type = 'tree' #'tabular' #gap_store["table_type"]
        
        # print("\n\nPT Table Columns: ", table_columns)
        # print("\n\nPT Data: ", data)

        downloadButtonType = {"css": "btn btn-standard", "text": "Export", "type": "xlsx"}
        
        options={'rowFormatter': ns("rtRowFormatter"), 'dataTree': True, 'dataTreeStartExpanded': True, 
                 'selectable': True, 'layout': 'fitData'}
        
        #check type and build options
        if table_type == 'tabular':
            options={'layout': 'fitData', 
                     'selectable': True, 'height': '100%', "minHeight": 500}
        
        elif table_type == 'tree':
            options={'rowFormatter': ns("rtRowFormatter"), 'dataTree': True, 'dataTreeStartExpanded': True, 
                 'selectable': True, 'layout': 'fitData'}
            
        #return in no condition met
        else:
            return (dash.no_update, dash.no_update, dash.no_update, dash.no_update, 
                    dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)
        
        #Construct tabulator table
        table = construct_tabulator_table(data, table_columns, options, downloadButtonType, table_id)
        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

    elif trigger_id == 'exception-report-link':
        table_title = "Test Package - Exception Report"
        table_id = 'exception-report-table'

        if exception_data is not None and len(exception_data) > 0:
            df_transformed, error_mapping = transform_exceptions(exception_data)
            
            #print(f"\n\nEXCEPTIONS_2: (size:{df_transformed.shape[0]}) \n", df_transformed)

            # Initial set of columns
            initial_columns = [
                {'title': i.replace('_', ' ').title(), 'field': i} for i in ['type', 'project', 'package', 'weld', 'drawing', 'stencil', 'lot', 'option']
            ]

            # Grouping columns based on error_mapping
            grouped_columns = {}
            if error_mapping is not None:
                for error, details in error_mapping.items():
                    col_name = details["column_name"]
                    group_name = details["group"]
            
                    # Adjust col_def here
                    col_def = {
                        'title': col_name,
                        'field': col_name,
                        'hozAlign': 'center',
                        'cellStyles': {
                            'color': '#FF0000',
                            'font-weight': 'bold'
                        }
                    }
            
                    if group_name not in grouped_columns:
                        grouped_columns[group_name] = []
                    grouped_columns[group_name].append(col_def)
        
                # Converting the grouped_columns dictionary to Tabulator's required format
                group_columns = [{'title': group, 'columns': cols} for group, cols in grouped_columns.items()]
                table_columns = initial_columns + group_columns # Combining initial and grouped columns
                df_dict = df_transformed.to_dict(orient='records')
            else:
                df_dict = None
                table_columns = None

            #Define table parameters
            downloadButtonType = {"css": "btn btn-standard", "text": "Export", "type": "xlsx"}
            options={'layout': 'fitDataFill', 
                        'selectable': True, 'height': '100%', "minHeight": 450, 'groupBy': 'type'}
            if df_dict:
                print(f"\n\nException Report Returned: 'len(df_dict)' rows.")
        else:
           df_dict, table_columns, options, downloadButtonType, table_id = None, None, None, None, None 

        #Construct tabulator table
        table = construct_tabulator_table(df_dict, table_columns, options, downloadButtonType, table_id)

        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

    elif trigger_id == "welder-continuity-link":
        table_title = "Welder Continuity"
        table_id = 'continuity-table'

        if cont_data is not None and len(cont_data) > 0:
            #Create the dataframe
            data = json.loads(cont_data)
            #print("CONT DATA CALLBACK: ", type(cont_data), "\nDATA:\n", cont_data)
            df = pd.DataFrame(data)

            #print("\n\nTriggered CONT:\n", df )

            options = {
                'groupBy': ['7', '8'],  # Group by these fields
                'selectable': True,
                'layout': 'fitData',
                'virtualDom': True,
                'pagination': 'local',
                'paginationSize': 75
            }

            # Extract column names from the first dictionary
            column_names = list(data[0].keys()) if data else []
            table_columns = field_id_to_field_name_for_tabulator(column_names, cont_field_mapping)

            table_columns[0]['frozen'] = True
            downloadButtonType = {"css": "btn btn-primary", "text": "Export", "type": "xlsx"}
        else:
           data, table_columns, options, downloadButtonType, table_id = None, None, None, None, None 
        
        #Construct tabulator table
        table = construct_tabulator_table(data, table_columns, options, downloadButtonType, table_id)

        return ([table], table_id, table_title, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

    else:
        "\n\nTriggered button not registered -- No Update"
        return (dash.no_update, dash.no_update, dash.no_update, dash.no_update, 
                dash.no_update, dash.no_update, dash.no_update, dash.no_update, False)

#Update RT BY Welder Icon and error_log
@app.callback(
    [Output('rt-icon', 'style'),
    Output('rt-icon', 'title'),
    Output('hidden-error-log-1', 'children'),
    Output('hidden-seen-errors-1', 'children')
    ],
    [Input('rt-data-store', 'data')],
    [State('hidden-error-log-1', 'children'),
     State('hidden-seen-errors-1', 'children'),
     State('rt-data-store-flat', 'data')
     ],   
)
def update_rt_by_welder_icon(rt_data, hidden_error_log, hidden_seen_errors, flat_data):
    if hidden_error_log:
        error_log = json.loads(hidden_error_log)
    else:
        error_log = []

    if hidden_seen_errors:
        seen_errors = {frozenset(item.items()) for item in json.loads(hidden_seen_errors)}
    else:
        seen_errors = set()

    if rt_data is not None and len(rt_data) > 0:
        # Convert the JSON in rt_data back to a dataframe
        #print("\n\nRT DATA:", type(rt_data), rt_data)

        #flattened_data = tree_to_dataframe(rt_data)
        #df = pd.DataFrame(flattened_data)
        #print("\n\nFLATTENED DATA: \n", df)

        by_welder_lot_df = pd.DataFrame(flat_data)
        by_welder_lot_df.columns = by_welder_lot_df.columns.astype(str)

        # Count how many "Needs RT" are in column '13'
        count_needs_rt = sum(by_welder_lot_df['13'].str.contains("Need", case=False))

        # If count is greater than 0, return display:inline-block, otherwise keep it as display:none
        if count_needs_rt > 0:
            style, tooltip, error_log_data = check_rt_status(by_welder_lot_df, "RT - By Welder", '7', '8', '13',"")

            #Append to main error log
            if error_log_data is not None:
                error_log, seen_errors = process_error_log(error_log, seen_errors, error_log_data)
                #print("\n\nRT ERROR LOG: ", error_log)

            return style, tooltip, json.dumps(error_log), json.dumps([dict(item) for item in seen_errors])
        else:
            return {"height": "15px", "width": "15px", "display": "none"}, "", dash.no_update, dash.no_update
    else:   
        print('rt_data is None')
        return {"height": "15px", "width": "15px", "display": "none"}, "", dash.no_update, dash.no_update

#Update RT BY Welder/Option Icon
@app.callback(
    [Output('rt-welder-option-filtered-icon', 'style'),
    Output('rt-welder-option-filtered-icon', 'title'),
    Output('hidden-error-log-2', 'children'),
    Output('hidden-seen-errors-2', 'children'),
    ],
    [Input('rt-welder-option-data-filtered-store', 'data')],
    [State('hidden-error-log-2', 'children'),
     State('hidden-seen-errors-2', 'children'),
     State('rt-welder-option-data-filtered-store-flat', 'data')
    ],
)
def update_welder_option_icon(rt_welder_option_data, hidden_error_log, hidden_seen_errors, flat_data):
    if hidden_error_log:
        error_log = json.loads(hidden_error_log)
    else:
        error_log = []

    if hidden_seen_errors:
        seen_errors = {frozenset(item.items()) for item in json.loads(hidden_seen_errors)}
    else:
        seen_errors = set()

    #Check if data has rows
    if len(flat_data) == 0:
        return {"height": "15px", "width": "15px", "display": "none"}, "", dash.no_update, dash.no_update

    by_welder_option_df = pd.DataFrame(flat_data)

    if rt_welder_option_data is not None:
        print("rt_data is not None")
        # Convert the JSON in rt_data back to a dataframe
        #by_welder_option_df = pd.read_json(rt_welder_option_data, orient='records')
        by_welder_option_df.columns = by_welder_option_df.columns.astype(str)

        # Count how many "Needs RT" are in column '14'
        count_needs_rt = sum(by_welder_option_df['14'].str.contains("Need", case=False))

        # If count is greater than 0, return display:inline-block, otherwise keep it as display:none
        if count_needs_rt > 0:
            style, tooltip, error_log_data = check_rt_status(by_welder_option_df, "RT - By Welder/Option", '8', '9', '14', '7','21')

        #Append to main error log
            if error_log_data is not None:
                error_log, seen_errors = process_error_log(error_log, seen_errors, error_log_data)
                #print("\n\nRT ERROR LOG: ", error_log)
            return style, tooltip, json.dumps(error_log), json.dumps([dict(item) for item in seen_errors])
        else:
            return {"height": "15px", "width": "15px", "display": "none"}, "", dash.no_update, dash.no_update
    else:    
        return {"height": "15px", "width": "15px", "display": "none"}, "", dash.no_update, dash.no_update

#update weld log log icon and error log
@app.callback(
    [
        Output('weld-error-icon', 'style'),
        Output('weld-error-icon', 'title'),
        Output('weld-error-icon-1', 'style'),
        Output('weld-error-icon-1', 'title'),
        Output('hidden-error-log-3', 'children'),
        Output('hidden-seen-errors-3', 'children')
    ],
    [
        Input('standard-link', 'n_clicks'),
        Input('tree-link', 'n_clicks')
    ],
    [
        State('hidden-error-log-3', 'children'),
        State('hidden-seen-errors-3', 'children'),
        State('weld-data-store', 'data'),
    ],
)
def update_weld_error_icon_and_tooltip(standard_clicks, tree_clicks, hidden_error_log, hidden_seen_errors, weld_data):
    if len(weld_data) == 0:
        return {"height": "15px", "width": "15px", "display": "none"}, "", dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    data = json.loads(weld_data)
    df = pd.DataFrame(data)
    if hidden_error_log:
        error_log = json.loads(hidden_error_log)
    else:
        error_log = []

    if hidden_seen_errors:
        seen_errors = {frozenset(item.items()) for item in json.loads(hidden_seen_errors)}
    else:
        seen_errors = set()

    # Using callback_context to figure out which input has fired
    ctx = dash.callback_context

    if not ctx.triggered:
        # Call the check_weld_corrections function
        style, tooltip, error_log_data, highlight_welds = check_weld_repairs(df)

        #Append to main error log
        if error_log_data is not None:
            error_log, seen_errors = process_error_log(error_log, seen_errors, error_log_data)

        # Append new column
        #trans_df['_error'] = trans_df['3'].isin(highlight_welds).map({True: 'Y', False: ''})
            return style, tooltip, style, tooltip, json.dumps(error_log), json.dumps([dict(item) for item in seen_errors])
        else:
            return style, tooltip, style, tooltip, dash.no_update, dash.no_update
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if button_id in ['standard-link', 'tree-link']:
            # Call the check_weld_corrections function
            style, tooltip, error_log_data, highlight_welds = check_weld_repairs(df)

            #Append to main error log
            if error_log_data is not None:
                error_log, seen_errors = process_error_log(error_log, seen_errors, error_log_data)

            #print("\n\nERROR LOG:\n", error_log)

            # Append new column
            #trans_df['_error'] = trans_df['3'].isin(highlight_welds).map({True: 'Y', False: ''})
            return style, tooltip, style, tooltip, json.dumps(error_log), json.dumps([dict(item) for item in seen_errors])

#Combines data from multiple divs and deduplicates to create the exception report
@app.callback(
    Output('exception-report', 'data'),
    [
        Input('hidden-error-log-1', 'children'),
        Input('hidden-error-log-2', 'children'),
        Input('hidden-error-log-3', 'children')
        # ... include other divs as needed ...
    ]
)
def combine_and_deduplicate_logs(log1, log2, log3):
    # Combine logs from multiple divs
    combined_data = []
    if log1:
        combined_data.extend(json.loads(log1))
    if log2:
        combined_data.extend(json.loads(log2))
    if log3:
        combined_data.extend(json.loads(log3))
    # ... repeat for other logs ...

    # Convert combined data to dataframe and deduplicate
    df = pd.DataFrame(combined_data)
    df.fillna('N/A', inplace=True)  # Replacing NaN values with blanks
    df_unique = df.drop_duplicates()
    # Convert dataframe to a dictionary format to store in dcc.Store
    data_dict = df_unique.to_dict(orient='records')

    return data_dict

if __name__ == '__main__': #Get the Correct Operating System to Run Gunicorn(Linux) or Waitress (Windows) #Waitress Local Server for testing #No need for this for the Azure
    # Detect the operating system
    os_name = platform.system()

    logging.info("CURRENT PLATFORM IS (PYTHON): %s", os_name)

    if os_name == 'Linux':
        # Run the app with Gunicorn or another Linux-compatible server
        # This part will not execute directly in the script; you typically run Gunicorn through command line
        # Example: `gunicorn -w 4 -b 0.0.0.0:8080 main:flask_server`
        pass

    elif os_name == 'Windows':
        # Run the app with Waitress on Windows
        serve(app.server, host='0.0.0.0', port=8080)
        #serve(app.server, host="0.0.0.0", port=8080)
        
        #http://localhost:8080/?parentRid=34