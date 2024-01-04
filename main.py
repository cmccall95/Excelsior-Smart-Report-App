#main : Creates webapp
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
# from pprint import pprint
# from functools import partial
from waitress import serve
#from flask_cors import CORS #<< Simulate Cors

#http://localhost:8000/?parentRid=123

from dotenv import load_dotenv
import os
import base64

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

# Register the custom namespace
ns = Namespace("myNamespace", "tabulator")

#Arguments to be passed
record_id = None

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
    dynamic_layout = test_layout(record_id) #'34'
    
    return dynamic_layout

if __name__ == '__main__': #Waitress Local Server for testing 
    serve(app.server, host="0.0.0.0", port=8000)



