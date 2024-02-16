from dash_extensions.javascript import Namespace
import pandas as pd
import pprint as pprint
from dotenv import load_dotenv
import os
from connect_qb import get_headers, get_rt_report
from hierarchy import generate_columns, dataframe_to_tree
from pprint import pprint
import numpy as np

load_dotenv()  # take environment variables from .env.

APP_ID = os.getenv('APP_ID')
WELD_LOG_ID = os.getenv('WELD_LOG_ID')
JOB_STENCIL_LOTS_ID = os.getenv('JOB_STENCIL_LOTS_ID')
JOB_STENCIL_OPTION_LOTS_ID = os.getenv('JOB_STENCIL_OPTION_LOTS_ID')
TPSL_ID = os.getenv('TPSL_ID')
JOB_SETUP_TABLE = os.getenv('JOB_SETUP_TABLE')

tree_column_name = ""

# Register the custom namespace
ns = Namespace("myNamespace", "tabulator")

def flatten_data(data):
    """Function to recursively flatten nested data and return a dataframe."""
    def recurse_items(node, path=[]):
        if '_children' in node:
            for child in node['_children']:
                yield from recurse_items(child, path + [node.get('name', '')])
        else:
            yield {**node, 'path': ' > '.join(path)}

    flattened_data = [item for entry in data for item in recurse_items(entry)]
    return pd.DataFrame(flattened_data)


def sort_rt_data(df, sort_list):
    df.sort_values(by=sort_list, inplace=True)

def rt_clicked(df, tpsl_data):
    #RT
    #PT/MT
    #Gap

    pd.reset_option('display.max_rows')
    pd.set_option('display.max_rows', 50)  # display 50 rows

    headers = get_headers()

    # Extra formatters
    extra_formatters_by_welder = {
        "13": {"formatter": ns("rtStatusFormatter")}
        }

    #Build column list for Quickbase query
    by_welder_column_query = ["6","7","8","11","9","10","12","13"]

    #Build where clause for Quickbase query
    if tpsl_data:
        job_field, test_pkg_field, tpsl_option, job_type, tpsl_full_df, tpsl_filtered_df, tpsl_field_mapping = tpsl_data

        #Filter for "BW" in 'Joint', id 20 - Unique List
        filtered_df = df[df['20'] == 'BW']

        #print("\n\nRT CLICKED - Filtered DF: \n ", filtered_df) #Verified is receiving dataframe

        # Unique list of Stencil 1
        if '28' in filtered_df:
            unique_values_s1 = filtered_df['28'].unique().tolist()
        else:
            "No Stencil 1[id 28] in dataframe"

        # Unique list of Stencil 1
        unique_values_s2 = filtered_df['30'].unique().tolist()
        # Combine the list and get a unique list
        combined_list = unique_values_s1 + unique_values_s2
        unique_values_combined = list(set(combined_list))

        #print("\n\nUnique Values Combined: ", unique_values_combined)

        by_welder_where_clause = ""

        for item in unique_values_combined:
            if item: 
                by_welder_where_clause += "{7.EX.'" + str(item) + "'}OR"
                
        by_welder_where_clause = "(" + by_welder_where_clause[:-2] + ")AND{6.EX.'" + job_field + "'}AND{14.EX.'BW'}"
    else:
        by_welder_where_clause = "{14.EX.'BW'}"

    #Construct Dataframes for RT Reports (By Welder, By Welder by Option)
    #"\n\nFrom RT Clicked"
    by_welder_lot_df, rt_welder_field_mapping = get_rt_report(df, headers, APP_ID, JOB_STENCIL_LOTS_ID, 
                                                              tpsl_data, by_welder_column_query, by_welder_where_clause, lot_column='8')

    #print( "\n\nRT Clicked - By Welder DF:\n", by_welder_lot_df)

    #Select columns - Reorders and selects only wanted columns.
    if by_welder_lot_df is not None:
        by_welder_lot_df = by_welder_lot_df[['6', '7', '8', '11','9', '10', '12','13']]

        #Create list to be sorted ascending (Columns will be sorted in the order supplied)
        sort_list = ['7','8']
        sort_rt_data(by_welder_lot_df, sort_list)

        #pprint(rt_welder_field_mapping)

        print("\n 9. Format and create RT trees \n")

        #Format %
        #By Welder data
        by_welder_lot_df['11'] = pd.to_numeric(by_welder_lot_df['11'], errors='coerce')
        by_welder_lot_df['11'] = by_welder_lot_df['11'].apply(lambda x: '{:.1f}%'.format(x) if pd.notna(x) else '')
        by_welder_lot_df['12'] = pd.to_numeric(by_welder_lot_df['12'], errors='coerce')
        by_welder_lot_df['12'] = by_welder_lot_df['12'].apply(lambda x: '{:.1f}%'.format(x) if pd.notna(x) else '')

        by_welder_lot_df['13'] = by_welder_lot_df['13'].replace(["Needs RT", "Need RT", "Needs RT/PT", "Need RT/PT"], "Need NDE")
        by_welder_lot_df['13'] = by_welder_lot_df['13'].replace("Lot Good", "OK")
        by_welder_lot_df['13'] = by_welder_lot_df['13'].replace("Current Lot", "Current")

        #Create tree from RT Data - by Welder
        rt_welder_parent_keys = ['6', '7']
        rt_welder_child_keys = ['8']
        rt_welder_columns_config = generate_columns(rt_welder_field_mapping, by_welder_lot_df, tree_column_name, rt_welder_parent_keys,
                                                   rt_welder_child_keys, extra_formatters=extra_formatters_by_welder)
        rt_welder_tree_data = dataframe_to_tree(by_welder_lot_df, rt_welder_parent_keys, rt_welder_child_keys)

        #pprint(rt_welder_option_tree_data)
        return by_welder_lot_df, rt_welder_tree_data, rt_welder_columns_config, rt_welder_field_mapping
    else:
        return None, None, None, None #df is none

def rt_option_clicked(df, tpsl_data, joint, nde_type, filtered):
    print(f"\n\nRT OPTION: \nJoint: {joint} \nType: {nde_type}")

    headers = get_headers()
    # Extra formatters
    extra_formatters_by_welder_option = {
        "14": {"formatter": ns("rtStatusFormatter")}
        }

    #Build column list for Quickbase query
    by_welder_option_column_query = ["6","7","8","9","10","11","31","37","12","13","14","15", "20","21", "36"]

    #Build where clause for Quickbase query
    if tpsl_data:
        
        job_field, test_pkg_field, tpsl_option, job_type, tpsl_full_df, tpsl_filtered_df, tpsl_field_mapping = tpsl_data

        #Filter for "BW" in 'Joint', id 20 - Unique List (No longer Necessary when pulling by NDE Type)
        #filtered_df = df[df['20'] == joint]

        #print("\n\nBreakpoint 1 \n", filtered_df)

        # Unique list of Stencil 1
        if '28' in df:
            unique_values_s1 = df['28'].unique().tolist() #filtered_df
        else:
           print("No '28' column in df")   

        # Unique list of Stencil 1
        unique_values_s2 = df['30'].unique().tolist() #filtered_df
        # Combine the list and get a unique list of that
        combined_list = unique_values_s1 + unique_values_s2
        unique_values_combined = list(set(combined_list))

        #print("\n\nBreakpoint 2 \n", unique_values_combined)
        print("\n\n", unique_values_combined)

        by_welder_option_where_clause = ""

        for item in unique_values_combined:
            if item: 
                by_welder_option_where_clause += "{8.EX.'" + str(item) + "'}OR"

        by_welder_option_where_clause = "(" + by_welder_option_where_clause[:-2] + ")AND{6.EX.'" + job_field + "'}AND{36.EX.'" + nde_type + "'}" #AND{15.EX.'" + joint + "'}
        #print(where_clause)
    else:
        #by_welder_option_where_clause = "{15.EX.'" + joint + "'}"
        by_welder_option_where_clause = "{36.EX.'" + nde_type + "'}"

    #Construct Dataframes for NDE Reports (By Welder, By Welder by Option)
    by_welder_option_lot_df, rt_option_field_mapping = get_rt_report(df, headers, APP_ID, JOB_STENCIL_OPTION_LOTS_ID, 
                                                                     tpsl_data, by_welder_option_column_query, by_welder_option_where_clause, lot_column='9')
    
    #Check if dataframe exists
    if by_welder_option_lot_df is not None:
        print("\n\nDF Size: ", by_welder_option_lot_df.shape[0], "\n, Columns: ", by_welder_option_lot_df.columns.tolist())
        columns_to_replace = ['11', '31', '37']
        by_welder_option_lot_df[columns_to_replace] = by_welder_option_lot_df[columns_to_replace].replace(['', 'nan', np.nan], 0)
        
        #By Welder by Option data:
        if '12' in by_welder_option_lot_df:
            by_welder_option_lot_df['12'] = pd.to_numeric(by_welder_option_lot_df['12'], errors='coerce')
            by_welder_option_lot_df['12'] = by_welder_option_lot_df['12'].apply(lambda x: '{:.1f}%'.format(x) if pd.notna(x) else '')
        if '13' in by_welder_option_lot_df:
            by_welder_option_lot_df['13'] = pd.to_numeric(by_welder_option_lot_df['13'], errors='coerce')
            by_welder_option_lot_df['13'] = by_welder_option_lot_df['13'].apply(lambda x: '{:.1f}%'.format(x) if pd.notna(x) else '0%')
        if '14' in by_welder_option_lot_df:
            by_welder_option_lot_df['14'] = by_welder_option_lot_df['14'].replace(["Needs RT", "Need RT", "Needs RT/PT", "Need RT/PT"], "Need NDE")
            by_welder_option_lot_df['14'] = by_welder_option_lot_df['14'].replace("Lot Good", "OK")
            by_welder_option_lot_df['14'] = by_welder_option_lot_df['14'].replace("Current Lot", "Current")

        #Select columns - Reorders and selects only wanted columns.
        if nde_type == "RT":
            by_welder_option_lot_df_full = by_welder_option_lot_df[['6', '7', '8', '15', '9', '12', '10', '11', '13', '14', '21']]
            by_welder_option_lot_df = by_welder_option_lot_df[['6', '7', '8', '15', '9','12', '10', '11', '13', '14', '21']]
        if nde_type == "Gap":    
            by_welder_option_lot_df_full = by_welder_option_lot_df[['6', '7', '8', '15', '9', '12', '10', '37', '13', '14', '21']]
            by_welder_option_lot_df = by_welder_option_lot_df[['6', '7', '8', '15','9','12', '10', '37', '13', '14', '21']]
            
        if nde_type == "PT/MT":   
            by_welder_option_lot_df_full = by_welder_option_lot_df[['6', '7', '8', '15', '9', '12', '10', '31', '13', '14', '21']]
            by_welder_option_lot_df = by_welder_option_lot_df[['6', '7', '8', '15', '9', '12', '10', '31', '13', '14', '21']]
        
        #Create list to be sorted ascending (Columns will be sorted in the order supplied)
        # 7:Option, 8:Stencil, 9:Lot, 15:Joint
        #sort_list = ['7','9','8']
        sort_list = ['8','9','7','15']
       
        # print(f"NDE TYPE == {nde_type}")
        # print(f"NDE TYPE == {tpsl_option}")
        sort_rt_data(by_welder_option_lot_df_full, sort_list)

        # Check if there's at least one 'Percent' in column '7'
        if (by_welder_option_lot_df['7'] == "Percent").any():
            # Function to format percent value
            def format_percent(row):
                # Convert percent_value to string if it's not already, and append '%'
                percent_value = str(row['12'])

                print("\n\nPercent Value: ", percent_value)
                formatted_percent = percent_value #+ "%"
                return f"{nde_type} {formatted_percent}"

            # Replace 'Percent' with 'RT - (formatted percent_value)'
            by_welder_option_lot_df.loc[by_welder_option_lot_df['7'] == "Percent", '7'] = by_welder_option_lot_df[by_welder_option_lot_df['7'] == "Percent"].apply(format_percent, axis=1)

        # Continue with your original filtering
        print(f"\n\n>>Before Filter {tpsl_option}: {by_welder_option_lot_df.shape[0]}")
        option_filtered_df = by_welder_option_lot_df[
        (by_welder_option_lot_df['7'] == tpsl_option) | (by_welder_option_lot_df['7'].str.startswith(nde_type))
         ]

        # >>> Temporary for unfiltered report. REMOVE
        option_filtered_df = by_welder_option_lot_df
        
        # print(f"\n\n>>After Filter: {option_filtered_df.shape[0]}")
        # print("\n\nBreakpoint 4 \n", option_filtered_df)
        # print("\n\nBreakpoint 4 \n", by_welder_option_lot_df)
        

        #Check which dataframe to return
        if filtered == False:
            option_df = by_welder_option_lot_df_full
        elif filtered == True:
            option_df = option_filtered_df
    
        #print("\n\nBreakpoint 5 \n", by_welder_option_lot_df_full)

        #Create tree from RT Data - by Welder Option (6: Job, 7: Option, 8: Stencil, 15: Joint, 9: Lot)
        #rt_welder_option_parent_keys = ['6', '8', '7']
        rt_welder_option_parent_keys = ['6', '8', '7', '15']
        rt_welder_option_child_keys = ['9']
        rt_welder_option_columns_config = generate_columns(rt_option_field_mapping, option_df, tree_column_name, rt_welder_option_parent_keys,
                                                          rt_welder_option_child_keys, extra_formatters=extra_formatters_by_welder_option)
        
        rt_welder_option_tree_data = dataframe_to_tree(option_df, rt_welder_option_parent_keys, rt_welder_option_child_keys)
        
        #pprint(rt_welder_option_tree_data)

        return option_df, rt_welder_option_tree_data, rt_welder_option_columns_config, rt_option_field_mapping
    else:
        return None, None, None, None
    
def create_gap_report(df, gap_percent, gap_option=None):
    #Options By Welder, By Option, By Welder/Option
    column_definitions = []
    
    #Filter for "SW" in 'Joint', id 20
    filtered_df = df[df['20'] == 'SW']
    #print("Filtered DF: ", filtered_df)

    #Determine gap_option and data type
    if not gap_option or gap_option == "By Welder": #Default to By Welder
        table_type = 'tabular'
        title = "GAP REPORT BY WELDER"
        
        # Unique list of Stencil 1, exclude blanks and None
        if '28' in df:
            unique_values_s1 = filtered_df[filtered_df['28'].str.strip() != '']['28'].dropna().unique().tolist()
        else:
           print("No '28' column in df")   

        # Unique list of Stencil 2, exclude blanks and None
        unique_values_s2 = filtered_df[filtered_df['30'].str.strip() != '']['30'].dropna().unique().tolist()

        # Combine the list and get a unique list of that
        combined_list = unique_values_s1 + unique_values_s2
        unique_values_combined = list(set(combined_list))

        # Build DataFrame
        gap_report_df = pd.DataFrame(columns=['Welder', 'Weld Count', 'GAP RT Count', 'NDE', '% Compliant', 'Status'])

        #print("Initial Gap Report DF: \n\n", gap_report_df)

        #for welder in unique_values_combined:
        #    weld_count = ((filtered_df['28'] == welder) | (filtered_df['30'] == welder)).sum()
        #    # Count instances where either '28' or '30' match the welder and '206' is not null or blank
        #    gap_rt_count = filtered_df[((filtered_df['28'] == welder) | (filtered_df['30'] == welder)) & filtered_df['206'].apply(lambda x: isinstance(x, str) and x.strip() != '')].shape[0]
        #    if gap_percent is None or gap_percent == '' or gap_percent == 0:
        #        nde = '0%'
        #    else:
        #        try:
        #            # Convert gap_percent to a float
        #            gap_percent_float = float(gap_percent)
        #            nde = "{:.0%}".format(gap_percent_float / 100)
        #        except ValueError:
        #            # Handle cases where gap_percent cannot be converted to a float
        #            print(f"Error: gap_percent '{gap_percent}' cannot be converted to float.")
        #            nde = 'N/A'

        #    percent_compliant = "{:.2%}".format(gap_rt_count / weld_count if weld_count else 0)
        #    if percent_compliant >= nde:
        #        gap_status = 'OK'
        #    else:
        #        gap_status = 'Need Gap'

        #    new_row = pd.DataFrame([{
        #        'Welder': welder,
        #        'Weld Count': weld_count,
        #        'GAP RT Count': gap_rt_count,
        #        'NDE': nde,
        #        '% Compliant': percent_compliant,
        #        'Status': gap_status 
        #    }])

        #    gap_report_df = pd.concat([gap_report_df, new_row], ignore_index=True)

        #column_definitions = create_column_definitions(gap_report_df.columns.tolist())

        #print("\n\nFINAL GAP REPORT\n", gap_report_df)

        return gap_report_df, column_definitions, title, table_type

    elif gap_option == "By Option":
        print("RETURNED: By Option. NOT CONFIGURED FOR THIS OPTION")
    elif gap_option == "By Welder/Option":
        print("RETURNED: By Welder/Option. NOT CONFIGURED FOR THIS OPTION")
    return None, None

def create_column_definitions(df_columns):
    # Create column definitions for tabulator
    column_definitions = []
    for col in df_columns:
        col_def = {
            "title": col,
            "field": col,
            #"headerWordWrap": True,
            "resizable": True
        }
        if col == 'Status':
            col_def['formatter'] = ns("rtStatusFormatter")
        column_definitions.append(col_def)
    return column_definitions
  
def get_gap_report_type(df):
    #Options By Welder, By Option, By Welder/Option
    
    #Filter for "SW" in 'Joint', id 20
    filtered_df = df[df['20'] == 'SW']

    # Check if count of SW, else 'None'
    if filtered_df.empty:
        return None

    # Get unique values of Gap Report Option (field '227') excluding blanks. 
    # Get unique values of Gap Shot % (field '228') excluding blanks.
    gap_report_options = filtered_df.loc[filtered_df['227'].astype(str) != '', '227'].unique()
    gap_shot_values = filtered_df.loc[filtered_df['228'].astype(str) != '', '228'].unique()

    print("\n\nGAP REPORT OPTIONS: ", gap_report_options, "\n\nCOUNT:", len(gap_report_options))
    print("\n\nGAP REPORT %: ", gap_shot_values, "\n\nCOUNT:", len(gap_shot_values))

    if len(gap_report_options) == 0:
        gap_option = "By Welder"
    elif len(gap_report_options) == 1:

        gap_option = gap_report_options[0]
    else:
        print("GAP EXIT ON 1")
        return {  #More than 1 Option Type Error
           "gap_data": None,
           "column_definitions": None,
           "title": None,
           "table_type":None
        }

    #Logic for type
    if len(gap_shot_values) == 0:
        ("GAP EXIT ON 2")
        return {  #No Gap Shot % Required
           "gap_data": None,
           "column_definitions": None,
           "title": None,
           "table_type":None
        }

   
    elif len(gap_shot_values) == 1:
        table_type = 'tabular'
        gap_percent = gap_shot_values[0]
    else:
        table_type = 'hierarchy'

    #Determine gap_option and data type
    if gap_option == "By Welder": #Default to By Welder
        title = "GAP REPORT BY WELDER"
        
        # Unique list of Stencil 1, exclude blanks and None
        if '28' in df:
            unique_values_s1 = filtered_df[filtered_df['28'].str.strip() != '']['28'].dropna().unique().tolist()
        else:
           print("No '28' column in df")   

        # Unique list of Stencil 2, exclude blanks and None
        unique_values_s2 = filtered_df[filtered_df['30'].str.strip() != '']['30'].dropna().unique().tolist()

        # Combine the list and get a unique list of that
        combined_list = unique_values_s1 + unique_values_s2
        unique_values_combined = list(set(combined_list))

        # Build DataFrame
        gap_report_df = pd.DataFrame(columns=['Welder', 'Weld Count', 'GAP RT Count', 'NDE', '% Compliant', 'Status'])

        print("\n\nInitial Gap Report DF: \n\n", gap_report_df)

        for welder in unique_values_combined:
            weld_count = ((filtered_df['28'] == welder) | (filtered_df['30'] == welder)).sum()
            # Count instances where either '28' or '30' match the welder and '206' is not null or blank
            gap_rt_count = filtered_df[((filtered_df['28'] == welder) | (filtered_df['30'] == welder)) & filtered_df['206'].apply(lambda x: isinstance(x, str) and x.strip() != '')].shape[0]
            if gap_percent is None or gap_percent == '' or gap_percent == 0:
                nde = '0%'
            else:
                try:
                    # Convert gap_percent to a float
                    gap_percent_float = float(gap_percent)
                    nde = "{:.0%}".format(gap_percent_float / 100)
                except ValueError:
                    # Handle cases where gap_percent cannot be converted to a float
                    print(f"Error: gap_percent '{gap_percent}' cannot be converted to float.")
                    nde = 'N/A'

            percent_compliant = "{:.2%}".format(gap_rt_count / weld_count if weld_count else 0)
            if percent_compliant >= nde:
                gap_status = 'OK'
            else:
                gap_status = 'Need Gap'

            new_row = pd.DataFrame([{
                'Welder': welder,
                'Weld Count': weld_count,
                'GAP RT Count': gap_rt_count,
                'NDE': nde,
                '% Compliant': percent_compliant,
                'Status': gap_status 
            }])

            gap_report_df = pd.concat([gap_report_df, new_row], ignore_index=True)

        column_definitions = create_column_definitions(gap_report_df.columns.tolist())

        print("\n\nCOL_DEF\n", type(column_definitions))

        print("\n\nFINAL GAP REPORT\n", gap_report_df)

        return {
           "gap_data": gap_report_df.to_dict('records') if gap_report_df is not None else None,
           "column_definitions": column_definitions,
           "title": title,
           "table_type": table_type
        }

    elif gap_option == "By Option":
        print("RETURNED: By Option. NOT CONFIGURED FOR THIS OPTION")
    elif gap_option == "By Welder/Option":
        print("RETURNED: By Welder/Option. NOT CONFIGURED FOR THIS OPTION")
    else:
       print("\n\nGAP EXIT ON 3. GAP OPTION = ", gap_option)
       
    return {
           "gap_data": None,
           "column_definitions": None,
           "title": None,
           "table_type":None
        }
