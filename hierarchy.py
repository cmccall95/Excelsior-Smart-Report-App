#hierarchy.py
import pandas as pd
import numpy as np
from dash_extensions.javascript import Namespace

# Register the custom namespace
ns = Namespace("myNamespace", "tabulator")

def select_df_columns(df,selected_columns):
    # Construct DataFrame with selected columns
    df = df[selected_columns]
    return df

# Determine columns to remove based on count of non-empty items (For table to PDF report)
def remove_selected_columns(df, col_list):
    print("LIST TO REMOVE: ", col_list)

    # Helper function
    def is_non_empty(x):
        if isinstance(x, float):
            return not np.isnan(x) and bool(str(x).strip())
        else:
            return bool(str(x).strip())

    # Ensure columns in col_list are in the dataframe
    col_list = [col for col in col_list if col in df.columns]

    # List of exceptions. These columns will be dropped regardless of whether they are empty or not
    exceptions = ['140', '141', '202', '203', '3', '_error'] #For weld log only #'77','78' Lot Options and OPtion Fields

    # Ensure columns in exceptions are in the dataframe
    exceptions = [col for col in exceptions if col in df.columns]

    columns_to_remove = []
    for col in col_list:
        #non_empty_count = df[col].apply(lambda x: not (isinstance(x, (type(None), float)) and np.isnan(x)) and bool(str(x).strip())).sum()
        non_empty_count = df[col].apply(is_non_empty).sum()
        if non_empty_count == 0:
            columns_to_remove.append(col)

    #Add columns in exceptions to columns_to_remove 
    for col in exceptions:
        if col in exceptions:
            columns_to_remove.append(col)
            
    # Remove columns with all empty items
    df = df.drop(columns=columns_to_remove)
    
    return df

def field_id_to_field_name(df, field_mapping):
    # Rename columns using field mapping
    column_mapping = {col: field_mapping.get(col, col) for col in df.columns}
    final_df = df.rename(columns=column_mapping)
    return final_df

def field_id_to_field_name_for_tabulator(df_columns, field_mapping):
    # Convert field ids to human-readable field names for tabulator
    return [{
        "title": field_mapping.get(col, col),
        "field": col,
        "headerWordWrap": True,
        "resizable": True,
        "visible": False if col in ['_error', '3'] else True, #'77', '78'
        "formatter": "textarea" if col == '219' else None
        } for col in df_columns]

def build_hierarchy_data(df, field_mapping):
    hierarchy_data = {}
    for _, row in df.iterrows():
        job = row['6']
        iso = row['8']
        test_pkg = row['11']
        weld = row['15']
        r1_status = row['39']
        r2_status = row['40']
        record_id = row['3']

        # Build hierarchy data
        job_data = hierarchy_data.setdefault(job, {})
        test_pkg_data = job_data.setdefault(test_pkg, {})
        weld_data = test_pkg_data.setdefault(weld, {})

        # Check R1 status
        if r1_status in ['ACC', 'REJ']:
            if "R1" not in weld_data:
                weld_data["R1"] = {}
            for col, field_name in field_mapping.items():
                if col in row:
                    weld_data["R1"][field_name] = row[col]

        # Check R2 status
        if r2_status in ['ACC', 'REJ']:
            if "R2" not in weld_data:
                weld_data["R2"] = {}
            for col, field_name in field_mapping.items():
                if col in row:
                    weld_data["R2"][field_name] = row[col]

    return hierarchy_data
    
    # Recursive case
    for item in tree:
        if item['name'] == repair_item[current_key]:
            if find_and_group_child(item.get('_children', []), repair_item, parent_keys[1:]):
                return True
    return False

def dataframe_to_tree(df, parent_keys, child_keys, repair_hierarchy=None):
    # Convert DataFrame to list of dictionaries
    data = df.to_dict('records')
    
    # Initialize root of tree
    tree = []
    
    print("\n\nInitial Repair Hierarchy\n: ", repair_hierarchy)
    # Define function to insert data into tree
    def insert(data, tree, parent_keys, child_keys):
        if not parent_keys:
            tree_data = {'name': data[child_keys[0]]}
            tree_data.update({key: data[key] for key in data.keys() if key not in parent_keys + child_keys})
            tree.append(tree_data)
        else:
            parent_key = parent_keys[0]
            for parent in tree:
                if parent['name'] == data[parent_key]:
                    if '_children' not in parent:
                        parent['_children'] = []
                    insert(data, parent['_children'], parent_keys[1:], child_keys)
                    return
            new_parent = {'name': data[parent_key], '_children': []}
            tree.append(new_parent)
            insert(data, new_parent['_children'], parent_keys[1:], child_keys)
    
    # Insert each row of data into tree
    for row in data:
        insert(row, tree, parent_keys, child_keys)

    # Process the repair_hierarchy if provided
    if repair_hierarchy:
        for repair_item in repair_hierarchy:
            # Find the parent based on job, test_pkg, and weld
            for parent in tree:
                if parent['name'] == repair_item['6']:
                    for pkg in parent.get('_children', []):
                        if pkg['name'] == repair_item['11']:
                            for iso in pkg.get('_children', []):
                                if iso['name'] == repair_item['8']:
                                    for weld in iso.get('_children', []):
                                        if weld['name'] == repair_item['15']:
                                            # If the weld is found, find the child_weld under iso
                                            for index, child in enumerate(iso.get('_children', [])):
                                                if child['name'] == repair_item['child_weld']:
                                                    # Move child_weld under the weld
                                                    if '_children' not in weld:
                                                        weld['_children'] = []
                                                    weld['_children'].append(child)
                                                    # Remove child_weld from the iso's _children list
                                                    del iso['_children'][index]
    return tree

def generate_columns(field_mapping, data_frame, tree_column, p_keys, c_keys, extra_formatters=None):

    #print("Formatter: ", extra_formatters)
    # Extract the column names from the DataFrame
    column_names = data_frame.columns.tolist()
    
    # Exclude parent and child keys from column names
    column_names = [col for col in column_names if col not in p_keys + c_keys]

    # Generate the columns configuration
    columns = [
        {
            "title": tree_column,
            "field": "name",
            "resizable": True,
            #"headerWordWrap": True,
            "cellClick": "function(e, cell){cell.getRow().toggleSelect();}"
        }
    ]
    
    # Standard columns
    for col in column_names:
        column_config ={"title": field_mapping.get(col, col), "field": col, "resizable": True,} #, "width": 150}#, "headerFilter": True} "htmlOutput":True ,
        # If extra formatters are passed in, check if there is a formatter for the current column
        if extra_formatters and col in extra_formatters:
            print("\n EXTRA FORMATTING APPLIED: ", col, "\n")
            column_config.update(extra_formatters[col])
        

        # Hide columns '_error', '3', '77', '78'
        if col in ['_error', '3']:#, '77', '78']:
            column_config.update({"visible": False})

        columns.append(column_config)

    #print("COLUM CONFIG:", columns)
    return columns


