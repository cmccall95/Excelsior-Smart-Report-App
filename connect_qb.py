# module: connect_qb.py For processing and functions
from ast import Name
import requests
import json  
import pandas as pd
import numpy as np
import re
from datetime import datetime
from pprint import pprint
from dotenv import load_dotenv
import os, logging

from error_lib import check_weld_repairs

#Enable the current logger
logger = logging.getLogger(__name__)

load_dotenv()  # take environment variables from .env.
QB_REALM_HOSTNAME = os.getenv('QB_HOSTNAME')
QB_TOKEN = os.getenv('QB_TOKEN')
QB_USER_AGENT = os.getenv('QB_USER_AGENT')

QUICKBASE_API_URL = "https://api.quickbase.com/v1" 

def parse_number(entry):
    # Regular expression to match the leading number
    number_match = re.match(r'(\d+)', entry)
    if number_match:
        leading_number = int(number_match.group(1))
    else:
        leading_number = float('inf')
    
    # Regular expression to match the suffix (R1, R2, CO, CO1, CO2, CO3)
    suffix_match = re.search(r'(R1|R2|CO1|CO2|CO3|CO)', entry.replace(" ", ""))
    if suffix_match:
        suffix = suffix_match.group(1)
    else:
        suffix = ''
    
    # Custom ordering for suffixes
    suffix_ordering = {'R1': 1, 'R2': 2, 'CO': 3, 'CO1': 4, 'CO2': 5, 'CO3': 6}
    suffix_rank = suffix_ordering.get(suffix, 0)
    
    # Return a tuple (leading_number, suffix_rank) for sorting
    return (leading_number, suffix_rank)

# Custom date conversion with error handling
def convert_to_date(val):
    try:
        return pd.to_datetime(val).strftime("%m/%d/%Y")
    except:
        return val

# Get field properties (Gets infomration about Field Attributes and stores in a JSON.)
def get_field_properties(headers, app_id, table_id):
    url = f"https://api.quickbase.com/v1/fields?tableId={table_id}&includeFieldPerms=true"
    response = requests.get(url, headers=headers, verify=False) #STACKTRACE ERROR #Remove `verify=False` for production

    if response.status_code == 200:
        field_properties = response.json()
        #Local Debugging
        # logger.info(f"\nFailed to fetch field properties. Status Code: {response.status_code}")
        # logger.info(f"\nRequest Headers: {headers}")
        # logger.info(f"\nResponse Headers: {response.headers}")
        # logger.info(f"\nRequest URL: {response.url}")
        # logger.info(f"\nResponse Body: {response.text}")

        return field_properties
    else:
        # Log additional details when the response is not successful
        logger.error(f"Failed to fetch field properties. Status Code: {response.status_code}")
        #logger.error(f"Request Headers: {headers}")
        logger.error(f"Response Headers: {response.headers}")
        logger.error(f"Request URL: {response.url}")
        logger.error(f"Response Body: {response.text}")

        return {"error_get_field_properties": "Unable to fetch field properties"}

def get_headers():
    headers = {
        "QB-Realm-Hostname": QB_REALM_HOSTNAME,
        #"User-Agent": QB_USER_AGENT,
        "Authorization": QB_TOKEN
    }
    return headers

# get information about the table and store the Field ID/Field Name relationship
def get_table_info(headers, APP_ID, table_id):
    # Retrieve field properties from the API instead of JSON file
    try:
        field_properties = get_field_properties(headers, APP_ID, table_id)
        field_mapping = {str(field['id']): field['label'] for field in field_properties}
        return field_mapping
    except:
        #Additional logging handled in get_field_properties
        logger.error("Failed to fetch table info (get_table_info)")
        return None
    

def get_job_setup(table_id, job_id):
    headers = get_headers()

    query_body = {
        "from": table_id,
        "select": ["3","6","18","20","37","52", "70"],
        "where":  f"{{6.EX.{job_id}}}", #f"{{'{tpsl _id}'.3.CT.{record_id}}}", #'{record_id}'
        "orderBy": [],
        "skip": 0,
    }

    response = requests.post(
        f"{QUICKBASE_API_URL}/records/query",
        headers=headers,
        json=query_body
    )

    if response.status_code == 200:
        print("Job Setup Request Successful... " + str(response.status_code))
        data = response.json()
        records = data.get('data', [])
        setup_table = pd.DataFrame(records)

        if len(records) > 0:
            field_id_job = '6'  # Field ID for 'Job'
            field_id_qualifiers = '18'  # Field ID for 'Qualifiers Required'
            field_id_lot_option = '20'  # Field ID for 'Lot Option'
            field_id_gap_percent = '37'  # Field ID for 'Test PKG #'
            field_id_gap_option = '52'
            field_id_job_type = '70' # Field ID for 'Test PKG #' job_type

            if field_id_job:
                job_val = records[0].get(field_id_job, {}).get('value', None)

            #Check that jobs match
            if job_val != job_id:
                print(f"JOBS DO NOT MATCH. INITIAL JOB VALUE {job_id} DOES NOT MATCH {job_val}")
                return None, None, None

            if field_id_gap_percent: #if field_test_pkg:
                gap_percent = records[0].get(field_id_gap_percent, {}).get('value', None)
            if field_id_gap_option: #if field_test_pkg:
                gap_option = records[0].get(field_id_gap_option, {}).get('value', None)
        else:
            print("No records found for the specified row ID.")
        try:
            job_val
        except NameError:
            return None, None, None

        if job_val and gap_percent:
            return gap_percent, gap_option, setup_table
        else:
            return None, None
    else:
        logger.error(f"Failed to fetch Job Set up (get_job_setup). Status Code: {response.status_code}")
        #logger.error(f"Query Body: {query_body}")
        #logger.error(f"Request Headers: {headers}")
        logger.error(f"Response Headers: {response.headers}")
        logger.error(f"Request URL: {response.url}")
        logger.error(f"Response Body: {response.text}")
        return {"error_get_row_data": "Unable to fetch Job Setup data \n " }
    
def get_tpsl_data(headers, APP_ID, tpsl_id, job_val, pkg_val, job_type):
    logger.info(f"Preparing query based on job type: {job_type}")

    # Always query based on job_val
    if not job_val:
        logger.warning("Job value is missing.")
        return None, None

    query_where = f"{{6.EX.{job_val}}}"

    # Construct the query based on job_type
    if job_type == "Construction":
        if not job_val:
            logger.warning("Job or Package value is missing for Construction type.")
            pkg_val = None
        
        query_select = ["6", "10", "11", "41", "14", "36", "16", "13", "12", 
                        "27", "7", "38", "23", "30", "22", "26", "115", "24", "116", "25"]

    elif job_type == "Fab Shop":
        if not job_val:
            logger.warning("Job value is missing for Fab Shop type.")
            return []

        query_select = ["6", "108", "109", "10", "11", "41", "14", "36", "16", "13", "12", 
                        "27", "7", "38", "23", "30", "22", "26", "115", "24", "116", "25"]

    else:
        logger.error("Invalid or unknown job type.")
        return []

    query_body = {
        "from": tpsl_id,
        "select": query_select,
        "where": query_where,
        "orderBy": [],
        "skip": 0,
    }

    response = requests.post(
        f"{QUICKBASE_API_URL}/records/query",
        headers=headers,
        json=query_body
    )

    if response.status_code == 200:
        logger.info("Query successful... " + str(response.status_code))
        data = response.json()
        raw_records = data.get('data', [])

        # Process the records to extract actual values
        processed_records = []
        for record in raw_records:
            processed_record = {field: record[field]['value'] if record[field] else None for field in record}
            processed_records.append(processed_record)

        # Create full dataframe
        full_df = pd.DataFrame(processed_records)

        # Filter dataframe for Construction job type
        if job_type == "Construction" and pkg_val is not None:
            filtered_df = full_df[full_df['11'] == pkg_val]
        else:
            filtered_df = pd.DataFrame()
        
        return full_df, filtered_df

        #return processed_records
    
    else:
        print(f"Failed to query records. Status Code: {response.status_code}")
        return []

def get_tpsl_row_data(headers, APP_ID, tpsl_id, record_id): #Gets Package and Job
    print("Fetching data for row ID: ", record_id)
    # Get table info and update field mapping
    field_mapping = get_table_info(headers, APP_ID, tpsl_id)

    #print('\n FIELD MAPPING: \n ', field_mapping)

    query_body = {
        "from": tpsl_id,
        "select": ["3","6","11", "57", "113"],
        "where":  f"{{3.EX.{record_id}}}", #f"{{'{tpsl_id}'.3.CT.{record_id}}}", #'{record_id}'
        "orderBy": [],
        "skip": 0,
    }

    response = requests.post(
        f"{QUICKBASE_API_URL}/records/query",
        headers=headers,
        json=query_body
    )

    if response.status_code == 200:
        print("TPSL Request Successful... " + str(response.status_code))
        data = response.json()
        records = data.get('data', [])

        if len(records) > 0:
            field_id_job = '6'  # Field ID for 'Job'
            field_id_test_pkg = '11'  # Field ID for 'Test PKG #'
            field_id_lot_option = '57' #Field ID for 'Lot Option'
            field_id_job_type= '113' #Field ID for 'Lot Option'


            if field_id_job:
                job_val = records[0].get(field_id_job, {}).get('value', None)

            if field_id_test_pkg: 
                pkg_val = records[0].get(field_id_test_pkg, {}).get('value', None)

            if field_id_lot_option: 
                option_val = records[0].get(field_id_lot_option, {}).get('value', None)

            if field_id_job_type: 
                job_type = records[0].get(field_id_job_type, {}).get('value', None)

                tpsl_df, tpsl_filtered_df = get_tpsl_data(headers,APP_ID, tpsl_id, job_val, pkg_val, job_type)

                #test_pkg_df = pd.DataFrame(pkg_records)
                #print("\n\n>>>TPSL DF:\n", test_pkg_df)


            #print("Job:", job_val, "PKG:", pkg_val, "OPTION:", option_val, "JOB TYPE:", job_type)
        else:
            logger.warning("No records found for the specified row ID.")
        try:
            job_val
        except NameError:
            print("Error out on 1")
            return None, None, None, None, None, None, None
        #Check job type and handle the error logic
        if job_type == 'Construction':
            if job_val and pkg_val:
                return job_val, pkg_val, option_val, job_type, tpsl_df, tpsl_filtered_df, field_mapping
            else:
                print("Error out on 2")
                return None, None, None, None, None, None, None
        elif job_type == "Fab Shop":
            if job_val:
                return job_val, "", option_val, job_type, tpsl_df, tpsl_filtered_df, field_mapping
    else:
        logger.error(f"Failed to fetch TPSL Row info (get_tpsl_row_data). Status Code: {response.status_code}")
        logger.error(f"Query Body: {query_body}")
        # logger.error(f"Request Headers: {headers}")
        logger.error(f"Response Headers: {response.headers}")
        logger.error(f"Request URL: {response.url}")
        logger.error(f"Response Body: {response.text}")
        return {"error_get_row_data": "Unable to fetch data \n " }, None, None, None

# get data from a QuickBase table
def get_table_data(headers, APP_ID, table_id, tpsl_data, row_limit=None):
    print("Fetching data...")
    # Get table info and update field mapping
    qb_fields = ["3","6","14","8","9","10","11","76","15","16","17","18","19","20","21","49","52","22","23","24","28","30","43", 
                    "31","32","33","34","35","219","207","206","36","37","38","212","213","214","90","208","209","210","211","95", 
                        "97","140","141","71","39","45","47","81","83","55","56","91","40","46","48","85","87","62","63","92", "202",
                        "203", "77", "78", "225", "227", "228", "229", "230"] #"230: Original weld id for referencing cutouts" 

    field_mapping = get_table_info(headers, APP_ID, table_id)

    #print(field_mapping)

    #Get the job and test package
    if tpsl_data:
        job_field, test_pkg_field, tpsl_option, job_type, tpsl_df, filter_tpsl_df, tpsl_field_mapping = tpsl_data

        #print("TPSL DATA: ", tpsl_data)

        if job_type == 'Construction':
        # Construct where clause using tpsl_data
            where_clause = f"{{6.EX.{job_field}}} AND {{11.EX.{test_pkg_field}}}"

            #print("\n\nQB FIELDS: ", qb_fields)

        if job_type == 'Fab Shop':
            where_clause = f"{{6.EX.{job_field}}}"
    else:
        where_clause = ""

    query_body = {
        "from": table_id, 
        "select": qb_fields, #["3","6","14","8","9","10","11","76","15","16","17","18","19","20","21","49","52","22","23","24","28","30","43", 
        #            "31","32","33","34","35","219","207","206","36","37","38","212","213","214","90","208","209","210","211","95", 
        #                "97","140","141","71","39","45","47","81","83","55","56","91","40","46","48","85","87","62","63","92", "202",
        #                "203", "77", "78", "225", "227", "228", "229"], 
        "where": where_clause,
        "orderBy": [],
        "skip": 0,
    }

    if row_limit is not None:
        query_body["top"] = row_limit

    response = requests.post(
        f"{QUICKBASE_API_URL}/records/query",
        headers=headers,
        json=query_body
    )
    
    if response.status_code == 200:
        print("Request Successful... " + str(response.status_code))
        data = response.json()
        records = data.get('data', [])

        #Extract values from dictionaries
        for record in records:
            for field_id in record.keys():
                if isinstance(record[field_id], dict) and 'value' in record[field_id]:
                    record[field_id] = record[field_id]['value']

        # Get the number of records
        print("COUNT OF RECORDS: " + str(len(records)) + "\n")
        data['data'] = records

        df = pd.DataFrame(records)

        #Get table LOT options and column ID
    

        # Specify the filename and sheet name
        filename = 'Original Dataframe.xlsx'
        sheet_name = 'Original Dataframe'

        # Export the DataFrame to an Excel file
        #df.to_excel(filename, sheet_name=sheet_name, index=False)
        #print("Dataframe Exported....")

        #Lot option = id 77, Lot Builder selection = "78"

        return df, field_mapping
        
    else:
        logger.error(f"Failed to fetch Table Data (get_table_data). Status Code: {response.status_code}")
        # logger.error(f"Query Body: {query_body}")
        # logger.error(f"Request Headers: {headers}")
        logger.error(f"Response Headers: {response.headers}")
        logger.error(f"Request URL: {response.url}")
        logger.error(f"Response Body: {response.text}")
        return {"error_get_table_data": "Unable to fetch table data"}

def modify_original_data(df, index, row):
    # Modify the original data before processing (Handle stencil values)
    if row['90'] == "['Stencil 1']":
        df.at[index, '90'] = row['28']
    elif row['90'] == "['Stencil 2']":
        df.at[index, '90'] = row['30']
    else:
        df.at[index, '90'] = ""

    if row['91'] == "['Stencil 1']":
        df.at[index, '91'] = row['81']
    elif row['91'] == "['Stencil 2']":
        df.at[index, '91'] = row['83']
    else:
        df.at[index, '91'] = ""

    if row['92'] == "['Stencil 1']":
        df.at[index, '92'] = row['85']
    elif row['92'] == "['Stencil 2']":
        df.at[index, '92'] = row['87']
    else:
        df.at[index, '92'] = ""


def build_row(df, index, weld, repair_number):
    status_col, vt_date_col, rpt_num_col, rpt_date_col, qc_rep_col, stencil1_col, stencil2_col, reject_stencil_col = (
        ('39', '55', '47', '45', '56', '81', '83', '91') if repair_number == 1 else
        ('40', '62', '48', '46', '63', '85', '87', '92')
    )

    new_row = {
        '15': f"{str(weld)} R{repair_number}",
        '38': df.at[index, status_col],
        '31': df.at[index, vt_date_col],
        '36': df.at[index, rpt_num_col],
        '37': df.at[index, rpt_date_col],
        '32': df.at[index, qc_rep_col],
        '28': df.at[index, stencil1_col],
        '30': df.at[index, stencil2_col],
        '90': df.at[index, reject_stencil_col]
    }

    columns_to_copy = ["6", "11", "10", "8", "9", "14", "16", "17", "20", "21", "49", "22", "23", "24", "52", "76","77","78", "3"]
    for col in columns_to_copy:
        new_row[col] = df.at[index, col]

    return new_row

def transform_data(df, field_mapping):
    #Check if dataframe has any rows.
    if len(df)==0:
        return df, None
    
    new_rows = []
    repair_hierarchy = []

    logger.info(f"Transforming Dataframe with {len(df)} rows.")

    #print("\n\nTRANSFORM DF:\n",df)
    # Check and prepend 'LOT ' if columns '202' and '203' exist
    for col in ['202', '203']:
        if col in df.columns:
            df = prepend_lot_to_integers(df, col)

    for index, row in df.iterrows():
        job = row['6']
        iso = row['8']
        test_pkg = row['11']
        weld = row['15']
        r1_status = row['39']
        r2_status = row['40']
        record_id = row['3']

        # Modify the original data
        modify_original_data(df, index, row)

        # Check R1 status
        if r1_status in ['ACC', 'REJ']:
            new_row_r1 = build_row(df, index, weld, repair_number=1)
            new_rows.append(new_row_r1)
            # Update repair hierarchy
            repair_hierarchy.append({"6": job, "11": test_pkg, "8": iso, "15": weld, "child_weld": new_row_r1['15'], "type": "R1"})

        # Check R2 status
        if r2_status in ['ACC', 'REJ']:
            new_row_r2 = build_row(df, index, weld, repair_number=2)
            new_rows.append(new_row_r2)
            # Update repair hierarchy
            repair_hierarchy.append({"6": job, "11": test_pkg, "8": iso, "15": weld, "child_weld": new_row_r2['15'], "type": "R2"})

        ## Check CO1 status
        #if r2_status in ['ACC', 'REJ']:
        #    new_row_r2 = build_row(df, index, weld, repair_number=2)
        #    new_rows.append(new_row_r2)
        #    # Update repair hierarchy
        #    repair_hierarchy.append({"6": job, "11": test_pkg, "8": iso, "15": weld, "child_weld": new_row_r2['15'], "type": "R2"})

    new_data = pd.DataFrame(new_rows)
    df = pd.concat([df, new_data], ignore_index=True)

    # Sort by the '15' column using a custom key function
    df = df.sort_values(by='15', key=lambda x: x.apply(parse_number))

    # Format specific columns as "m/d/yyyy"
    for col in ["31", "37", "45", "46", "62", "207", "208"]:
        df[col] = df[col].apply(convert_to_date)

    # Add new column "hl format" to the DataFrame for formatting
    df = df.assign(**{'_error': ''})

    #Get welds not repaired and log errors.
    style, tooltip, error_log_data, highlight_welds = check_weld_repairs(df)
    # Append new column
    df['_error'] = df['3'].isin(highlight_welds).map({True: 'Y', False: ''})

    return df, repair_hierarchy

def get_rt_report(df, headers, APP_ID, table_id, tpsl_data, select_query, where_clause, lot_column, row_limit=None):
    print("Fetching RT data...")
    # Get table info and update field mapping

    #print("\n\nRT DF: \n", df) # Verified is receiving

    
    #print("\n\nWHERE_CLAUSE: \n", where_clause)

    field_mapping = get_table_info(headers, APP_ID, table_id)
   
    query_body = {
    "from": table_id, 
    "select": select_query, 
    "where": where_clause,
    "orderBy": [],
    "skip": 0,
    }

    #print("\n\nRT REPORT BY WELDER QUERY:\n", query_body)

    if row_limit is not None:
        query_body["top"] = row_limit

    response = requests.post(
        f"{QUICKBASE_API_URL}/records/query",
        headers=headers,
        json=query_body
    )

    if response.status_code == 200:
        print("Request Successful (RT Report)... " + str(response.status_code))
        data = response.json()
        records = data.get('data', [])

        #print("\n\nRECORDS: \n", records)

        #Extract values from dictionaries
        for record in records:
            for field_id in record.keys():
                if isinstance(record[field_id], dict) and 'value' in record[field_id]:
                    record[field_id] = record[field_id]['value']

        # Get the number of records
        record_count = len(records)
        print("COUNT OF RT RECORDS: " + str(len(records)) + "\n")
        data['data'] = records

        if record_count > 0:
            df_lots = pd.DataFrame(records)
            df_lots = prepend_lot_to_integers(df_lots, lot_column) 
        else:
            print("\n\nget_rt_report = None")
            return None, None

            # Specify the filename and sheet name
            #filename = 'RT Dataframe.xlsx'
            #sheet_name = 'RT Dataframe'

            # Export the DataFrame to an Excel file
            #df_lots.to_excel(filename, sheet_name=sheet_name, index=False)
            #print("Dataframe Exported....")

        return df_lots, field_mapping

    else:
        return {"error_get_table_data": "Unable to fetch data"}, ""

def prepend_lot_to_integers(df, column):
    # Convert column to string
    column = str(column)

    # Store original values
    #print(df.columns)
    #print("Prepend Column: ", column, ":", type(column))

    if column not in df.columns:
        #print("Error columns: ", df.columns)
        #raise ValueError(f"Column {column} not present in the DataFrame!")
        logger.warning(f"Column {column} not present in the DataFrame!")

    else:

        original_values = df[column].copy()

        # Convert to numeric
        df[column] = pd.to_numeric(df[column], errors='coerce')

        # For non-NaN values that are greater than 0, add 'LOT ' prefix and convert to integer string
        df[column] = df[column].apply(lambda x: 'LOT ' + str(int(x)) if pd.notnull(x) and x > 0 else x)

        # Revert to original where conversion failed
        df[column].fillna(original_values, inplace=True)

    return df

def get_continuity(headers, APP_ID, table_id, tpsl_data, row_limit=None):
    print("Fetching Continuity data...")

    #Get Job No.

    field_mapping = get_table_info(headers, APP_ID, table_id)
   
    query_body = {
    "from": table_id, 
    "select": ['3','7','8','17','18','19','20','21','22'], #[int(k) for k in field_mapping.keys()], #All columns
    "where": "", 
    "orderBy": [],
    "skip": 0,
    }

    if row_limit is not None:
        query_body["top"] = row_limit

    response = requests.post(
        f"{QUICKBASE_API_URL}/records/query",
        headers=headers,
        json=query_body
    )

    #print("\n\nCONT RESPONDE:\n", response, "TABLE ID:\n", table_id, "query_body:\n", query_body)
    if response.status_code == 200:
        print("\n\nRequest Successful (Cont. Report)... " + str(response.status_code))
        data = response.json()
        records = data.get('data', [])

        #Extract values from dictionaries
        for record in records:
            for field_id in record.keys():
                if isinstance(record[field_id], dict) and 'value' in record[field_id]:
                    record[field_id] = record[field_id]['value']

        # Get the number of records
        record_count = len(records)
        print("COUNT OF Cont RECORDS: " + str(len(records)) + "\n")
        data['data'] = records

        if record_count > 0:
            df = pd.DataFrame(records)
            #print("\n\n", df)
        else:
            return None, None

            # Specify the filename and sheet name
            #filename = 'RT Dataframe.xlsx'
            #sheet_name = 'RT Dataframe'

            # Export the DataFrame to an Excel file
            #df_lots.to_excel(filename, sheet_name=sheet_name, index=False)
            #print("Dataframe Exported....")

        return df, field_mapping

    else:
        return {"error_get_table_data": "Unable to fetch data"}, ""



