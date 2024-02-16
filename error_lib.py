import pandas as pd

def check_weld_repairs(df):
    # Keep track of error welds
    error_log_data = []
    error_welds = []
    tracer_not_assigned = []
    tracers_not_repaired = []
    highlight_list = []
    
    # Loop through each row in the DataFrame
    for index, row in df.iterrows():
        # Check if the weld status is "REJ"
        if row['38'] == "REJ":
            # Extract weld number, project, package, and drawing
            weld_number = row['15']
            project = row['6']
            package = row['11']
            drawing = row['8']
            record_id = row['3']
            
            # Check if there is a corresponding repair or cutout with accepted status
            correction_found = False
            for suffix in ["R1", "R2", "R3", "CO", "CO1", "CO2", "CO3"]:
                for spacing in [" ", ""]:
                    corrected_weld_number = weld_number + spacing + suffix
                    corrected_rows = df[
                        (df['15'] == corrected_weld_number) &
                        (df['38'] == "ACC") &  
                        (df['6'] == project) &
                        (df['8'] == package) &
                        (df['11'] == drawing)
                    ]
                    if not corrected_rows.empty:
                        correction_found = True
                        break
                if correction_found:
                    break

            #Check Tracers were assigned and build list (S1T1(95), S1T2(97), S2T1(140), S2T2(141))
            non_blank_count = sum(row[col] != '' for col in ['95', '97', '140', '141'])
            #tracer_list = [weld_number, row['95'], row['97'], row['140'], row['141'],tracer_repair_status]

            if non_blank_count < 2:
                tracer_not_assigned.append((project, package, drawing, record_id, weld_number))
                highlight_list.append(record_id)
            else:
                tracers_not_repaired = []
                for tracer_id in row[['95', '97', '140', '141']].values:
                    tracer_rows = df[(df['15'] == tracer_id) & (df['38'] != 'ACC')]
                    if not tracer_rows.empty:
                        tracers_not_repaired.append((project, package, drawing, tracer_id, record_id, weld_number))
                        highlight_list.append(record_id)
            
            # If no correction is found, add the weld to the error_welds list
            if not correction_found:
                error_welds.append((project, package, drawing, record_id, weld_number))
                highlight_list.append(record_id)

    highlight_list = list(set(highlight_list))
    # Create the error message
    error_message = ''

    # Error welds
    if error_welds:
        error_message += 'NOT IN COMPLIANCE:\n\nThe following welds have not been repaired:\n\nJob | Test PKG | Drawing | Record ID | Weld\n'
        for project, package, drawing, record_id, weld_number in error_welds:
            error_message += f"{project} | {package} | {drawing} | {record_id} | {weld_number}\n"
            error_log_data.append({
                "type": "Weld Log",
                "error": "Weld Not Repaired",
                "project": project,
                "package": package,
                "drawing": drawing,
                "record_id": record_id,
                "weld": weld_number
            })

    # Tracer not assigned
    if tracer_not_assigned:
        error_message += '\n\nThe following welds do not have tracers assigned:\n\nJob | Test PKG | Drawing |  Record ID |Weld\n'
        for project, package, drawing, record_id, weld_number in tracer_not_assigned:
            error_message += f"{project} | {package} | {drawing} | {record_id} | {weld_number}\n"
            # Append a dictionary to the error_log
            error_log_data.append({
                "type": "Weld Log",
                "error": "Tracer Not Assigned",
                "project": project,
                "package": package,
                "drawing": drawing,
                "record_id": record_id,
                "weld": weld_number
            })

    # Tracers not repaired
    if tracers_not_repaired:
        error_message += '\n\nThe following tracers have not been repaired:\n\nProject | Test PKG | Drawing | Tracer ID  |  Record ID | Tracer Origin Weld\n'
        for project, package, drawing, tracer_id, record_id, weld_number in tracers_not_repaired:
            error_message += f"{project} | {package} | {drawing} | {tracer_id} | {record_id} | {weld_number}\n"
            # Append a dictionary to the error_log
            error_log_data.append({
                "type": "Weld Log",
                "error": "Tracer Not Repaired",
                "project": project,
                "package": package,
                "drawing": drawing,
                "record_id": record_id,
                "weld": weld_number
            })

    if error_message == '':
        return {"height": "15px", "width": "15px", "display": "none"}, '', None, highlight_list
    else:
        return {"height": "15px", "width": "15px", "display": "inline-block"}, error_message, error_log_data, highlight_list

def check_rt_status(df, rt_type, stencil_col, lot_col, status_col, option_col, entire_lot_col = None):
    error_log_data = []
    needs_rt = []
    rt_lot = []

    # Loop through each row in the DataFrame
    for index, row in df.iterrows():
        # Extract stencil and lot
        stencil = row[stencil_col]
        lot = row[lot_col]
        # Check if the weld status is "REJ"
        if "need" in row[status_col].lower():
            needs_rt.append((stencil, lot))

         # If entire_lot_col is given and its value is 'yes', add it to rt_lot
        if entire_lot_col is not None and row[entire_lot_col] == 'yes':
            option_val = row[option_col]
            rt_lot.append((stencil, option_val, lot))
        else:
            option_val = ""

    # Create the error message
    error_message = ''
    if needs_rt or rt_lot:
        if needs_rt:
            error_message = 'NOT IN COMPLIANCE:\n\nThe following needs RT:\n\n Stencil | LOT |\n'
            for stencil, lot in needs_rt:
                error_message += f" -- {stencil} | {lot} | \n"

                # Append a dictionary to the error_log
                error_log_data.append({
                    "type": rt_type,
                    "error": "Needs RT",
                    "stencil": stencil,
                    "lot": lot,
                    "option": option_val,
                    "entire_lot": "N"
                })

        if rt_lot:
            error_message += '\nThe following lots need RT:\n\n Stencil |  Option  | LOT  |\n'
            for stencil, option_val, lot in rt_lot:
                error_message += f" -- {stencil} | {option_val} | {lot} | \n"

                # Append a dictionary to the error_log
                error_log_data.append({
                    "type": rt_type,
                    "error": "RT Entire LOT",
                    "stencil": stencil,
                    "lot": lot,
                    "option": option_val,
                    "entire_lot": "Y"
                })

        if error_message:
            return {"height": "15px", "width": "15px", "display": "inline-block"}, error_message, error_log_data
        else:
            return {"height": "15px", "width": "15px", "display": "inline-block"}, '', None
    else:
        return {"height": "15px", "width": "15px", "display": "none"}, '', None
            
def process_error_log(error_log, seen_errors, error_log_data):
    # Append unique errors to main error log
    for error in error_log_data:
        error_as_frozenset = frozenset(error.items())
        if error_as_frozenset not in seen_errors:
            error_log.append(error)
            seen_errors.add(error_as_frozenset)
    return error_log, seen_errors

def transform_exceptions(data):
    df = pd.DataFrame(data)
    
    print(f"\n\nEXCEPTIONS_1: (size:{df.shape[0]}) \n", df)
    
    # df.to_excel('Exception Report.xlsx', sheet_name="Exceptions", index=False)

    #Check if None type or empty
    if df is None or df.empty:
        return None, None

    # Create a mapping for error codes to their new column names with groupings
    error_mapping = {
        "Weld Not Repaired": {"column_name": "Need Repair", "group": "Weld Log Errors"},
        "Tracer Not Assigned": {"column_name": "Tracer(s) Unassigned", "group": "Weld Log Errors"},
        "Tracer Not Repaired": {"column_name": "Need Tracer(s) Repair", "group": "Weld Log Errors"},
        "Needs PT/MT": {"column_name": "Needs PT/MT", "group": "Weld Log Errors"},
        "Needs GAP Shot": {"column_name": "Needs GAP", "group": "RT Errors"},
        "RT Entire LOT": {"column_name": "RT Entire LOT", "group": "RT Errors"},
        "Needs RT": {"column_name": "Needs RT", "group": "RT Errors"}
    }

    # Function to aggregate errors for a group
    def aggregate_errors(group):
        aggregated_errors = set(','.join(group['error']).split(','))
        for error, details in error_mapping.items():
            group[details["column_name"]] = "X" if error in aggregated_errors else ""
        return group.iloc[0]
    
    # # Check if 'weld' column exists, and group accordingly
    # group_by_cols = ['type', 'weld'] if 'weld' in df.columns else ['type']
    # final_df = df.groupby(group_by_cols).apply(aggregate_errors)

    # Determine grouping columns based on 'type' values
    if 'type' in df.columns:
        # Store the group as a string instead of a list
        df['grouping_columns'] = df['type'].apply(
            lambda x: 'type,stencil,lot,option' if x != 'Weld Log' else 'type,weld'
        )

        grouped_dfs = []
        for group in df['grouping_columns'].unique():
            group_cols = group.split(',')
            group_df = df[df['grouping_columns'] == group].groupby(group_cols).apply(aggregate_errors)
            
            # Reset index to turn all index levels back into columns
            group_df.reset_index(drop=True, inplace=True)
        
            # Sorting logic based on 'type'
            if 'type,weld' in group:
                # Sort for 'Weld Log'
                group_df.sort_values(by=['drawing', 'weld'], inplace=True)
            else:
                # Sort for 'RT Types'
                group_df.sort_values(by=['stencil', 'lot'], inplace=True)

            grouped_dfs.append(group_df)

        # Combine the grouped DataFrames
        final_df = pd.concat(grouped_dfs)
        
    else:
        # Default grouping if 'type' column is not present
        #final_df = df.groupby(['type']).apply(aggregate_errors)
        print("\n\n>>> Column 'type' does not exist in Exception Data. Check your data.\n", df)
        return None, None

    # Drop the original "error" column
    final_df.drop(columns=['error'], inplace=True)

    # Organize columns in the desired order
    initial_order = ['type', 'project', 'package', 'weld', 'drawing', 'stencil', 'lot', 'option', 'entire_lot']
    weld_log_error_cols = [details["column_name"] for error, details in error_mapping.items() if details["group"] == "Weld Log Errors"]
    rt_error_cols = [details["column_name"] for error, details in error_mapping.items() if details["group"] == "RT Errors"]

    # Concatenate all the column orders together
    final_column_order = initial_order + weld_log_error_cols + rt_error_cols

    existing_columns = [col for col in final_column_order if col in final_df.columns]
    final_df = final_df[existing_columns]

    return final_df.reset_index(drop=True), error_mapping

