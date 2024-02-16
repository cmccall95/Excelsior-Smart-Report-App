import pandas as pd
import numpy as np
import re
from fpdf import FPDF#, TTFontFile
from io import BytesIO

#def create_groups(parent_columns, df):
#    grouped = df.groupby(parent_columns)

#    # Initialize an empty dataframe
#    new_df = pd.DataFrame()
#    last_parent = None

#    for name, group in grouped:
#        # Append rows for each level in the hierarchy
#        for i in range(len(parent_columns)):
#            # Skip the row if it's the top parent and it's the same as the last group
#            if i == 0 and name[i] == last_parent:
#                continue
#            row_data = [''] * i + [name[i]] + [''] * (len(df.columns) - i - 1)
#            row = pd.Series(row_data, index=df.columns)
#            new_df = pd.concat([new_df, row.to_frame().T], ignore_index=True)

#        # Append child rows
#        group = group.copy()
#        group[parent_columns] = ''  # remove parent and subparent values from child rows
#        new_df = pd.concat([new_df, group], ignore_index=True)

#        # Insert a blank row after each group for better readability
#        blank_row = pd.Series([''] * new_df.shape[1], index=new_df.columns)
#        new_df = pd.concat([new_df, blank_row.to_frame().T], ignore_index=True)
#        print(new_df)

#        # Remember the top parent of this group for the next iteration
#        last_parent = name[0]
    
#    # Reorder columns to place parent_columns at the beginning
#    new_df = new_df[[*parent_columns, *[col for col in new_df.columns if col not in parent_columns]]]
#    #print(new_df)

#    return new_df

def create_groups_columns(parent_columns, df):
    # Reorder columns to place parent_columns at the beginning
    df = df.reindex(columns=parent_columns + [c for c in df.columns if c not in parent_columns])

    grouped = df.groupby(parent_columns)

    # Initialize an empty dataframe
    new_df = pd.DataFrame()

    for name, group in grouped:
        # Append rows for each level in the hierarchy
        for i in range(len(parent_columns)):
            row_data = [''] * len(df.columns)
            row_data[i] = name[i]
            row = pd.Series(row_data, index=df.columns)
            new_df = pd.concat([new_df, row.to_frame().T], ignore_index=True)

        # Append child rows
        new_df = pd.concat([new_df, group], ignore_index=True)

    # Replace duplicates in parent_columns with ''
    for col in parent_columns:
        new_df[col] = new_df[col].mask(new_df[col].duplicated(), '')
    
    return new_df

def create_groups(parent_columns, df, indent_amount=6):
    # Reorder columns to place parent_columns at the beginning
    df = df.reindex(columns=parent_columns + [c for c in df.columns if c not in parent_columns])

    grouped = df.groupby(parent_columns)

    # Initialize an empty dataframe
    new_df = pd.DataFrame()

    for name, group in grouped:
        # Append rows for each level in the hierarchy
        for i in range(len(parent_columns)):
            row_data = [''] * len(df.columns)
            row_data[i] = name[i]
            row = pd.Series(row_data, index=df.columns)
            new_df = pd.concat([new_df, row.to_frame().T], ignore_index=True)

        # Append child rows
        new_df = pd.concat([new_df, group], ignore_index=True)

    # Replace duplicates in parent_columns with ''
    for col in parent_columns:
        new_df[col] = new_df[col].mask(new_df[col].duplicated(), '')

    # Create 'Items' column
    new_df['Items'] = ''

    for i, col in enumerate(parent_columns):
        indent = ' ' * (i * indent_amount)  # increase the indent based on the indent_amount for each subparent level
        bullet = '\u2022 ' if i == 0 else '\u25E6 '  # bullet point for parent, hollow bullet point for subparents
        new_df['Items'] += new_df[col].apply(lambda x: indent + bullet + x if x else '')

    # Remove parent and subparent columns
    new_df.drop(columns=parent_columns, inplace=True)
    #Move Items column to the beginning
    new_df = new_df.reindex(columns=['Items'] + [col for col in new_df.columns if col != 'Items'])
    return new_df

def create_groups_from_paths(df, indent_amount=6):
    if 'path' not in df.columns:
        raise ValueError("The 'path' column is not present in the dataframe.")

    # Split the 'path' column into separate levels
    hierarchy_levels = df['path'].str.split(' > ', expand=True)
    num_levels = hierarchy_levels.shape[1]

    # Add hierarchy levels to the DataFrame
    for i in range(num_levels):
        df[f'Level_{i+1}'] = hierarchy_levels[i]

    # Create a combined hierarchy column for indentation
    df['Hierarchy'] = df.apply(lambda row: ' ' * indent_amount * (row.notna() & row.str.startswith('Level')).sum(), axis=1)

    # Group by the hierarchical levels
    grouped = df.groupby([f'Level_{i+1}' for i in range(num_levels)])

    # Initialize an empty dataframe for the new grouped data
    new_df = pd.DataFrame()

    for _, group in grouped:
        # Append the group with hierarchy indentation
        group['Hierarchy'] = group['Hierarchy'] + group[f'Level_{num_levels}']
        new_df = pd.concat([new_df, group], ignore_index=True)

    # Drop the level columns and reorder columns
    level_columns = [f'Level_{i+1}' for i in range(num_levels)]
    new_df = new_df.drop(columns=level_columns)
    new_df = new_df.reindex(columns=['Hierarchy'] + [col for col in new_df.columns if col != 'Hierarchy'])

    return new_df


def create_print_dataframe(tabulator_data, column_properties):
    # Convert the tabulator data into a pandas DataFrame
    df = pd.DataFrame(tabulator_data)
    
    # Create a dictionary to store new column names mapping
    column_name_mapping = {}
    
    # Loop through each column property
    for column in column_properties:
        # If the column's 'visible' property is False, drop it from the DataFrame
        if not column['visible']:
            df = df.drop(column['field'], axis=1)
        else:
            # If the column's 'visible' property is True, update the column name mapping
            column_name_mapping[column['field']] = column['title']
    
    # Rename the DataFrame columns based on the column name mapping
    df.rename(columns=column_name_mapping, inplace=True)
    
    # Return the updated DataFrame
    return df

class PDF(FPDF):
    def __init__(self, title, df, size='tabloid', gridlines=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title_str = title
        self.title_font_size = 56
        self.table_top_margin = 50
        self.df = df
        self.df.replace([np.inf, -np.inf], "", inplace=True)
        self.df.fillna("", inplace=True)  

        # Replace newline characters with a placeholder (e.g., '|||')
        self.df.replace(to_replace='\n', value='|n|', regex=True, inplace=True)

        self.df = df.astype(str)

        self.max_font_size = 28
        self.header_font_size = 30
        self.min_font_size = 12
        self.min_col_width = 20
        self.l_margin = 40  # increase left margin
        self.r_margin = 5   # decrease right margin

        #Add custom font
        self.add_font("DejaVuSerifCondensed", style="", fname="assets/dejavu-fonts-ttf-2.37/ttf/DejaVuSerifCondensed.ttf", uni=True)  # add custom font
        # add bold version of the font
        self.add_font('DejaVuSerifCondensed', style='B', fname="assets/dejavu-fonts-ttf-2.37/ttf/DejaVuSerifCondensed-Bold.ttf", uni=True)
        self.add_font('DejaVuSerifCondensed', style='I', fname="assets/dejavu-fonts-ttf-2.37/ttf/DejaVuSerifCondensed-Italic.ttf", uni=True)
        self.set_font('DejaVuSerifCondensed', size = self.max_font_size)

        # Set the margins after FPDF's __init__
        self.set_margins(self.l_margin, self.t_margin, self.r_margin) 

        self.max_header_width = (self.w - self.l_margin - self.r_margin) / len(self.df.columns)

        #Dynamically adjust font size to fit the table on the page
        self.adjust_font_sizes()

        self.widths = self.calculate_column_widths()
        self.index = 0

        # Add new attributes for size and gridlines
        self.size = size
        self.gridlines = gridlines

    def adjust_font_sizes(self):
        self.widths = self.calculate_column_widths()
        while self.get_total_width() > (self.w - self.l_margin - self.r_margin):
            if self.max_font_size > self.min_font_size:
                self.max_font_size -= 1
                self.header_font_size -= 1
                self.set_font('DejaVuSerifCondensed', size = self.max_font_size)
                self.widths = self.calculate_column_widths()
            else:
                print("Even at the smallest font size, the table doesn't fit in the page.")
                #raise ValueError("Even at the smallest font size, the table doesn't fit in the page.")

    def get_total_width(self):
        return sum(self.calculate_column_widths())

    def calculate_column_widths(self):
        widths = []
        padding = 8  # adjust this value to your needs
        for col in self.df.columns:
            header_lines, _ = self.multi_cell(self.max_header_width, 0, txt=col.strip(), align='C')
            max_header_width = max(self.get_string_width(line.strip()) for line in header_lines) + padding
            max_data_width = max(max(self.get_string_width(line) for line in self.multi_cell_data(self.max_header_width, 10, txt=str(item))[0]) for item in self.df[col]) + 6
            column_width = max(max_data_width, max_header_width, self.min_col_width)
            widths.append(column_width)

            # after calculating column widths, let's check if total width is within the page width
        page_width = 1224 #1224  # in mm
        total_column_width = sum(widths)
        #print("Total Col Width:", total_column_width)

        #if total_column_width > (page_width - self.l_margin - self.r_margin):
        #    raise ValueError("Total column width exceeds the page width after considering margins.")

        return widths

    def header(self):
        self.image('assets/excel_logo.png', x = 40, y = 15, w = 85) 
        self.set_font('DejaVuSerifCondensed', 'B', size=self.title_font_size)
        # Shift the title down
        self.set_y(22)  # Adjust this value as per your requirements

        title_w = self.get_string_width(self.title_str) + 6
        doc_w = self.w - self.l_margin - self.r_margin
        self.set_x(self.l_margin + (doc_w - title_w) / 2)

        self.cell(title_w, 10, self.title_str, 0, 1, 'C')
        self.ln(10)

        self.set_y(self.table_top_margin)  # Set the y-coordinate to shift the start of the table down
        self.set_x(self.l_margin)  #

        self.set_font('Times', 'B', size=self.header_font_size)

        max_line_count = 0
        header_data = []
        for i, col in enumerate(self.df.columns):
            lines, line_count = self.multi_cell(self.widths[i], 10, txt=col)
            max_line_count = max(max_line_count, line_count)
            header_data.append((lines, self.widths[i]))

        padding = 2  # adjust this value to your needs
        for line_index in range(max_line_count):
            for lines, width in header_data:
                if line_index < len(lines):
                    self.cell(width, 10, lines[line_index], align='C')  # Add 'C' for center alignment
                else:
                    self.cell(width, 10, '')  # empty cell for no text
            self.ln(10)

        self.set_xy(self.l_margin, self.table_top_margin)  # reset the position to the shifted down location
        for lines, width in header_data:
            self.cell(width, max_line_count * 10, border=1)
        self.ln()

        self.set_font("DejaVuSerifCondensed", size=self.max_font_size)  # Reset font size for data

    def footer(self):
        self.set_y(-15)
        self.set_font('DejaVuSerifCondensed', 'I',22)
        self.cell(0, 10, 'Page %s' % self.page_no(), 0, 0, 'C')

    def table_body(self):
        for row in self.df.values:
            max_line_count = 0
            for item in row:
                _, line_count = self.multi_cell_data(self.widths[self.index], 10, txt=str(item))
                max_line_count = max(max_line_count, line_count)
                self.index += 1

            space_left = self.h - self.t_margin - self.b_margin - self.y - 20  
            if max_line_count * 10 > space_left:
                self.add_page()

            self.index = 0

            max_line_count = 0
            row_data = []
            for item in row:
                lines, line_count = self.multi_cell_data(self.widths[self.index], 10, txt=str(item))
                max_line_count = max(max_line_count, line_count)
                row_data.append((lines, self.widths[self.index]))
                self.index += 1

            max_lines_in_row = max_line_count

            for line_index in range(max_line_count):
                for lines, width in row_data:
                    if line_index < len(lines):
                        cell_text = lines[line_index]
                    else:
                        cell_text = ''

                    if self.gridlines:
                        if line_index == 0:
                            border = 'LTR'  # full border for the first line
                        elif line_index == max_lines_in_row - 1:  # use the stored max_lines_in_row
                            border = 'LRB'  # full border for the last line
                        else:
                            border = 'LR'  # only left and right border for middle lines
                    else:
                        border = 0  # No border

                    self.cell(width, 10, cell_text, border=border)
                self.ln()
            self.index = 0

    def set_data(self, df):
        self.df = df
        self.max_font_size = 28
        while max([self.get_string_width(str(max(df[col]))) + 6 for col in df.columns]) > (self.w - self.l_margin - self.r_margin) / len(df.columns):
            self.max_font_size -= 1
            self.set_font('DejaVuSerifCondensed', size = self.max_font_size)
        self.widths = [self.get_string_width(str(max(df[col]))) + 6 for col in df.columns]
        self.index = 0

    def set_title(self, title):
        self.title = title

    def multi_cell(self, w, h, txt, border=0, align='C', fill=False):
        # Check for bullet points in txt and skip operations if found
        if "\u2022" in txt or "\u25E6" in txt:
            return [txt], 1  # This return value might be adjusted based on your specific requirements

        words = re.split('(?<= )|(?<= - )|(?<=/)', txt.strip())
        lines = []
        line = ''
        for word in words:
            if self.get_string_width(line + word) <= w - 2:
                line += word + ' '
            else:
                if len(lines) >= 2:  
                    line += word + ' '
                else:
                    lines.append(line.strip())
                    line = word + ' '
        lines.append(line.strip()) 
        line_count = len(lines)
        return lines, line_count

    def multi_cell_data(self, w, h, txt, border=0, align='C', fill=False):
        if "\u2022" in txt or "\u25E6" in txt:
            return [txt], 1  # This return value might be adjusted based on your specific requirements

        if len(txt) < 11: 
            return [txt], 1
        lines = []
        line = ''

        # Split both by space and by our placeholder '|||'
        segments = txt.split('|n|')
        for segment in segments:
            if ' ' in segment:
                words = segment.split()
                for word in words:
                    if len(line) + len(word) <= 10:  
                        line += word + ' '
                    else:
                        lines.append(line.strip())
                        line = word + ' '
                lines.append(line.strip())
            else:
                lines.extend([segment[i:i+10] for i in range(0, len(segment), 10)])
            
        line_count = len(lines)
        return lines, line_count


def table_to_pdf(title, df, pdf_path, size='tabloid', gridlines=True):
    page_format = (792, 1224) if size == 'tabloid' else (612, 792)  # Letter size in portrait orientation is 612x792 points
    pdf_bytes = BytesIO()  # Use in-memory storage

    pdf = PDF(title, df, size=size, gridlines=gridlines, orientation='L' if size == 'tabloid' else 'P', format=page_format)
    pdf.set_auto_page_break(auto=True, margin=pdf.r_margin)  # considering the right margin
    pdf.add_page()

    # Add table data
    pdf.table_body()

    pdf.output(pdf_bytes)
    pdf_bytes.seek(0)  # Reset pointer
    #Download to repository
    pdf.output(pdf_path)

    return pdf_bytes

