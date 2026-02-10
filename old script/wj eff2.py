import pandas as pd
import os
import sys
from datetime import time, timedelta

# --- 1. FILE PATH SETUP ---
base_path = os.path.dirname(os.path.abspath(__file__))
file_name = os.path.join(base_path, 'WjEff.xlsx')

if not os.path.exists(file_name):
    print(f"Error: File '{file_name}' not found.")
    sys.exit()

# --- 2. HELPER FUNCTIONS ---
def time_to_minutes(val):
    if pd.isna(val): return 0
    if isinstance(val, time): return (val.hour * 60) + val.minute
    if isinstance(val, timedelta): return int(val.total_seconds() / 60)
    if isinstance(val, str) and ':' in val:
        try:
            h, m = map(int, val.split(':'))
            return (h * 60) + m
        except: return 0
    if isinstance(val, str) and ';' in val:
        try:
            h, m = map(int, val.split(';'))
            return (h * 60) + m
        except: return 0    
    try: return float(val)
    except: return 0

def process_shift(data, shift_type, shift_limit, date_label):
    # Standardize column names immediately
    temp_df = data.copy()
    temp_df.columns = ['Machine Number', 'Quality', 'Power Time', 'Run Time', 'Stops', 'Efficiency Raw']
    
    # Calculations
    temp_df['Power Mins'] = temp_df['Power Time'].apply(time_to_minutes)
    temp_df['Run Mins'] = temp_df['Run Time'].apply(time_to_minutes)
    
    temp_df['Run_Ratio'] = temp_df['Run Mins'] / shift_limit
    
    # Efficiency calculations
    temp_df['Run Efficiency'] = (temp_df['Run Mins'] / temp_df['Power Mins']).fillna(0).replace([float('inf'), -float('inf')], 0)
    temp_df['Actual Efficiency'] = (temp_df['Run Mins'] / shift_limit).fillna(0)
    
    temp_df['Shift'] = shift_type
    temp_df['Date'] = date_label  # Track which sheet this came from
    return temp_df

# --- 3. MAIN PROCESSING (ALL SHEETS) ---
# Load all sheets into a dictionary of DataFrames
excel_file = pd.read_excel(file_name, sheet_name=None, skiprows=1)

all_data_list = []

for sheet_name, df in excel_file.items():
    # Only process sheets that look like they have data (check column count)
    if df.shape[1] >= 11:
        DAY_LIMIT = 11 * 60
        NIGHT_LIMIT = 13 * 60

        # Split Day (Cols 0-5) and Night (0 + 6-10)
        day_processed = process_shift(df.iloc[:, [0, 1, 2, 3, 4, 5]], 'Day', DAY_LIMIT, sheet_name)
        night_processed = process_shift(df.iloc[:, [0, 6, 7, 8, 9, 10]], 'Night', NIGHT_LIMIT, sheet_name)
        
        all_data_list.append(day_processed)
        all_data_list.append(night_processed)

# Combine everything from all dates into one massive table
full_report = pd.concat(all_data_list, ignore_index=True)

# --- 4. PREPARE THE 3 OUTPUT DATASETS ---

# 1. Active < 95% (Now includes Date and Shift)
low_power_final = full_report[full_report['Run_Ratio'] < 0.95][
    ['Date', 'Machine Number', 'Shift', 'Quality', 'Stops', 'Run Efficiency', 'Actual Efficiency']
].copy()

# 2. Run Efficiency Sheet
run_eff_final = full_report[['Date', 'Machine Number', 'Shift', 'Run Efficiency']].copy()

# 3. Actual Efficiency Sheet
actual_eff_final = full_report[['Date', 'Machine Number', 'Shift', 'Actual Efficiency']].copy()

# --- 5. EXPORT WITH FORMATTING ---
output_path = os.path.join(base_path, 'Production_Analysis.xlsx')

with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    workbook = writer.book
    percent_fmt = workbook.add_format({'num_format': '0%'})
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})

    # Function to write sheets and apply formatting
    def write_sheet(df, name, pct_cols):
        df.to_excel(writer, sheet_name=name, index=False)
        ws = writer.sheets[name]
        # Format columns as percentages (xlsxwriter uses 0-based indexing)
        for col_letter in pct_cols:
            ws.set_column(f'{col_letter}:{col_letter}', 15, percent_fmt)

    # Write the sheets
    # Column letters: F=Actual Eff, G=Run Eff for Low Power | D=Efficiency for others
    write_sheet(low_power_final, 'Active<95%', ['F', 'G'])
    write_sheet(run_eff_final, 'Run Efficiency', ['D'])
    write_sheet(actual_eff_final, 'Actual Efficiency', ['D'])

print(f"Analysis Complete. Processed {len(excel_file)} dates. Results saved to: {output_path}")