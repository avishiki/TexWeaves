import pandas as pd
import os
import sys
from datetime import time, timedelta

# --- 1. FILE PATH SETUP ---
base_path = os.path.dirname(os.path.abspath(__file__))
file_name = os.path.join(base_path, 'WjEff.xlsx')

if not os.path.exists(file_name):
    print(f"Error: The file '{file_name}' was not found.")
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
    if isinstance(val, str) and ':' in val:
        try:
            h, m = map(int, val.split(';'))
            return (h * 60) + m
        except: return 0
    try: return float(val)
    except: return 0

def process_shift(data, shift_type, shift_limit):
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
    return temp_df

# --- 3. MAIN PROCESSING ---
df = pd.read_excel(file_name, skiprows=1)

DAY_SHIFT_MINS = 11 * 60
NIGHT_SHIFT_MINS = 13 * 60

# Split columns
day_data = df.iloc[:, [0, 1, 2, 3, 4, 5]]
night_data = df.iloc[:, [0, 6, 7, 8, 9, 10]]

# Process
day_processed = process_shift(day_data, 'Day', DAY_SHIFT_MINS)
night_processed = process_shift(night_data, 'Night', NIGHT_SHIFT_MINS)

# Combine
full_report = pd.concat([day_processed, night_processed], ignore_index=True)

# --- 4. CREATE THE FILTERED SHEET ---
# 1. Filter rows where Run_Ratio is < 95%
low_power = full_report[full_report['Run_Ratio'] < 0.95].copy()

# 2. SELECT SPECIFIC COLUMNS 
# Note: These names must match the ones inside process_shift EXACTLY
columns_to_keep = ['Machine Number', 'Quality', 'Stops', 'Run Efficiency', 'Actual Efficiency']
low_power_final = low_power[columns_to_keep]

# 4. Prepare other sheets with the same % formatting
run_eff_sheet = full_report[['Machine Number', 'Shift', 'Run Efficiency']].copy()

actual_eff_sheet = full_report[['Machine Number', 'Shift', 'Actual Efficiency']].copy()

# --- 5. EXPORT WITH EXCEL FORMATTING ---
output_path = os.path.join(base_path, 'Production_Analysis.xlsx')

# Use engine='xlsxwriter' to allow cell formatting
with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    workbook = writer.book
    # Define a Percentage Format (0% = whole number percentage, 0.0% = 1 decimal place)
    percent_format = workbook.add_format({'num_format': '0%'}) 
    
    # Write Sheet 1: Active<95%
    low_power_final.to_excel(writer, sheet_name='Active<95%', index=False)
    worksheet = writer.sheets['Active<95%']
    # Columns 'D' and 'E' (Run Efficiency and Actual Efficiency) get the format
    worksheet.set_column('D:E', 15, percent_format)

    # Write Sheet 2: Run Efficiency
    run_eff_sheet.to_excel(writer, sheet_name='Run Efficiency', index=False)
    worksheet = writer.sheets['Run Efficiency']
    # Column 'C' gets the format
    worksheet.set_column('C:C', 15, percent_format)

    # Write Sheet 3: Actual Efficiency
    actual_eff_sheet.to_excel(writer, sheet_name='Actual Efficiency', index=False)
    worksheet = writer.sheets['Actual Efficiency']
    # Column 'C' gets the format
    worksheet.set_column('C:C', 15, percent_format)

print(f"Analysis Complete. Results saved to: {output_path}")