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
    if pd.isna(val) or val == "": 
        return 0
    
    # 1. Handle actual Python time objects (from Excel Time formatting)
    if isinstance(val, time): 
        return (val.hour * 60) + val.minute
    
    # 2. Handle timedelta objects
    if isinstance(val, timedelta): 
        return int(val.total_seconds() / 60)

    # Convert to string and clean spaces
    s_val = str(val).strip()

    # 3. Check for separators (:, ;, .) 
    # If a separator is found, we treat it as Hours.Minutes
    for sep in [':', ';', '.']:
        if sep in s_val:
            try:
                parts = s_val.split(sep)
                hours = int(float(parts[0]))
                minutes = int(float(parts[1])) if len(parts) > 1 and parts[1] != "" else 0
                return (hours * 60) + minutes
            except (ValueError, TypeError):
                continue 

    # 4. IF NO SEPARATOR IS FOUND
    # Treat the naked number as HOURS (e.g., 11 -> 660)
    try:
        # Convert to float first to handle cases like "11.0"
        return int(float(s_val)) * 60
    except (ValueError, TypeError):
        return 0

def process_shift(data, shift_type, shift_limit, date_label):
    temp_df = data.copy()
    # Standardizing names
    temp_df.columns = ['Machine Number', 'Quality', 'Power Time', 'Run Tim'
    'e', 'Stops', 'Efficiency Raw']
    
    temp_df['Power Mins'] = temp_df['Power Time'].apply(time_to_minutes)
    temp_df['Run Mins'] = temp_df['Run Time'].apply(time_to_minutes)
    
    # We use Power Mins / shift_limit to see if the machine was "Active" (On)
    temp_df['Active_Ratio'] = temp_df['Run Mins'] / shift_limit
    
    # Efficiency calculations
    temp_df['Run Efficiency'] = (temp_df['Run Mins'] / temp_df['Power Mins']).fillna(0).replace([float('inf'), -float('inf')], 0)
    temp_df['Actual Efficiency'] = (temp_df['Run Mins'] / shift_limit).fillna(0)
    
    temp_df['Shift'] = shift_type
    temp_df['Date'] = date_label 
    return temp_df

# --- 3. MAIN PROCESSING (ALL SHEETS) ---
excel_file = pd.read_excel(file_name, sheet_name=None, skiprows=1)

all_data_list = []

for sheet_name, df in excel_file.items():
    if df.shape[1] >= 11:
        DAY_LIMIT = 11 * 60
        NIGHT_LIMIT = 13 * 60

        # Process Day and Night
        day_p = process_shift(df.iloc[:, [0, 1, 2, 3, 4, 5]], 'Day', DAY_LIMIT, sheet_name)
        night_p = process_shift(df.iloc[:, [0, 6, 7, 8, 9, 10]], 'Night', NIGHT_LIMIT, sheet_name)
        
        all_data_list.append(day_p)
        all_data_list.append(night_p)

# Combine everything
full_report = pd.concat(all_data_list, ignore_index=True)

# --- NEW: CHRONOLOGICAL SORTING ---
# Convert the 'Date' strings to actual datetime objects for sorting
# format='%d-%m-%y' matches '31-1-26'
full_report['Date_Obj'] = pd.to_datetime(full_report['Date'], format='%d-%m-%y', errors='coerce')

# First, create a numeric rank for shifts so Night (2) comes before Day (1) in descending sort
full_report['Shift_Rank'] = full_report['Shift'].map({'Day': 1, 'Night': 2})

# SORT THE MAIN REPORT: Latest Date at top, then Night Shift, then Day Shift, then Machine Order
full_report = full_report.sort_values(
    by=['Date_Obj', 'Shift_Rank', 'Machine Number'], 
    ascending=[False, False, True])

# Now that we have sorted, we can find the "True" Latest Date for WhatsApp
latest_date_str = full_report.iloc[0]['Date']

# --- 5. CHRONIC PERFORMANCE ANALYSIS (TEMPORARY COPY) ---
# We use a COPY here so we don't mess up the main full_report sorting
history_df = full_report.sort_values(
    by=['Machine Number', 'Date_Obj', 'Shift_Rank'], 
    ascending=[True, True, True]
)

chronic_low_performers = []

# Group by machine to analyze each machine's specific timeline
for machine, group in history_df.groupby('Machine Number'):
    # Requirement: Machine must have at least 4 recorded shifts in the file
    if len(group) >= 4:
        # Grab exactly the 4 most recent chronological shifts for this machine
        last_4_shifts = group.tail(4)
        
        # CONDITION: Every single one of these 4 specific shifts must be < 85%
        # .all() returns True ONLY if the condition is met for all 4 rows
        if (last_4_shifts['Actual Efficiency'] < 0.90).all():
            
            # RELEVANCE: We only alert if the machine's "Last Shift" was on the latest date
            # This prevents alerting for machines that haven't run in weeks
            if last_4_shifts.iloc[-1]['Date'] == latest_date_str:
                chronic_low_performers.append(machine)


# --- 4. PREPARE THE 3 OUTPUT DATASETS ---

# 1. Active < 95% (Machine Active Time vs Shift Duration)
low_power_final = full_report[full_report['Active_Ratio'] < 0.95][
    ['Date', 'Machine Number', 'Shift', 'Quality', 'Stops', 'Run Efficiency', 'Actual Efficiency']
].copy()

# 2. Run Efficiency Sheet
run_eff_final = full_report[['Date', 'Machine Number', 'Shift', 'Run Efficiency']].copy()

# 3. Actual Efficiency Sheet
actual_eff_final = full_report[['Date', 'Machine Number', 'Shift', 'Actual Efficiency']].copy()

# --- 5. EXPORT WITH PROFESSIONAL FORMATTING ---
output_path = os.path.join(base_path, 'Waterjet_Efficiency.xlsx')

with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    workbook = writer.book
    
    # Define Formats
    percent_fmt = workbook.add_format({'num_format': '0%', 'align': 'center'})
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#CFE2F3', 'border': 1, 'align': 'center'})
    date_fmt   = workbook.add_format({'num_format': 'dd-mm-yyyy', 'align': 'left'})

    def write_formatted_sheet(df, sheet_name, pct_cols):
        # Write Header
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        
        # Freeze the top row
        worksheet.freeze_panes(1, 0)
        
        # Set column widths and formats
        # Date Column (A)
        worksheet.set_column('A:A', 15, None)
        # Machine, Shift, Quality (B, C, D)
        worksheet.set_column('B:D', 15, None)
        
        # Apply percentage format to specific columns
        for col_letter in pct_cols:
            worksheet.set_column(f'{col_letter}:{col_letter}', 18, percent_fmt)
            
        # Format the header row specifically
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_fmt)

    # Execute Writing
    # For low_power_final: F=Run Eff, G=Actual Eff
    write_formatted_sheet(low_power_final, 'Active<95%', ['F', 'G'])
    # For others: D=Efficiency
    write_formatted_sheet(run_eff_final, 'Run Efficiency', ['D'])
    write_formatted_sheet(actual_eff_final, 'Actual Efficiency', ['D'])

print(f"Level Up Complete! Processed {len(excel_file)} dates into consolidated reports.")

import pywhatkit as kit
import time
from datetime import datetime

# --- 6. WHATSAPP ALERT SYSTEM ---

# 1. Configuration
# Add all recipient phone numbers with country code (e.g., '+91' for India)
recipients = ["+919638832321"] 


def send_whatsapp_summary(report_df, latest_date_target, chronic_list):
    # Filter for Latest Date Alerts
    daily_low = report_df[
        (report_df['Date'] == latest_date_target) & 
        (report_df['Active_Ratio'] < 0.90)
    ]
    
    daily_high_stops = report_df[
        (report_df['Date'] == latest_date_target) & 
        (pd.to_numeric(report_df['Stops'], errors='coerce').fillna(0) > 30)
    ]

    # If everything is empty, skip
    if daily_low.empty and daily_high_stops.empty and not chronic_list:
        print(f"No alerts for {latest_date_target}.")
        return

    # Construct Message
    message = f"*📊 PRODUCTION REPORT - {latest_date_target}* 📊\n"

    # Section 1: Chronic Low Performance (NEW)
    if chronic_list:
        message += "\n*📉 CHRONIC UNDERPERFORMERS*\n"
        message += "_(Efficiency < 90% for LAST 4 SHIFTS)_\n"
        for m in chronic_list:
            message += f"• Machine {m}\n"
        else:
            message += "\n_No such Machines under 90% Efficiency,_"    

    # Section 2: Low Activity
    if not daily_low.empty:
        message += "\n*⚠️ LOW ACTIVITY (< 85%)*\n"
        for _, row in daily_low.iterrows():
            message += f"• Machine {row['Machine Number']} | {row['Shift']} | {row['Active_Ratio']:.1%} | Stops: {int(row['Stops'])}\n"

    # Section 3: High Stops
    if not daily_high_stops.empty:
        message += "\n*🛑 HIGH STOPS (> 30)*\n"
        for _, row in daily_high_stops.iterrows():
            message += f"• Machine {row['Machine Number']} | {row['Shift']} | Stops: {int(row['Stops'])}\n"

    message += "\n_Action required for Chronic Underperformers._"

    # Send to recipients
    for phone in recipients:
        try:
            kit.sendwhatmsg_instantly(phone, message, wait_time=15, tab_close=True)
            time.sleep(10)
        except Exception as e:
            print(f"Error: {e}")

# Call the function with the new parameter
send_whatsapp_summary(full_report, latest_date_str, chronic_low_performers)