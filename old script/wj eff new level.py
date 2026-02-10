import pandas as pd
import os
import sys
from datetime import time, timedelta

# --- 1. FILE PATH SETUP ---
base_path = os.path.dirname(os.path.abspath(__file__))
file_name = os.path.join(base_path, 'WjEff.xlsx')
quality_file = os.path.join(base_path, 'Quality Data.xlsx')
machine_file = os.path.join(base_path, 'Machine Data.xlsx')


# Check all files exist
for f in [file_name, quality_file, machine_file]:
    if not os.path.exists(f):
        print(f"Error: Required file '{f}' missing."); sys.exit()

# --- 2. HELPER FUNCTIONS ---
def time_to_minutes(val):
    if pd.isna(val) or val == "":  return 0
    if isinstance(val, time): return (val.hour * 60) + val.minute
    if isinstance(val, timedelta):  return int(val.total_seconds() / 60)
    s_val = str(val).strip()
    for sep in [':', ';', '.']:
        if sep in s_val:
            try:
                parts = s_val.split(sep)
                hours = int(float(parts[0]))
                minutes = int(float(parts[1])) if len(parts) > 1 and parts[1] != "" else 0
                return (hours * 60) + minutes
            except (ValueError, TypeError):
                continue 
    try:
        # Convert to float first to handle cases like "11.0"
        return int(float(s_val)) * 60
    except (ValueError, TypeError):
        return 0

def process_shift(data, shift_type, shift_limit, date_label):
    temp_df = data.copy()
    
 # Now including 'Run RPM' (assuming it's at index 5 for Day and 11 for Night based on your sheet structure)
    temp_df.columns = ['Machine Number', 'Quality', 'Power Time', 'Run Time', 'Stops', 'Run RPM']
    
    temp_df['Power Mins'] = temp_df['Power Time'].apply(time_to_minutes)
    temp_df['Run Mins'] = temp_df['Run Time'].apply(time_to_minutes)
    temp_df['Active_Ratio'] = temp_df['Run Mins'] / shift_limit
    temp_df['Actual Efficiency'] = (temp_df['Run Mins'] / shift_limit).fillna(0)
    temp_df['Run Efficiency'] = (temp_df['Run Mins'] / temp_df['Power Mins']).fillna(0).replace([float('inf'), -float('inf')], 0)
    
    temp_df['Shift'] = shift_type
    temp_df['Date'] = date_label 
    return temp_df

# --- 3. LOAD EXTERNAL DATA ---
df_quality_lookup = pd.read_excel(quality_file)
df_machine_lookup = pd.read_excel(machine_file)


# --- 4. MAIN PROCESSING (ALL SHEETS) ---
excel_file = pd.read_excel(file_name, sheet_name=None, skiprows=1)
all_data_list = []

for sheet_name, df in excel_file.items():
    if df.shape[1] >= 11:
        DAY_LIMIT = 11 * 60
        NIGHT_LIMIT = 13 * 60

        # Columns: Machine(0), Qual(1), Pow(2), Run(3), Stop(4), RPM(5) | Night: RPM is (10)
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

# --- 5. KEY TAKEAWAYS CALCULATIONS ---
# Merge External Data
merged = full_report.merge(df_quality_lookup[['Quality', 'Quality Pick']], on='Quality', how='left')
merged = merged.merge(df_machine_lookup[['Machine Number', 'True RPM']], on='Machine Number', how='left')

# Math Constants
C = 18.28

# Production Meter(per shift) = 18.28 * RUN RPM * ACTUAL EFFICIENCY / QUALITY PICK
merged['Prod_Meter'] = (C * merged['Run RPM'] * merged['Actual Efficiency']) / merged['Quality Pick']

# True Production Meter(per shift) = 18.28 * TRUE RPM * 1.0 (100% Eff) / QUALITY PICK
merged['True_Prod_Meter'] = (C * merged['True RPM']) / merged['Quality Pick']

# A. Group by Date and Quality
takeaway_qual = merged.groupby(['Date', 'Date_Obj', 'Quality']).agg(
    NOM=('Machine Number', 'nunique'),
    Prod_Meter_Sum=('Prod_Meter', 'sum'),
    True_Prod_Sum=('True_Prod_Meter', 'sum')
).reset_index()

# B. Group by Date (for Total Eff)
takeaway_date = merged.groupby('Date').agg(
    Total_Prod=('Prod_Meter', 'sum'),
    Total_True_Prod=('True_Prod_Meter', 'sum')
).reset_index()

# Merge Date-wise totals back to Quality-wise summary
final_takeaway = takeaway_qual.merge(takeaway_date, on='Date', how='left')

# Efficiency Calculations
final_takeaway['TRUE EFFICIENCY(QUALITY)'] = (final_takeaway['Prod_Meter_Sum'] / final_takeaway['True_Prod_Sum'])
final_takeaway['TRUE EFFICIENCY(TOTAL)'] = (final_takeaway['Total_Prod'] / final_takeaway['Total_True_Prod'])
final_takeaway['DIFFERENCE'] = (final_takeaway['TRUE EFFICIENCY(QUALITY)'] - final_takeaway['TRUE EFFICIENCY(TOTAL)']) * 100

# Cleanup and Sorting
final_takeaway = final_takeaway.sort_values(by=['Date_Obj', 'Quality'], ascending=[False, True])
final_takeaway_output = final_takeaway[[
    'Date', 'NOM', 'Quality', 'Prod_Meter_Sum', 
    'TRUE EFFICIENCY(QUALITY)', 'TRUE EFFICIENCY(TOTAL)', 'DIFFERENCE'
]].copy()

# Convert to integer only for the output (Round first to be accurate)
final_takeaway_output['Prod_Meter_Sum'] = final_takeaway_output['Prod_Meter_Sum'].round(0).astype(int)

# Final rename for the sheet
final_takeaway_output = final_takeaway_output.rename(columns={'Prod_Meter_Sum': 'PRODUCTION METER'})

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
run_eff_final = full_report[['Date', 'Machine Number', 'Shift','Quality', 'Run Efficiency']].copy()

# 3. Actual Efficiency Sheet
actual_eff_final = full_report[['Date', 'Machine Number', 'Shift','Quality', 'Run Efficiency']].copy()

# --- 5. EXPORT WITH PROFESSIONAL FORMATTING ---
output_path = os.path.join(base_path, 'Waterjet Efficiency LU.xlsx')
with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    workbook = writer.book
    pct_fmt = workbook.add_format({'num_format': '0.00%', 'align': 'center'})
    dec_fmt = workbook.add_format({'num_format': '0.00', 'align': 'center'})
    int_fmt = workbook.add_format({'num_format': '0', 'align': 'center'}) # Format for Meters
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
    percent_fmt = workbook.add_format({'num_format': '0%', 'align': 'center'})
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#CFE2F3', 'border': 1, 'align': 'center'})
    date_fmt   = workbook.add_format({'num_format': 'dd-mm-yyyy', 'align': 'left'})



    # Write Takeaways Sheet
    final_takeaway_output.to_excel(writer, sheet_name='Key Takeaways', index=False)
    ws = writer.sheets['Key Takeaways']
    # Column A: Date, B: NOM, C: Quality
    # Column D: PRODUCTION METER (Apply integer format)
    ws.set_column('D:D', 18, int_fmt)
    ws.set_column('E:F', 25, pct_fmt) # Efficiencies
    ws.set_column('G:G', 15, dec_fmt) # Difference

    # Format Headers
    for col_num, value in enumerate(final_takeaway_output.columns.values):
        ws.write(0, col_num, value, header_fmt)


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
    write_formatted_sheet(run_eff_final, 'Run Efficiency', ['E'])
    write_formatted_sheet(actual_eff_final, 'Actual Efficiency', ['E'])

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

# --- 7. WHATSAPP (2): PRODUCTION SUMMARY ---

# 1. Configuration for the new message
production_recipients = ["+919638832321"]

def send_production_whatsapp():
    # Load the data directly from the newly created Excel file
    if not os.path.exists(output_path):
        print("Excel file not found for WhatsApp extraction.")
        return
        
    df_takeaway = pd.read_excel(output_path, sheet_name='Key Takeaways')
    
    # Identify latest date (since it's sorted, it's the first row)
    latest_date_val = df_takeaway.iloc[0]['Date']
    
    # Filter for only the latest date
    daily_stats = df_takeaway[df_takeaway['Date'] == latest_date_val]
    
    if daily_stats.empty:
        return

    # 2. Construct the Message
    msg = f"*📈 PRODUCTION SUMMARY - {latest_date_val}* 📈\n"
    
    for _, row in daily_stats.iterrows():
        # Using :.2% to format the decimals from Excel as percentages
        # Assuming your column names match the Excel output exactly
        msg += f"\n*Quality: {row['Quality']}*\n"
        msg += f"• Meter: {int(row['PRODUCTION METER'])}\n"
        msg += f"• Eff: {row['TRUE EFFICIENCY(QUALITY)']:.2%}\n"
        msg += f"• Diff: {row['DIFFERENCE']:+.2f}\n" # + sign shows positive/negative

    # Get the Total Efficiency (same for all rows of the same day)
    total_prod_Keytakeaways = daily_stats['PRODUCTION METER'].sum()
    total_eff = daily_stats.iloc[0]['TRUE EFFICIENCY(TOTAL)']
    
    msg += f"\n*__________________________*\n"
    msg += f"*TOTAL PRODUCTION METER: {int(total_prod_Keytakeaways)}*\n"
    msg += f"*TOTAL TRUE EFFICIENCY: {total_eff:.2%}*"

    # 3. Send to new recipients
    print(f"Sending Production Summary for {latest_date_val} to {len(production_recipients)} recipients...")
    for phone in production_recipients:
        try:
            # We use a slightly longer wait_time (20s) to ensure the larger message loads
            kit.sendwhatmsg_instantly(phone, msg, wait_time=20, tab_close=True)
            time.sleep(12) # Gap between messages
        except Exception as e:
            print(f"Error sending production summary to {phone}: {e}")

# Call the function (Ensure this is after the 'with pd.ExcelWriter' block is closed)
send_production_whatsapp()