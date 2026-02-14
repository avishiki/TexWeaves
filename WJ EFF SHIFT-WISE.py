import pandas as pd
import os
import sys
from datetime import time, timedelta
import pywhatkit as kit
import time as time_lib

# --- 1. FILE PATH SETUP ---
base_path = os.path.dirname(os.path.abspath(__file__))
file_name = os.path.join(base_path, 'WjEff.xlsx')
quality_file = os.path.join(base_path, 'Quality Data.xlsx')
machine_file = os.path.join(base_path, 'Machine Data.xlsx')
beam_book_file = os.path.join(base_path, 'BEAM BOOK.xlsx')

# Check all files exist
for f in [file_name, quality_file, machine_file]:
    if not os.path.exists(f):
        print(f"Error: Required file '{f}' missing."); sys.exit()

# --- 2. HELPER FUNCTIONS ---
def time_to_minutes(val):
    if pd.isna(val) or val == "": return 0
    if isinstance(val, time): return (val.hour * 60) + val.minute
    if isinstance(val, timedelta): return int(val.total_seconds() / 60)
    s_val = str(val).strip()
    for sep in [':', ';', '.']:
        if sep in s_val:
            try:
                parts = s_val.split(sep)
                h = int(float(parts[0]))
                m = int(float(parts[1])) if len(parts) > 1 and parts[1] != "" else 0
                return (h * 60) + m
            except: continue
    try: return int(float(s_val)) * 60
    except: return 0

def process_shift(data, shift_type, shift_limit, date_label):
    temp_df = data.copy()
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

# --- 4. MAIN PROCESSING (WITH VALIDATION) ---
excel_file = pd.read_excel(file_name, sheet_name=None, skiprows=1)
all_data_list = []

for sheet_name, df in excel_file.items():
    
    if df.shape[1] >= 11:
        DAY_LIMIT = 11 * 60
        NIGHT_LIMIT = 13 * 60
        
        # --- 1. PROCESS DAY SHIFT ---
        # Select columns: Machine(0), Qual(1), Power(2), Run(3), Stops(4), RPM(5)
        day_raw = df.iloc[:, [0, 1, 2, 3, 4, 5]]
        
        #Skip Rows where Power Mins, Run Mins and Stops are empty.
        day_mask = day_raw.iloc[:, [2, 3, 4]].notna().any(axis=1)
        day_valid = day_raw[day_mask].copy()
        if not day_valid.empty:
            all_data_list.append(process_shift(day_valid, 'Day', DAY_LIMIT, sheet_name))

        # --- 2. PROCESS NIGHT SHIFT ---
        # Select columns: Machine(0), Qual(6), Power(7), Run(8), Stops(9), RPM(10)
        night_raw = df.iloc[:, [0, 6, 7, 8, 9, 10]]

        #Skip Rows where Power Mins, Run Mins and Stops are empty.
        night_mask = night_raw.iloc[:, [2, 3, 4]].notna().any(axis=1)
        night_valid = night_raw[night_mask].copy()
        if not night_valid.empty:
            all_data_list.append(process_shift(night_valid, 'Night', NIGHT_LIMIT, sheet_name))

if not all_data_list:
    print("No valid production data found."); sys.exit()

#Combining everything
full_report = pd.concat(all_data_list, ignore_index=True)
# Convert the 'Date' strings to actual datetime objects for sorting
# format='%d-%m-%y' matches '31-1-26'
full_report['Date_Obj'] = pd.to_datetime(full_report['Date'], format='%d-%m-%y', errors='coerce')
full_report['Shift_Rank'] = full_report['Shift'].map({'Day': 1, 'Night': 2})

# SORT: Latest Date and Latest Shift at top
full_report = full_report.sort_values(by=['Date_Obj', 'Shift_Rank', 'Machine Number'], ascending=[False, False, True])
latest_date_str = full_report.iloc[0]['Date']
latest_shift_str = full_report.iloc[0]['Shift']

# --- 5. KEY TAKEAWAYS (WIDE FORMAT) ---
def get_stats(df_subset, suffix):
    qual_stats = df_subset.groupby(['Date', 'Quality']).agg(
        NOM=('Machine Number', 'nunique'), 
        Prod_Sum=('Prod_Meter', 'sum'), 
        True_Sum=('True_Prod_Meter', 'sum')
    ).reset_index()
    
    date_totals = df_subset.groupby('Date').agg(
        T_Prod=('Prod_Meter', 'sum'), 
        T_True=('True_Prod_Meter', 'sum')
    ).reset_index()
    
    res = qual_stats.merge(date_totals, on='Date', how='left')
    
    # Create the clean names directly
    res[f'NOM ({suffix})'] = res['NOM']
    res[f'PRODUCTION METER ({suffix})'] = res['Prod_Sum']
    res[f'TRUE EFFICIENCY (QUALITY) ({suffix})'] = (res['Prod_Sum'] / res['True_Sum']).fillna(0)
    res[f'TRUE EFFICIENCY (TOTAL) ({suffix})'] = (res['T_Prod'] / res['T_True']).fillna(0)
    res[f'DIFFERENCE ({suffix})'] = (res[f'TRUE EFFICIENCY (QUALITY) ({suffix})'] - res[f'TRUE EFFICIENCY (TOTAL) ({suffix})']) * 100
    
    return res[['Date', 'Quality', f'NOM ({suffix})', f'PRODUCTION METER ({suffix})', 
                f'TRUE EFFICIENCY (QUALITY) ({suffix})', f'TRUE EFFICIENCY (TOTAL) ({suffix})', f'DIFFERENCE ({suffix})']]

C_VAL = 18.28
#Feeds data from Quality Data Excel
merged = full_report.merge(df_quality_lookup[['Quality', 'Quality Pick']], on='Quality', how='left')
#Feeds data from Machine Data Excel
merged = merged.merge(df_machine_lookup[['Machine Number', 'True RPM']], on='Machine Number', how='left')
# Production Meter(per shift) = 18.28 * RUN RPM * ACTUAL EFFICIENCY / QUALITY PICK
merged['Prod_Meter'] = (C_VAL * merged['Run RPM'] * merged['Actual Efficiency']) / merged['Quality Pick']
# True Production Meter(per shift) = 18.28 * TRUE RPM * 1.0 (100% Eff) / QUALITY PICK
merged['True_Prod_Meter'] = (C_VAL * merged['True RPM']) / merged['Quality Pick']

total_stats = get_stats(merged, 'TOTAL')
day_stats = get_stats(merged[merged['Shift'] == 'Day'], 'DAY')
night_stats = get_stats(merged[merged['Shift'] == 'Night'], 'NIGHT')

final_takeaway = merged[['Date', 'Date_Obj', 'Quality']].drop_duplicates()
final_takeaway = final_takeaway.merge(total_stats, on=['Date', 'Quality'], how='left').merge(day_stats, on=['Date', 'Quality'], how='left').merge(night_stats, on=['Date', 'Quality'], how='left').fillna(0)

prod_cols = ['PRODUCTION METER (DAY)', 'PRODUCTION METER (NIGHT)', 'PRODUCTION METER (TOTAL)']
for col in prod_cols:
    final_takeaway[col] = final_takeaway[col].round(0).astype(int)

final_takeaway = final_takeaway.sort_values(by=['Date_Obj', 'Quality'], ascending=[False, True])
final_takeaway_output = final_takeaway.drop(columns=['Date_Obj']).rename(columns={'Date': 'DATE', 'Quality': 'QUALITY'})

# --- 5b. ADVANCED BEAM BOOK & RELOADING CALCULATIONS ---
if os.path.exists(beam_book_file):
    df_bb_raw = pd.read_excel(beam_book_file)
    
    # 1. Standardize All Date Columns
    date_cols = ['Loading Date', 'Bhidan Date', 'Re-Loading Date', 'Re-Bhidan Date']
    for col in date_cols:
        df_bb_raw[f'{col} Obj'] = pd.to_datetime(df_bb_raw[col], format='%d-%m-%y', errors='coerce')

    def calculate_cumulative_received(row):
        total_received = 0
        
        # --- Run 1: Primary Machine ---
        if pd.notna(row['Loading Date Obj']):
            m1 = row['Machine Number']
            start1 = row['Loading Date Obj']
            end1 = row['Bhidan Date Obj']
            
            mask1 = (merged['Machine Number'] == m1) & (merged['Date_Obj'] >= start1)
            if pd.notna(end1):
                mask1 &= (merged['Date_Obj'] <= end1)
            
            total_received += merged.loc[mask1, 'Prod_Meter'].sum()

        # --- Run 2: Re-Loaded Machine ---
        # Logic: Only calculate if Re-Loading is valid (>= previous Bhidan)
        if pd.notna(row['Re-Loading Date Obj']):
            # Validation: Reloading Date must be >= Bhidan Date
            if pd.isna(row['Bhidan Date Obj']) or (row['Re-Loading Date Obj'] >= row['Bhidan Date Obj']):
                m2 = row['Re-Machine Number']
                start2 = row['Re-Loading Date Obj']
                end2 = row['Re-Bhidan Date Obj']
                
                mask2 = (merged['Machine Number'] == m2) & (merged['Date_Obj'] >= start2)
                if pd.notna(end2):
                    mask2 &= (merged['Date_Obj'] <= end2)
                
                total_received += merged.loc[mask2, 'Prod_Meter'].sum()
        
        return total_received

    # Apply Cumulative Production Calculation
    df_bb_raw['Received Meters'] = df_bb_raw.apply(calculate_cumulative_received, axis=1)
    df_bb_raw['Pending Meters'] = df_bb_raw['Warp Meter'] - df_bb_raw['Received Meters']
    
    # Shortage Rule: If Pending < 7% of Warp Meter, it's considered Empty/Complete
    df_bb_raw['Is_Complete'] = df_bb_raw['Pending Meters'] < (0.10 * df_bb_raw['Warp Meter'])

    # --- CATEGORY A: BEAM STATUS (Currently Running) ---
    # Logic: (Loaded but no Bhidan) OR (Re-Loaded but no Re-Bhidan)
    active_mask = (
        (df_bb_raw['Loading Date Obj'].notna() & df_bb_raw['Bhidan Date Obj'].isna()) |
        (df_bb_raw['Re-Loading Date Obj'].notna() & df_bb_raw['Re-Bhidan Date Obj'].isna())
    )
    df_status = df_bb_raw[active_mask].copy()

    if not df_status.empty:
        # Determine current machine and loading date for display
        def get_current_info(row):
            if pd.notna(row['Re-Loading Date Obj']) and pd.isna(row['Re-Bhidan Date Obj']):
                return row['Re-Machine Number'], row['Re-Loading Date']
            return row['Machine Number'], row['Loading Date']

        df_status[['Curr_Mc', 'Curr_Load']] = df_status.apply(
            lambda x: pd.Series(get_current_info(x)), axis=1
        )

        df_status['Received Meters'] = df_status['Received Meters'].round(0).astype(int)
        df_status['Pending Meters'] = df_status['Pending Meters'].round(0).astype(int)
        
        # Format Date for Display
        df_status['Curr_Load_Str'] = pd.to_datetime(df_status['Curr_Load']).dt.strftime('%d/%m/%Y')

        beam_status_output = df_status[[
            'Curr_Mc', 'Curr_Load_Str', 'Beam No', 'Quality', 
            'Warp Meter', 'Received Meters', 'Pending Meters'
        ]].rename(columns={'Curr_Mc': 'Mc no', 'Curr_Load_Str': 'Loading Date'})
        
        beam_status_output = beam_status_output.sort_values(by='Mc no')
    else:
        beam_status_output = pd.DataFrame()

    # --- CATEGORY B: BEAM STOCK (In Warehouse) ---
    # Logic: 
    # 1. Never loaded AND No Bhidan
    # 2. OR: Finished (Bhidan) but NOT Re-Loaded AND NOT Complete (>7% left)
    stock_mask = (
        (df_bb_raw['Loading Date Obj'].isna() & df_bb_raw['Bhidan Date Obj'].isna()) |
        (df_bb_raw['Bhidan Date Obj'].notna() & df_bb_raw['Re-Loading Date Obj'].isna() & ~df_bb_raw['Is_Complete'])
    )
    df_stock = df_bb_raw[stock_mask].copy()
    
    if not df_stock.empty:
        # If it was previously run, show the Date it came off the machine
        df_stock['Stock_Date'] = df_stock['Bhidan Date'].fillna(df_stock['Date'])
        df_stock['Stock_Date_Str'] = pd.to_datetime(df_stock['Stock_Date']).dt.strftime('%d/%m/%Y')
        
        # For stock, the "Meter" column should show what is actually left (Pending Meters)
        beam_stock_output = df_stock[['Stock_Date_Str', 'Beam No', 'Pending Meters', 'Quality']].copy()
        beam_stock_output.columns = ['Date', 'Beam No', 'Warp Meter', 'Quality']
    else:
        beam_stock_output = pd.DataFrame()

else:
    print("Warning: BEAM BOOK.xlsx not found.")
    beam_status_output = pd.DataFrame()
    beam_stock_output = pd.DataFrame()

# --- 6. CHRONIC PERFORMANCE (4 SHIFT STREAK) ---
history_df = full_report.sort_values(by=['Machine Number', 'Date_Obj', 'Shift_Rank'], ascending=[True, True, True])
chronic_low_performers = []
for machine, group in history_df.groupby('Machine Number'):
    if len(group) >= 4:
        last_4 = group.tail(4)
        if (last_4['Actual Efficiency'] < 0.90).all():
            if latest_date_str in last_4['Date'].values:
                chronic_low_performers.append(machine)

output_path = os.path.join(base_path, 'Waterjet Efficiency Shiftwise.xlsx')
with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    workbook = writer.book
    #Specifically for Keytakeaways SHEET
    pct_fmt = workbook.add_format({'num_format': '0.00%', 'align': 'center'})
    int_fmt = workbook.add_format({'num_format': '0', 'align': 'center'}) # Format for Meters
    dec_fmt = workbook.add_format({'num_format': '0.00', 'align': 'center'})
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#CFE2F3', 'border': 1, 'align': 'center'})

    percent_fmt = workbook.add_format({'num_format': '0%', 'align': 'center'})
    date_fmt   = workbook.add_format({'num_format': 'dd-mm-yyyy', 'align': 'left'})

    #Formating KeyTakeaways SHEET
    final_takeaway_output.to_excel(writer, sheet_name='Key Takeaways', index=False)
    ws = writer.sheets['Key Takeaways']
    for col in ['E:F', 'J:K', 'O:P']: ws.set_column(col, 15, pct_fmt)
    for col in ['D:D', 'I:I', 'N:N']: ws.set_column(col, 15, int_fmt)
    for col in ['G:G', 'L:L', 'Q:Q']: ws.set_column(col, 12, dec_fmt)
    ws.set_column('A:B', 15)

    def write_sheet(df, name, pct_cols):
        df.to_excel(writer, sheet_name=name, index=False)
        wsh = writer.sheets[name]
        wsh.freeze_panes(1, 0)
        wsh.set_column('A:D', 15)
        for c in pct_cols: wsh.set_column(f'{c}:{c}', 15, pct_fmt)
        for i, val in enumerate(df.columns): wsh.write(0, i, val, header_fmt)

    
    write_sheet(full_report[full_report['Active_Ratio'] < 0.95][['Date', 'Machine Number', 'Shift', 'Quality', 'Stops', 'Run Efficiency', 'Actual Efficiency']], 'Active<95%', ['F', 'G'])
    write_sheet(full_report[['Date', 'Machine Number', 'Shift', 'Quality', 'Run Efficiency']], 'Run Efficiency', ['E'])
    write_sheet(full_report[['Date', 'Machine Number', 'Shift', 'Quality', 'Actual Efficiency']], 'Actual Efficiency', ['E'])


    # 1. Write Beam Status Sheet
    date_format = workbook.add_format({'num_format': 'dd/mm/yyyy', 'align': 'center'})

    if not beam_status_output.empty:
        beam_status_output.to_excel(writer, sheet_name='Beam Status', index=False)
        ws_status = writer.sheets['Beam Status']
        ws_status.set_column('A:A', 15, None)
        ws_status.set_column('B:B', 15, date_format)
        ws_status.set_column('c:G', 15, None)
        

        
        # Special format for Pending Meters
        warn_fmt = workbook.add_format({'num_format': '0'})
        ws_status.set_column('G:G', 15, warn_fmt) # Column G is Pending Meters
        
        for i, val in enumerate(beam_status_output.columns):
            ws_status.write(0, i, val, header_fmt)

    # Create the low pending list for WhatsApp
    if not beam_status_output.empty:
        # Since beam_status_output already excludes finished beams (Bhidan), 
        # we only need to filter for Pending Meters < 1000
        low_pending_list = beam_status_output[beam_status_output['Pending Meters'] < 1000].copy()
    else:
        low_pending_list = pd.DataFrame()        

    # 2. Write Beam Stock Sheet
    if not beam_stock_output.empty:
        beam_stock_output.to_excel(writer, sheet_name='Beam Stock', index=False)
        ws_stock = writer.sheets['Beam Stock']
        
         # Center the date column
        center_fmt = workbook.add_format({'align': 'center'})
        ws_stock.set_column('A:A', 15, center_fmt) 
        ws_stock.set_column('B:D', 15, center_fmt)
        
        for i, val in enumerate(beam_stock_output.columns):
            ws_stock.write(0, i, val, header_fmt)

print(f"Excel Generated: {output_path}")

# --- 8. WHATSAPP ALERTS (SHIFT-WISE) ---

import webbrowser

def warmup_whatsapp():
    print("Warming up WhatsApp Web...")
    # This just opens the browser so it's ready in the background
    webbrowser.open("https://web.whatsapp.com")
    time_lib.sleep(25) # Give it 25 seconds to fully load the chats

def send_alerts():
    recipients = ["+919638832321"]
    # Filter only for current shift
    shift_low = full_report[(full_report['Date'] == latest_date_str) & (full_report['Shift'] == latest_shift_str) & (full_report['Active_Ratio'] < 0.90)]
    shift_stops = full_report[(full_report['Date'] == latest_date_str) & (full_report['Shift'] == latest_shift_str) & (pd.to_numeric(full_report['Stops'], errors='coerce') > 30)]

    msg = f"*📊 {latest_shift_str.upper()} SHIFT ALERTS - {latest_date_str}* 📊\n"
    if chronic_low_performers:
        msg += "\n*📉 CHRONIC UNDERPERFORMERS (Last 4 Shifts < 90%)*\n"
        for m in chronic_low_performers: msg += f"• Machine {m}\n"
    if not shift_low.empty:
        msg += f"\n*⚠️ LOW ACTIVITY*\n"
        for _, r in shift_low.iterrows(): msg += f"• Machine {r['Machine Number']} | {r['Active_Ratio']:.1%} | Stops: {int(r['Stops'])}\n"
    if not shift_stops.empty:
        msg += f"\n*🛑 HIGH STOPS*\n"
        for _, r in shift_stops.iterrows(): msg += f"• Machine {r['Machine Number']} | Stops: {int(r['Stops'])}\n"

    # Machines with less than 100 meters pending are about to finish (Bhidan)

    if not low_pending_list.empty:
        msg += "\n*🧶 UPCOMING BHIDAN (Pending < 1000m)*\n"
        for _, r in low_pending_list.iterrows():
            msg += f"• Machine {r['Mc no']} | Quality: {r['Quality']} | Rem: {r['Pending Meters']}m\n"
    else:
        msg += "\n*NO BHIDAN FOR NEXT 5 DAYS*\n"        

    for p in recipients:
        kit.sendwhatmsg_instantly(p, msg, wait_time=25, tab_close=True)
        time_lib.sleep(15)

def send_prod_summary():
    recipients = ["+919638832321"]
    df = pd.read_excel(output_path, sheet_name='Key Takeaways')
    latest_data = df[df['DATE'] == latest_date_str]
    
    # Match the suffix used in the header renaming
    sfx = latest_shift_str.upper() 
    
    msg = f"*📈 {sfx} SHIFT SUMMARY - {latest_date_str}* 📈\n"
    
    for _, r in latest_data.iterrows():
        # Only include qualities that actually ran in this shift
        if r[f'PRODUCTION METER ({sfx})'] > 0:
            msg += f"\n*Qual: {r['QUALITY']} | {r[f'NOM ({sfx})']}*\n"
            msg += f"• Meter: {int(r[f'PRODUCTION METER ({sfx})'])}\n"
            msg += f"• Eff: {r[f'TRUE EFFICIENCY (QUALITY) ({sfx})']:.2%}\n"
            msg += f"• Diff: {r[f'DIFFERENCE ({sfx})']:+.2f}\n"
    
    total_prod = latest_data[f'PRODUCTION METER ({sfx})'].sum()
    total_eff = latest_data[f'TRUE EFFICIENCY (TOTAL) ({sfx})'].iloc[0]
    
    msg += f"\n*__________________________*\n"
    msg += f"*TOTAL {sfx} PROD: {int(total_prod)}*\n"
    msg += f"*TOTAL {sfx} EFF: {total_eff:.2%}*"

    for p in recipients:
        print(f"Sending Production Summary to {p}...")
        kit.sendwhatmsg_instantly(p, msg, wait_time=25, tab_close=True)
        time_lib.sleep(10)

warmup_whatsapp()
send_alerts()
send_prod_summary()

import subprocess
import os

def upload_to_github():
    try:
        os.chdir(base_path) 
        print("Syncing with GitHub...")
        
        # We add --force to the push command
        subprocess.run('git add .', shell=True, check=True)
        subprocess.run('git commit -m "Auto-update production data"', shell=True, check=True)
        subprocess.run('git push origin main --force', shell=True, check=True) # Added --force
        
        print("✅ Online Dashboard Updated Successfully.")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ GitHub Upload Failed. Error code: {e.returncode}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

upload_to_github()