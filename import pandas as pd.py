import pandas as pd
import os
import sys
from datetime import time, timedelta
import pywhatkit as kit
import time as time_lib

# --- 1. FILE PATH SETUP ---
base_path = os.path.dirname(os.path.abspath(__file__))
beam_file = os.path.join(base_path, 'BEAM BOOK.xlsx')
efficiency_file = os.path.join(base_path, 'WjEff.xlsx')
quality_file = os.path.join(base_path, 'Quality Data.xlsx')
machine_file = os.path.join(base_path, 'Machine Data.xlsx')

# Check all files exist
for f in [file_name, quality_file, machine_file]:
    if not os.path.exists(f):
        print(f"Error: Required file '{f}' missing."); sys.exit()


# --- 2. HELPER FUNCTIONS ---
def time_to_minutes(val): #makes time readable (for efficiency file)
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

def process_shift(data, shift_type, shift_limit, date_label): #formulas for Efficiency calculations (for efficiency file)
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
df_efficiency_lookup = pd.read_excel(efficiency_file)

excel_file = pd.read_excel(df_efficiency_lookup, sheet_name=None, skiprows=1)
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

    

# --- 4. MAIN PROCESSING (WITH VALIDATION) ---
excel_file = pd.read_excel(beam_file, sheet_name=None, skiprows=1)
all_data_list = []


