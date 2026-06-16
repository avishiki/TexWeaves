import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- 1. PAGE CONFIGURATION ---
st.set_page_config(page_title="Tex Weaves Dashboard", layout="wide")

# --- 2. SIDEBAR NAVIGATION ---
with st.sidebar:
    st.title("🏭 Tex Weaves")
    # This creates the icon/dropdown on the top left
    app_mode = st.selectbox("Switch Dashboard:", ["🏠 HAR MAN", "🏭 JOBWORK"])

# --- 3. CUSTOM CSS FOR FOOTNOTES (From your original code) ---
st.markdown("""
    <style>
    .footnote { 
        font-size: 1.2rem; 
        font-weight: bold; 
        color: #1f77b4; 
        margin-top: -10px; 
        padding-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

# File Paths
file_path = 'Waterjet Efficiency Shiftwise.xlsx'
beam_book_path = 'BEAM BOOK.xlsx'
yarn_book_path = 'YarnBook-TEX WEAVES.xlsx'

@st.cache_data(ttl=600)
def load_data(path, sheet, skip=0):
    if os.path.exists(path):
        return pd.read_excel(path, sheet_name=sheet, skiprows=skip)
    return None

# ==========================================
# PAGE: HAR MAN (YOUR ORIGINAL CODE)
# ==========================================
if app_mode == "🏠 HAR MAN":
    try:
        # --- LOADING YOUR ORIGINAL DATA ---
        df_takeaway = load_data(file_path, 'Key Takeaways')
        df_beam = load_data(file_path, 'Beam Status')
        df_stock = load_data(file_path, 'Beam Stock')

        # Convert DATE to datetime for proper chronological sorting
        df_takeaway['DATE_OBJ'] = pd.to_datetime(df_takeaway['DATE'], format='%d-%m-%y', errors='coerce')
        df_takeaway = df_takeaway.sort_values('DATE_OBJ', ascending=False)

        # --- YOUR ORIGINAL DAILY PRODUCTION SECTION ---
        st.header("📊 Daily Production")
        
        col1, col2 = st.columns(2)
        with col1:
            date_list = df_takeaway['DATE'].unique()
            selected_date = st.selectbox("Select Date:", date_list, index=0)
        with col2:
            shift_choice = st.selectbox("Select Shift:", ["Total", "Day", "Night"], index=0)

        shift_suffix = shift_choice.upper()
        cols_to_show = [
            'QUALITY', 
            f'NOM ({shift_suffix})', 
            f'PRODUCTION METER ({shift_suffix})', 
            f'TRUE EFFICIENCY (QUALITY) ({shift_suffix})', 
            f'DIFFERENCE ({shift_suffix})'
        ]
        
        day_data = df_takeaway[df_takeaway['DATE'] == selected_date]
        filtered_df = day_data[cols_to_show]
        
        total_prod = filtered_df[f'PRODUCTION METER ({shift_suffix})'].sum()
        raw_total_eff = day_data[f'TRUE EFFICIENCY (TOTAL) ({shift_suffix})'].iloc[0]

        st.dataframe(
            filtered_df,
            use_container_width=True,
            column_config={
                f"TRUE EFFICIENCY (QUALITY) ({shift_suffix})": st.column_config.NumberColumn("True Efficiency", format="%.2f%%"),
                f"DIFFERENCE ({shift_suffix})": st.column_config.NumberColumn("Difference", format="%.2f")
            },
            hide_index=True
        )

        m1, m2 = st.columns(2)
        m1.metric("Total Production Meter", f"{int(total_prod)}")
        m2.metric("True Efficiency (Total)", f"{raw_total_eff:.2%}")

        st.divider()

       # --- ACTIVE BEAM STATUS ---
        st.header("🧶 Active Beam Status")
        
        # Clean quality list to avoid errors with empty values
        qualities_beam = ["All"] + sorted([str(q) for q in df_beam['Quality'].unique() if pd.notna(q) and str(q).strip() != ""])
        selected_qual_beam = st.selectbox("Filter Beam by Quality:", qualities_beam, key="beam_qual")
        
        filtered_beam = df_beam if selected_qual_beam == "All" else df_beam[df_beam['Quality'] == selected_qual_beam]

        # FIX: Convert columns to numeric so math works, but we will format them in the display
        numeric_cols = ['Beam No', 'Beam Meters', 'Received Meters', 'Pending Meters']
        for col in numeric_cols:
            if col in filtered_beam.columns:
                filtered_beam[col] = pd.to_numeric(filtered_beam[col], errors='coerce')

        def style_pending(v):
            if pd.isna(v): return ''
            return 'color: red; font-weight: bold' if v < 1000 else 'color: black'

        # FIX: Use column_config to force 0 decimal places (%d)
        st.dataframe(
            filtered_beam.style.map(style_pending, subset=['Pending Meters']),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Mc no": st.column_config.NumberColumn("Mc no", format="%d"),
                "Beam No": st.column_config.NumberColumn("Beam No", format="%d"),
                "Beam Meters": st.column_config.NumberColumn("Beam Meters", format="%d"),
                "Received Meters": st.column_config.NumberColumn("Received Meters", format="%d"),
                "Pending Meters": st.column_config.NumberColumn("Pending Meters", format="%d"),
            }
        )

        # Footnotes (Calculating totals safely)
        # Only count rows where a Beam No actually exists
        nom_running = filtered_beam[filtered_beam['Beam No'].notna()]['Mc no'].nunique()
        total_pending = filtered_beam['Pending Meters'].sum(skipna=True)
        
        st.markdown(f'<p class="footnote">NOM Running: {nom_running} | Warp Pending: {int(total_pending)} Meters</p>', unsafe_allow_html=True)

        st.divider()

        # --- YOUR ORIGINAL BEAM STOCK ---
        st.header("📦 Beam Stock")
        
        qualities_stock = ["All"] + sorted(list(df_stock['Quality'].unique()))
        selected_qual_stock = st.selectbox("Filter Stock by Quality:", qualities_stock, key="stock_qual")
        
        filtered_stock = df_stock if selected_qual_stock == "All" else df_stock[df_stock['Quality'] == selected_qual_stock]

        st.dataframe(filtered_stock, use_container_width=True, hide_index=True)

        num_beams = len(filtered_stock)
        total_stock_meters = filtered_stock['Warp Meter'].sum()
        st.markdown(f'<p class="footnote">Number of Beams: {num_beams} | Beam Meter: {int(total_stock_meters)} Meters</p>', unsafe_allow_html=True)

        st.divider()

        # --- YOUR ORIGINAL EFFICIENCY TREND ---
        st.header("📈 Efficiency Trend")
        trend_df = df_takeaway.drop_duplicates('DATE').sort_values('DATE_OBJ')
        fig = px.line(trend_df, x='DATE', y='TRUE EFFICIENCY (TOTAL) (TOTAL)', markers=True, title="Overall Daily Efficiency Performance")
        fig.update_xaxes(type='category', title="Date")
        fig.update_yaxes(tickformat=".1%", title="Total Efficiency")
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading HAR MAN data: {e}")

# ==========================================
# PAGE: JOBWORK (THE NEW PAGE)
# ==========================================
elif app_mode == "🏭 JOBWORK":
    st.title("🤝 Jobwork - Third Party Tracking")
    
    try:
        # Load Data
        df_bb_raw = load_data(beam_book_path, 0)
        df_yb_raw = load_data(yarn_book_path, '25-26 JW', skip=1)

        # Filter Beam Book for Job Work only
        df_sent = df_bb_raw[df_bb_raw['JOB'].notna()].copy()

        # Sidebar Filters for Jobwork
        st.sidebar.divider()
        party_list = sorted(list(df_sent['JOB'].unique()))
        selected_party = st.sidebar.selectbox("Select Party:", party_list)
        
        qual_list = sorted(list(df_sent[df_sent['JOB'] == selected_party]['Quality'].unique()))
        selected_qual = st.sidebar.selectbox("Select Quality:", ["All"] + qual_list)

        # Apply Filters
        party_sent = df_sent[df_sent['JOB'] == selected_party]
        party_rec = df_yb_raw[df_yb_raw['PARTY'] == selected_party]

        if selected_qual != "All":
            party_sent = party_sent[party_sent['Quality'] == selected_qual]
            party_rec = party_rec[party_rec['QUALITY'] == selected_qual]

        # Calculate Summary
        total_sent = party_sent['Warp Meter'].sum()
        total_rec = party_rec['METER'].sum()
        net_pending = total_sent - total_rec

        # --- DISPLAY METRICS ---
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Sent (Mtrs)", f"{int(total_sent)}")
        k2.metric("Total Received (Mtrs)", f"{int(total_rec)}")
        k3.metric("Net Pending", f"{int(net_pending)}", delta_color="inverse")

        # --- DISPLAY TABLES ---
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("📤 Sent Beams")
            st.dataframe(party_sent[['Date', 'Beam No', 'Quality', 'Warp Meter']], use_container_width=True, hide_index=True)
            st.markdown(f'<p class="footnote">Beams Sent: {len(party_sent)}</p>', unsafe_allow_html=True)

        with col_right:
            st.subheader("📥 Received Meters")
            # Format Date for Display
            party_rec['BILL DATE'] = pd.to_datetime(party_rec['BILL DATE']).dt.strftime('%d/%m/%Y')
            st.dataframe(party_rec[['BILL DATE', 'QUALITY', 'METER']], use_container_width=True, hide_index=True)
            st.markdown(f'<p class="footnote">Installments Received: {len(party_rec)}</p>', unsafe_allow_html=True)

    except Exception as e:
        st.warning("Ensure 'JOBWORK' sheet exists in YarnBook and 'JOB' column exists in Beam Book.")
        st.error(f"Detail: {e}")