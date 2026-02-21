import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Production Dashboard", layout="wide")

# --- 1. CUSTOM CSS FOR FOOTNOTES ---
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

# Load the file
file_path = 'Waterjet Efficiency Shiftwise.xlsx'

@st.cache_data
def load_data(path, sheet):
    return pd.read_excel(path, sheet_name=sheet)

try:
    # 2. DATA LOADING
    df_takeaway = load_data(file_path, 'Key Takeaways')
    df_beam = load_data(file_path, 'Beam Status')
    df_stock = load_data(file_path, 'Beam Stock')

    # Convert DATE to datetime for proper chronological sorting
    df_takeaway['DATE_OBJ'] = pd.to_datetime(df_takeaway['DATE'], format='%d-%m-%y', errors='coerce')
    df_takeaway = df_takeaway.sort_values('DATE_OBJ', ascending=False)

    # --- 3. DAILY PRODUCTION SECTION ---
    st.header("📊 Daily Production")
    
    col1, col2 = st.columns(2)
    with col1:
        date_list = df_takeaway['DATE'].unique()
        # Default to the most recent date
        selected_date = st.selectbox("Select Date:", date_list, index=0)
    with col2:
        # Default to Total
        shift_choice = st.selectbox("Select Shift:", ["Total", "Day", "Night"], index=0)

    # Map the dropdown choice to the exact Excel column headers
    shift_suffix = shift_choice.upper()
    
    cols_to_show = [
        'QUALITY', 
        f'NOM ({shift_suffix})', 
        f'PRODUCTION METER ({shift_suffix})', 
        f'TRUE EFFICIENCY (QUALITY) ({shift_suffix})', 
        f'DIFFERENCE ({shift_suffix})'
    ]
    
    # Filter Data for Table
    day_data = df_takeaway[df_takeaway['DATE'] == selected_date]
    filtered_df = day_data[cols_to_show]
    
    # Calculate Metrics for the Cards
    total_prod = filtered_df[f'PRODUCTION METER ({shift_suffix})'].sum()
    raw_total_eff = day_data[f'TRUE EFFICIENCY (TOTAL) ({shift_suffix})'].iloc[0]

    # Display Table with percentage formatting
    st.dataframe(
        filtered_df,
        use_container_width=True,
        column_config={
            f"TRUE EFFICIENCY (QUALITY) ({shift_suffix})": st.column_config.NumberColumn("True Efficiency", format="%.2f%%"),
            f"DIFFERENCE ({shift_suffix})": st.column_config.NumberColumn("Difference", format="%.2f")
        },
        hide_index=True
    )

    # Display Summary Cards below table
    m1, m2 = st.columns(2)
    m1.metric("Total Production Meter", f"{int(total_prod)}")
    m2.metric("True Efficiency (Total)", f"{raw_total_eff:.2%}")

    st.divider()

    # --- 4. ACTIVE BEAM STATUS ---
    st.header("🧶 Active Beam Status")
    
    qualities_beam = ["All"] + sorted(list(df_beam['Quality'].unique()))
    selected_qual_beam = st.selectbox("Filter Beam by Quality:", qualities_beam, key="beam_qual")
    
    filtered_beam = df_beam if selected_qual_beam == "All" else df_beam[df_beam['Quality'] == selected_qual_beam]

    # Color logic for Pending Meters
    def style_pending(v):
        return 'color: red; font-weight: bold' if v < 1000 else 'color: black'

    st.dataframe(
        filtered_beam.style.map(style_pending, subset=['Pending Meters']),
        use_container_width=True,
        hide_index=True
    )

    # Footnotes for Beam Status
    nom_running = filtered_beam['Mc no'].nunique()
    total_pending = filtered_beam['Pending Meters'].sum()
    st.markdown(f'<p class="footnote">NOM Running: {nom_running} | Warp Pending: {int(total_pending)} Meters</p>', unsafe_allow_html=True)

    st.divider()

    # --- 5. BEAM STOCK ---
    st.header("📦 Beam Stock")
    
    qualities_stock = ["All"] + sorted(list(df_stock['Quality'].unique()))
    selected_qual_stock = st.selectbox("Filter Stock by Quality:", qualities_stock, key="stock_qual")
    
    filtered_stock = df_stock if selected_qual_stock == "All" else df_stock[df_stock['Quality'] == selected_qual_stock]

    st.dataframe(filtered_stock, use_container_width=True, hide_index=True)

    # Footnotes for Stock
    num_beams = len(filtered_stock)
    total_stock_meters = filtered_stock['Warp Meter'].sum()
    st.markdown(f'<p class="footnote">Number of Beams: {num_beams} | Beam Meter: {int(total_stock_meters)} Meters</p>', unsafe_allow_html=True)

    st.divider()

    # --- 6. EFFICIENCY TREND ---
    st.header("📈 Efficiency Trend")
    
    # Use only one entry per date (Totals) for the line chart
    trend_df = df_takeaway.drop_duplicates('DATE').sort_values('DATE_OBJ')
    
    fig = px.line(
        trend_df, 
        x='DATE', 
        y='TRUE EFFICIENCY (TOTAL) (TOTAL)', 
        markers=True,
        title="Overall Daily Efficiency Performance"
    )
    
    # Treat X-axis as individual days (categorical) rather than a continuous timeline
    fig.update_xaxes(type='category', title="Date")
    fig.update_yaxes(tickformat=".1%", title="Total Efficiency")
    
    st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Waiting for fresh data or file path issue. Error: {e}")