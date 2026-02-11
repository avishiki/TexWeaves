import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Production Dashboard", layout="wide")

# --- CUSTOM CSS FOR FOOTNOTES ---
st.markdown("""
    <style>
    .footnote { font-size: 1.2rem; font-weight: bold; color: #1f77b4; margin-top: -15px; }
    </style>
    """, unsafe_allow_name_html=True)

# Load the file
file_path = 'Waterjet Efficiency Shiftwise.xlsx'

@st.cache_data
def load_data(path, sheet):
    return pd.read_excel(path, sheet_name=sheet)

try:
    # 1. DATA LOADING
    df_takeaway = load_data(file_path, 'Key Takeaways')
    df_beam = load_data(file_path, 'Beam Status')
    df_stock = load_data(file_path, 'Beam Stock')

    # Convert DATE to datetime for proper sorting
    df_takeaway['DATE_OBJ'] = pd.to_datetime(df_takeaway['DATE'], format='%d-%m-%y', errors='coerce')
    df_takeaway = df_takeaway.sort_values('DATE_OBJ', ascending=False)

    # --- 1. DAILY PRODUCTION SECTION ---
    st.header("📊 Daily Production")
    
    col1, col2 = st.columns(2)
    with col1:
        date_list = df_takeaway['DATE'].unique()
        selected_date = st.selectbox("Select Date:", date_list)
    with col2:
        shift_choice = st.selectbox("Select Shift:", ["Total", "Day", "Night"])

    # Determine columns based on shift
    suffix = f"({shift_choice.upper()})"
    cols_to_show = [
        'QUALITY', 
        f'NOM {suffix}', 
        f'PRODUCTION METER {suffix}', 
        f'TRUE EFFICIENCY (QUALITY) {suffix}', 
        f'DIFFERENCE {suffix}'
    ]
    
    # Filter Data for Table
    filtered_df = df_takeaway[df_takeaway['DATE'] == selected_date][cols_to_show]
    
    # Calculate Metrics for the Cards
    total_prod = filtered_df[f'PRODUCTION METER {suffix}'].sum()
    # Fetch True Efficiency (Total) - it's the same for all rows of that date/shift
    raw_total_eff = df_takeaway[df_takeaway['DATE'] == selected_date][f'TRUE EFFICIENCY (TOTAL) {suffix}'].iloc[0]

    # Display Table
    st.dataframe(
        filtered_df,
        use_container_width=True,
        column_config={
            f"TRUE EFFICIENCY (QUALITY) {suffix}": st.column_config.NumberColumn("Efficiency", format="%.2f%%", help="Efficiency in percentage"),
            f"DIFFERENCE {suffix}": st.column_config.NumberColumn("Difference", format="%.2f", help="Difference from average")
        },
        hide_index=True
    )

    # Display Summary Cards below table
    m1, m2 = st.columns(2)
    m1.metric("Total Production Meter", f"{int(total_prod)}")
    m2.metric("True Efficiency (Total)", f"{raw_total_eff:.2%}")

    st.divider()

    # --- 2. ACTIVE BEAM STATUS ---
    st.header("🧶 Active Beam Status")
    
    qualities_beam = ["All"] + list(df_beam['Quality'].unique())
    selected_qual_beam = st.selectbox("Filter Beam by Quality:", qualities_beam, key="beam_qual")
    
    filtered_beam = df_beam if selected_qual_beam == "All" else df_beam[df_beam['Quality'] == selected_qual_beam]

    # Styling for low pending
    def style_pending(v):
        return 'color: red; font-weight: bold' if v < 1000 else 'color: black'

    st.dataframe(
        filtered_beam.style.applymap(style_pending, subset=['Pending Meters']),
        use_container_width=True,
        hide_index=True
    )

    # Footnotes for Beam Status
    nom_running = filtered_beam['Mc no'].nunique()
    total_pending = filtered_beam['Pending Meters'].sum()
    st.markdown(f'<p class="footnote">NOM Running: {nom_running} | Warp Pending: {int(total_pending)} Meters</p>', unsafe_allow_name_html=True)

    st.divider()

    # --- 3. BEAM STOCK ---
    st.header("📦 Beam Stock")
    
    qualities_stock = ["All"] + list(df_stock['Quality'].unique())
    selected_qual_stock = st.selectbox("Filter Stock by Quality:", qualities_stock, key="stock_qual")
    
    filtered_stock = df_stock if selected_qual_stock == "All" else df_stock[df_stock['Quality'] == selected_qual_stock]

    st.dataframe(filtered_stock, use_container_width=True, hide_index=True)

    # Footnotes for Stock
    num_beams = len(filtered_stock)
    total_stock_meters = filtered_stock['Meters'].sum()
    st.markdown(f'<p class="footnote">Number of Beams: {num_beams} | Beam Meter: {int(total_stock_meters)} Meters</p>', unsafe_allow_name_html=True)

    st.divider()

    # --- 4. EFFICIENCY TREND ---
    st.header("📈 Efficiency Trend")
    
    # Prepare data for trend (Latest 15 days)
    trend_df = df_takeaway.drop_duplicates('DATE').sort_values('DATE_OBJ')
    
    fig = px.line(
        trend_df, 
        x='DATE', 
        y='TRUE EFFICIENCY (TOTAL) (TOTAL)', 
        markers=True,
        title="Overall Daily Efficiency Performance"
    )
    
    # Fix the X-Axis to show as Daily Categories instead of a Year Scale
    fig.update_xaxes(type='category', title="Date")
    fig.update_yaxes(tickformat=".1%", title="Total Efficiency")
    
    st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Dashboard Update required. Error Detail: {e}")