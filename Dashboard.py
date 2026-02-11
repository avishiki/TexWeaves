import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Production Dashboard", layout="wide")

st.title("🏭 Waterjet Production Dashboard")

# Load the file
file_path = 'Waterjet Efficiency Shiftwise.xlsx'

try:
    # 1. KEY TAKEAWAYS
    st.header("📊 Key Takeaways")
    df_takeaway = pd.read_excel(file_path, sheet_name='Key Takeaways')
    
    # Simple Filter for Date
    dates = df_takeaway['DATE'].unique()
    selected_date = st.selectbox("Select Date to View:", dates)
    
    filtered_df = df_takeaway[df_takeaway['DATE'] == selected_date]
    st.dataframe(filtered_df, use_container_width=True)

    # 2. BEAM STATUS
    st.header("🧶 Active Beam Status")
    df_beam = pd.read_excel(file_path, sheet_name='Beam Status')
    
    # Color Pending Meters (Red if low)
    def color_pending(val):
        color = 'red' if val < 1000 else 'black'
        return f'color: {color}'

    st.dataframe(df_beam.style.applymap(color_pending, subset=['Pending Meters']), use_container_width=True)

    # 3. CHARTS (Level Up!)
    st.header("📈 Efficiency Trend")
    # Show a chart of Total Efficiency over time
    fig = px.line(df_takeaway, x='DATE', y='TRUE EFFICIENCY (TOTAL) (TOTAL)', title="Daily Total Efficiency")
    st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Waiting for data... Ensure the main script has run. Error: {e}")