import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
import pandas as pd

# --------------------------------------------------
# Initialize Snowflake session (Snowpark connection)
# --------------------------------------------------
session = get_active_session()

# --------------------------------------------------
# Helper function to load a Snowflake view into Pandas
# - Reads the view
# - Converts to Pandas DataFrame
# - Normalizes column names to lowercase
# --------------------------------------------------
def load_view(view_name):
    df = session.table(view_name).to_pandas()
    df.columns = [col.lower() for col in df.columns]
    return df

# --------------------------------------------------
# Page Title
# --------------------------------------------------
st.title("Hospital Dashboard")

# ==================================================
# PIE CHART — Doctor Distribution
# ==================================================

st.subheader("Doctor Distribution by Specialization")

# Load aggregated view from Snowflake
df_doctor_count_by_specialization = load_view(
    "analytics.v_doctor_count_by_specialization"
)

# Create pie chart using Altair
pie = (
    alt.Chart(df_doctor_count_by_specialization)
    .mark_arc()  # Pie chart
    .encode(
        theta="doctor_count:Q",            # Quantitative value
        color="specialization:N",          # Category
        tooltip=["specialization", "doctor_count"]  # Hover info
    )
)

# Display chart
st.altair_chart(pie, use_container_width=True)

# ==================================================
# BAR CHART — Average Treatment Cost
# ==================================================

st.subheader("Average Treatment Cost by Type")

# Load aggregated cost view
df_avg_treatment_cost_by_type = load_view(
    "v_avg_treatment_cost_by_type"
)

# Create bar chart
bar_chart = (
    alt.Chart(df_avg_treatment_cost_by_type)
    .mark_bar()
    .encode(
        x=alt.X("treatment_type:N", title="Treatment Type"),
        y=alt.Y("avg_treatment_cost:Q", title="Average Cost"),
        tooltip=["treatment_type", "avg_treatment_cost"]
    )
)

# Display chart
st.altair_chart(bar_chart, use_container_width=True)

# ==================================================
# INTERACTIVE TABLE — Patient Treatment Details
# ==================================================

# Load detailed transactional view
df_patient_treatment_details = load_view(
    "analytics.v_patient_treatment_details"
)

# Ensure appointment_date is datetime for proper sorting
df_patient_treatment_details["appointment_date"] = pd.to_datetime(
    df_patient_treatment_details["appointment_date"]
)

# --------------------------------------------------
# Layout: Search + Sorting on same row
# --------------------------------------------------
col1, col2 = st.columns([3, 1])  # 3:1 width ratio

with col1:
    # Text search (partial, case-insensitive match)
    search_patient = st.text_input("Search Patient Name")

with col2:
    # Sorting control
    sort_order = st.selectbox(
        "Sort by Date",
        ["Newest First", "Oldest First"]
    )

# --------------------------------------------------
# Filtering Logic
# - If user types something → filter
# - Otherwise → show full dataset
# --------------------------------------------------
if search_patient:
    filtered_df = df_patient_treatment_details[
        df_patient_treatment_details["patient_name"]
        .str.contains(search_patient, case=False, na=False)
    ]
else:
    filtered_df = df_patient_treatment_details

# --------------------------------------------------
# Sorting Logic
# ascending=False → newest dates first
# ascending=True  → oldest dates first
# --------------------------------------------------
if sort_order == "Newest First":
    filtered_df = filtered_df.sort_values(
        "appointment_date", ascending=False
    )
else:
    filtered_df = filtered_df.sort_values(
        "appointment_date", ascending=True
    )

# --------------------------------------------------
# Display final interactive table
# hide_index=True removes Pandas index column
# --------------------------------------------------
st.dataframe(
    filtered_df,
    use_container_width=True,
    hide_index=True
)
