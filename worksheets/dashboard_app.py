import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
import pandas as pd
import math

# Initialize Snowflake session
session = get_active_session()

st.title("Hospital Dashboard")

def load_view(view_name):
    df = session.table(view_name).to_pandas()
    df.columns = [col.lower() for col in df.columns]
    return df
