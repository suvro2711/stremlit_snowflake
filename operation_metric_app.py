# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import when_matched, when_not_matched
import pandas as pd
from datetime import datetime
import time

# Get the current credentials
session = get_active_session()
st.set_page_config(layout = 'wide')
# captions
SUCCESS_MSG = "Records updated successfully!"
ERROR_MSG = "Error updating records. Please try again."
is_serached = False

def get_metric_data():
    query = f"SELECT * FROM STAGING_DEV.OPERATION_METRICS.OPERATION_METRIC_DETAILS"
    result = session.sql(query).collect()
    return result

def get_column_config():
    return {
         'METRIC_NAME': st.column_config.SelectboxColumn(
            label="METRIC NAME",
            options=[
                "Global Productivity",
                "Earned Hours",
                "Paid Hours",
            ],
            default="Global Productivity",
            # allow_other_values=True,
        ),
        'METRIC_TYPE': st.column_config.TextColumn(label="METRIC TYPE"),
        'LOCATION':st.column_config.TextColumn(label="LOCATION"),
        'TIME_PERIOD_TYPE': st.column_config.TextColumn(label="TIME PERIOD TYPE"),
        'TIME_PERIOD_VALUE': st.column_config.TextColumn(label="TIME PERIOD VALUE"),
        'FUTURE_1': st.column_config.TextColumn(label="FUTURE-1"),
        'FUTURE_2': st.column_config.TextColumn(label="FUTURE-2"),
        'METRIC_VALUE': st.column_config.TextColumn(label="METRIC VALUE"),
        'ENABLED_FLAG': st.column_config.TextColumn(label="ENABLED FLAG"),
        'COMMENTS': st.column_config.TextColumn(label="COMMENTS"),
        'METRIC_UOM': st.column_config.TextColumn(label="METRIC UOM"),
    }


# merging the changes to original table
def merge_data(dataset, updated_dataset):
    try:
        st.warning("Attempting to update dataset")
        dataset.merge(
            source=updated_dataset,
            join_expr=(
                (dataset["METRIC_NAME"] == updated_dataset["METRIC_NAME"])
                & (dataset["METRIC_TYPE"] == updated_dataset["METRIC_TYPE"])
                & (dataset["LOCATION"] == updated_dataset["LOCATION"])
                & (dataset["TIME_PERIOD_TYPE"] == updated_dataset["TIME_PERIOD_TYPE"])
                & (dataset["TIME_PERIOD_VALUE"] == updated_dataset["TIME_PERIOD_VALUE"])
            ),
            clauses=[
                when_matched().update({
                    col: updated_dataset[col] for col in updated_dataset.columns
                    if col in dataset.columns
                }),
                when_not_matched().insert({
                    col: updated_dataset[col] for col in updated_dataset.columns
                    if col in dataset.columns
                })
            ]
        )
        st.success(SUCCESS_MSG)
        time.sleep(.5)
        # st.experimental_rerun()
        
    except Exception as e:
        st.error(f"{ERROR_MSG}: {e}")
        st.stop()
        
current_table_data = session.create_dataframe(get_metric_data())

# Execute the query and convert it into a Pandas dataframe
queried_data = current_table_data.to_pandas()

st.subheader("Operations Metric Details")
search_col, add_row_col, col3 = st.columns([1,1, 3], gap="small", vertical_alignment="bottom")

if add_row_col.button("Add Row", type="primary",  icon=":material/add:"):
    # Adding a new row
    column_names = queried_data.columns.tolist()
    new_row = pd.DataFrame([{col: None for col in column_names}]) 
    queried_data = pd.concat([new_row, queried_data], ignore_index=True)

    
with search_col:
    search_text = st.text_input("Search:")
    if search_text:
        # Add your search logic here using queried_data
        is_serached = True
        queried_data = queried_data[
            queried_data.apply(
                lambda row: any(
                    search_text.lower() in str(value).lower() for value in row.astype(str).values
                ),
                axis=1,
            )
        ]
    else:
        is_serached = False
        queried_data = current_table_data.to_pandas() # Display the full dataframe if no search


with st.form("Operations Metric Details"):
    edited_df = st.data_editor(
        data=queried_data,
        column_config=get_column_config(),
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic"
    )
    submitted = st.form_submit_button("Submit Changes")


if submitted:
    updated_dataset = session.create_dataframe(edited_df)
    dataset = session.table("STAGING_DEV.OPERATION_METRICS.OPERATION_METRIC_DETAILS")
    merge_data(dataset, updated_dataset)