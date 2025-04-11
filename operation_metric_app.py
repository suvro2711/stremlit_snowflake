# Import python packages
from typing import Literal
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


def get_metric_data():
    query = f"SELECT * FROM STAGING_DEV.OPERATION_METRICS.OPERATION_METRIC_DETAILS"
    result = session.sql(query).collect()
    return result
    
#setups
current_table_data = session.create_dataframe(get_metric_data())

if 'open_to_edit_df' not in st.session_state:
    st.session_state.open_to_edit_df = current_table_data.to_pandas()
    
if 'edited_data' not in st.session_state:
    st.session_state.edited_data = current_table_data.to_pandas()
    
if 'filtered_df' not in st.session_state:
    st.session_state.filtered_df = current_table_data.to_pandas()

if 'is_filtered' not in st.session_state:
    st.session_state.is_filtered = False
    
def on_submit():
    dataset = session.table("STAGING_DEV.OPERATION_METRICS.OPERATION_METRIC_DETAILS")
    updated_dataset = session.create_dataframe(st.session_state.open_to_edit_df)
    updated_dataframe = st.session_state.open_to_edit_df
    current_dataframe = current_table_data.to_pandas()
    # submit_data(current_dataframe, updated_dataframe)
    submit_data(current_dataframe, updated_dataframe)
    
def get_column_config():
    return {
        'DELETE':st.column_config.TextColumn(label="DELETE"),
        'METRIC_NAME': st.column_config.TextColumn(
            label="METRIC NAME",
            # options=[
            #     "Global Productivity",
            #     "Earned Hours",
            #     "Paid Hours",
            # ],
            # default="Global Productivity",
            # allow_other_values=True,
        ),
        'METRIC_TYPE': st.column_config.TextColumn(label="METRIC TYPE"),
        'LOCATION':st.column_config.TextColumn(label="LOCATION"),
        'TIME_PERIOD_TYPE': st.column_config.TextColumn(label="TIME PERIOD TYPE"),
        'TIME_PERIOD_VALUE': st.column_config.TextColumn(label="TIME PERIOD VALUE"),
        'FUTURE_1': st.column_config.TextColumn(label="FUTURE-1"),
        'FUTURE_2': st.column_config.TextColumn(label="FUTURE-2"),
        'METRIC_VALUE': st.column_config.TextColumn(label="METRIC VALUE"),
        'METRIC_UOM': st.column_config.TextColumn(label="METRIC UOM"),
        'ENABLED_FLAG': st.column_config.TextColumn(label="ENABLED FLAG"),
        'COMMENTS': st.column_config.TextColumn(label="COMMENTS"),
        'LAST_UPDATE_DATE': st.column_config.DatetimeColumn(label="LAST UPDATE DATE", disabled=True),
        'LAST_UPDATED_BY': st.column_config.TextColumn(label="LAST UPDATED BY", disabled=True),
    }


# def find_inserted_rows(dataset, updated_dataset):
#     # Ensure both DataFrames have the same columns
#     common_columns = list(set(dataset.columns) & set(updated_dataset.columns))
#     dataset = dataset[common_columns]
#     updated_dataset = updated_dataset[common_columns]
    
#     # Define the condition for matching rows
#     condition = (
#         (dataset["METRIC_NAME"] == updated_dataset["METRIC_NAME"]) &
#         (dataset["METRIC_TYPE"] == updated_dataset["METRIC_TYPE"]) &
#         (dataset["LOCATION"] == updated_dataset["LOCATION"]) &
#         (dataset["TIME_PERIOD_TYPE"] == updated_dataset["TIME_PERIOD_TYPE"]) &
#         (dataset["FUTURE_1"] == updated_dataset["FUTURE_1"]) &
#         (dataset["FUTURE_2"] == updated_dataset["FUTURE_2"]) &
#         (dataset["TIME_PERIOD_VALUE"] == updated_dataset["TIME_PERIOD_VALUE"])
#     )
    
#     # Find rows that do not meet the condition
#     non_matching_rows = dataset[~condition]
    
#     return non_matching_rows


def find_updated_and_inserted_rows(dataset, updated_dataset):
    # Ensure both DataFrames have the same columns
    common_columns = list(set(dataset.columns) & set(updated_dataset.columns))
    dataset = dataset[common_columns]
    updated_dataset = updated_dataset[common_columns]
    
    # Merge the DataFrames to find non-matching rows
    merged_df = updated_dataset.merge(dataset, on=common_columns, how='left', indicator=True)
    
    # Identify rows that are only in updated_dataset
    inserted_rows = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    return inserted_rows

def find_only_updated(dataset, inserted_rows):
    # Define the condition for matching rows
    condition = (
        (inserted_rows["METRIC_NAME"].isin(dataset["METRIC_NAME"])) &
        (inserted_rows["METRIC_TYPE"].isin(dataset["METRIC_TYPE"])) &
        (inserted_rows["LOCATION"].isin(dataset["LOCATION"])) &
        (inserted_rows["TIME_PERIOD_TYPE"].isin(dataset["TIME_PERIOD_TYPE"])) &
        (inserted_rows["FUTURE_1"].isin(dataset["FUTURE_1"])) &
        (inserted_rows["FUTURE_2"].isin(dataset["FUTURE_2"])) &
        (inserted_rows["TIME_PERIOD_VALUE"].isin(dataset["TIME_PERIOD_VALUE"]))
    )
    
    # Find rows that meet the condition
    common_rows = inserted_rows[condition]
    
    return common_rows


def insert_history_table(rows):
    # history_table = session.table("STAGING_DEV.OPERATION_METRICS.OPERATION_METRIC_DETAILS_HISTORY")
    # history_table.insert(rows)
    session.write_pandas(
        df= rows,
        table_name = "OPERATION_METRIC_DETAILS_HISTORY",
        database = "STAGING_DEV",
        schema = "OPERATION_METRICS",
        auto_create_table = False,
        create_temp_table = False,
        overwrite = False
    )

# def insert_into_history_table(type, dataset, rows):
#     # history_df has soll the columns of st.session_state.open_to_edit_df but with additional row called ACTION_TYPE and HISTORY_CREATION_DATE
#     history_df = pd.DataFrame(columns=st.session_state.open_to_edit_df.columns.tolist() + ["ACTION_FLAG", "HISTORY_CREATION_DATE"])
#     if type == "INSERT_AND_UPDATE":
#         #inserting the row to the history_df with new columns ACTION_TYPE and HISTORY_CREATION_DATE
#         rows["ACTION_FLAG"] = type
#         rows["HISTORY_CREATION_DATE"] = datetime.now()
#         only_updated_rows = find_common_rows(dataset, updated_and_inserted_rows)
#         <write code here to mark the only_updated_rows ACTION_FLAG value as u>
#         history_df = pd.concat([history_df, rows], ignore_index=True)
#         insert_history_table(rows)
#     st.write("History DataFrame", history_df)


def insert_into_history_table(type, dataset, rows):
    # history_df has all the columns of st.session_state.open_to_edit_df but with additional columns ACTION_FLAG and HISTORY_CREATION_DATE
    history_df = pd.DataFrame(columns=st.session_state.open_to_edit_df.columns.tolist() + ["ACTION_FLAG", "HISTORY_CREATION_DATE"])
    
    if type == "INSERT_AND_UPDATE":
        # Inserting the row to the history_df with new columns ACTION_FLAG and HISTORY_CREATION_DATE
        rows["ACTION_FLAG"] = "I"
        rows["HISTORY_CREATION_DATE"] = datetime.now()
        
        # Assuming updated_and_inserted_rows is defined elsewhere in your code
        only_updated_rows = find_only_updated(dataset, rows)
        
        # Mark the only_updated_rows ACTION_FLAG value as 'u'
        rows.loc[only_updated_rows.index, "ACTION_FLAG"] = 'U'
        
        # Concatenate rows to history_df
        history_df = pd.concat([history_df, rows], ignore_index=True)
        
        # Insert history_df into the history table (assuming insert_history_table is defined elsewhere)
        insert_history_table(history_df)
    
    st.write("History DataFrame", history_df)

def update_history_table(dataset, updated_dataset):
    updated_and_inserted_rows = find_updated_and_inserted_rows(dataset, updated_dataset)

    if(len(updated_and_inserted_rows)):
        insert_into_history_table(type="INSERT_AND_UPDATE", dataset=dataset, rows=updated_and_inserted_rows)
    # if(len(inserted_rows)):
        # update_history(type="INSERT", rows=inserted_rows)
    # if(len(deleted_rows)):
        # update_history(type="DELETE", rows=dedleted_rows)

    
# merging the changes to original table
def submit_edited_data_to_table(dataset, updated_dataset):
    st.warning("Attempting to update dataset")
    dataset.merge(
        source=updated_dataset,
        join_expr=(
            (dataset["METRIC_NAME"] == updated_dataset["METRIC_NAME"])
            & (dataset["METRIC_TYPE"] == updated_dataset["METRIC_TYPE"])
            & (dataset["LOCATION"] == updated_dataset["LOCATION"])
            & (dataset["TIME_PERIOD_TYPE"] == updated_dataset["TIME_PERIOD_TYPE"])
            & (dataset["FUTURE_1"] == updated_dataset["FUTURE_1"])
            & (dataset["FUTURE_2"] == updated_dataset["FUTURE_2"])
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
    st.write(updated_dataset[col] for col in updated_dataset.columns if col in dataset.columns)

def submit_data(dataset, updated_dataset):
    try:
        # submit_edited_data_to_table(dataset, updated_dataset)
        update_history_table(dataset, updated_dataset)
        # st.experimental_rerun()
    except Exception as e:
        st.error(f"{ERROR_MSG}: {e}")
        st.stop()
        
def add_row_to_df(df_name):
    column_names = st.session_state[df_name].columns.tolist()
    new_row = pd.DataFrame([{col: None for col in column_names}]) 
    st.session_state[df_name]= pd.concat([new_row, st.session_state[df_name]], ignore_index=True)


st.subheader("Operations Metric Definition")
search_col, add_row_col, col3 = st.columns([1,1, 3], gap="small", vertical_alignment="bottom")
    
with search_col:
    search_text = st.text_input("Search:")
    if search_text:
        # Add your search logic here using queried_data
        # first_line = st.session_state.open_to_edit_df.iloc[1]
        # st.write(first_line)
        st.session_state.is_filtered = True
        st.session_state.filtered_df = st.session_state.open_to_edit_df[
            st.session_state.open_to_edit_df.apply(
                lambda row: any(
                    search_text.lower() in str(value).lower() for value in row.astype(str).values
                ),
                axis=1,
            )
        ]
        # st.write("Filtered", st.session_state.is_filtered, search_text)
        
    else:
        st.session_state.is_filtered = False
        # st.write("Not Filtered", st.session_state.is_filtered, search_text)
        
def on_data_change():
    st.session_state.edited_data = st.session_state.open_to_edit_df

# def create_data_editor(df_name, key=None):
#     st.session_state[df_name] = st.data_editor(
#         data=st.session_state[df_name],
#         column_config=get_column_config(),
#         use_container_width=True,
#         hide_index=True,
#         on_change=on_data_change,
#         key=key,
#     )
    
def sync_filtered_edits_with_original_df(change_type:Literal["add", "delete", "update"]):
    if change_type == "add":
        st.session_state.open_to_edit_df = pd.concat([st.session_state.open_to_edit_df, st.session_state.filtered_df], ignore_index=True)
    elif change_type == "update":
        st.session_state.open_to_edit_df.update(st.session_state.filtered_df)
    # elif change_type == "delete":
    #     st.session_state.open_to_edit_df = st.session_state.open_to_edit_df[~st.session_state.open_to_edit_df.isin(st.session_state.filtered_df)].dropna()

with add_row_col:
        st.button("Add Row", type="primary",  icon=":material/add:", on_click=lambda: add_row_to_df("open_to_edit_df"), disabled=st.session_state.is_filtered)

# with st.form("Edit Pending Orders"):
if st.session_state.is_filtered: 
    st.session_state.edited_filter_df = st.data_editor(
        data= st.session_state.filtered_df,
        column_config=get_column_config(),
        use_container_width=True,
        hide_index=True,
        # on_change=on_data_change,
        key="filtered_data_change"
    )
else: 
    edited_df = st.data_editor(
        data= st.session_state.open_to_edit_df,
        column_config=get_column_config(),
        use_container_width=True,
        # hide_index=True,
        # add_row_col="test"
        # on_change=on_data_change,
    
)
st.button("Submit Changes", on_click=on_submit, disabled=st.session_state.is_filtered)

    



