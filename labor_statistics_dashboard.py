# Dependencies

from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import pandas as pd
import os
import streamlit as st
import numpy as np
import plotly.express as px


FOLDER_NAME = "labor_statistics_api_data"
API_REGISTRATION_KEY = "93ee8e3a1d66428298ca51eceb0fca71"
API_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'


class LaborStatisticsDataPull:
    api_desc_list = [
        {
            'name': "Civilian Labor Force (Seasonally Adjusted)",
            'code': "LNS11000000"
        },
        {
            'name': "Output Per Hour - Non-farm Business Productivity",
            'code': "PRS85006092"
        },
        {
            "name": "Total Nonfarm Employment - Seasonally Adjusted",
            "code": "CES0000000001"
        },
        {
            "name": "Civilian Employment (Seasonally Adjusted)",
            "code": "LNS12000000"
        },
        {
            "name": "Total Private Average Hourly Earnings of Prod. and Nonsup. Employees - Seasonally Adjusted",
            'code': "CES0500000008"
        },
        {
            "name": "Nonfarm Business Unit Labor Costs",
            'code': "PRS85006112"
        },
    ]

    def __init__(self, name):
        self.name = name
        self.series_ids = [x['code'] for x in LaborStatisticsDataPull.api_desc_list]

    def pullDataFull(self):
        """
        Fetches all data for the API in the last year.
        """
        if not os.path.exists(FOLDER_NAME):
            os.makedirs(FOLDER_NAME)

        this_year = datetime.now().year
        prev_year = (datetime.now() - relativedelta(months=12)).year

        headers = {'Content-type': 'application/json'}
        data = json.dumps({
            "seriesid": self.series_ids,
            "startyear": str(prev_year),
            "endyear": str(this_year),
            "registrationkey": API_REGISTRATION_KEY
        })

        response = requests.post(API_URL, data=data, headers=headers)
        json_data = response.json()

        seried_tables = {}

        for _id in self.series_ids:
            data_obj = next((x for x in json_data['Results']['series'] if x['seriesID'] == _id), None)
            if data_obj:
                df_table = pd.DataFrame(data_obj['data'])
                seried_tables[_id] = df_table
                df_table.to_csv(os.path.join(FOLDER_NAME, f'{_id}.csv'), index=False)
            else:
                st.warning(f"No data returned for series ID: {_id}")

        return {'status': "Success"}

    def pullLatestData(self):
        """
        Fetches the latest data for the API.
        """
        headers = {'Content-type': 'application/json'}
        data = json.dumps({
            "seriesid": self.series_ids,
            "latest": True,
            "registrationkey": API_REGISTRATION_KEY
        })
        response = requests.post(API_URL, data=data, headers=headers)
        json_data = response.json()

        for _id in self.series_ids:
            data_obj = next((x for x in json_data['Results']['series'] if x['seriesID'] == _id), None)
            if data_obj:
                latest_data = pd.DataFrame(data_obj['data'])
                orig_table_path = os.path.join(FOLDER_NAME, f'{_id}.csv')

                if os.path.exists(orig_table_path):
                    orig_table = pd.read_csv(orig_table_path)
                    combined_df = pd.concat([orig_table, latest_data], ignore_index=True)
                    combined_df['year'] = combined_df['year'].astype(str)

                    unique_df = combined_df.drop_duplicates(subset=['year', 'periodName'])

                    unique_df.to_csv(orig_table_path, index=False)
                    st.success(f'Updated data for series ID: {_id}')
                else:
                    latest_data.to_csv(orig_table_path, index=False)
                    st.info(f'Created new data file for series ID: {_id}')
            else:
                st.warning(f"No latest data returned for series ID: {_id}")

        return {"status": "Incremental Successful"}


api_data_pull = LaborStatisticsDataPull(name='laborstats')

if not os.path.exists(FOLDER_NAME):
    with st.spinner('Fetching initial data from BLS API...'):
        api_data_pull.pullDataFull()
    st.success('Initial data fetched successfully.')


def AssignYM(df):
    """
    Assigns a 'yearMonth' and 'date' column to the DataFrame based on 'year' and 'periodName'.
    """
   
    month_map = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12',
        "1st Quarter": '03', '2nd Quarter': "06", "3rd Quarter": "09", "4th Quarter": "12"
    }

   
    df['yearMonth'] = df['year'].astype(str) + '-' + df['periodName'].map(month_map)

   
    df['date'] = pd.to_datetime(df['yearMonth'], errors='coerce')

    df = df.dropna(subset=['date'])

    return df

def load_data(series_code):
    """
    Loads and processes data for a given series code.
    """
    df_path = os.path.join(FOLDER_NAME, f'{series_code}.csv')
    if os.path.exists(df_path):
        df = pd.read_csv(df_path)
        df = AssignYM(df)
        return df
    else:
        st.error(f"Data file for series ID {series_code} not found.")
        return pd.DataFrame()

def filter_df(df, start_date, end_date):
    """
    Filters the DataFrame based on the selected date range.
    """
    if df.empty:
        return df
    return df.loc[(df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))]


st.set_page_config(page_title="Labor Statistics - Dashboard", layout="wide")
st.title("U.S. Bureau of Labor Statistics - Dashboard")


st.sidebar.header('Controls')


if st.sidebar.button('Pull Latest Data'):
    with st.spinner('Fetching latest data from BLS API...'):
        api_data_pull.pullLatestData()
    st.success('Latest data fetched successfully.')
    st.experimental_rerun()  


civ_labor_force = load_data('LNS11000000')
non_farm_prod_df = load_data('PRS85006092')
non_farm_employment_df = load_data('CES0000000001')
civ_employment_df = load_data('LNS12000000')
hourly_earnings_prod_emp_df = load_data('CES0500000008')
non_farm_bu_cost_df = load_data('PRS85006112')


latest_dates = [
    civ_labor_force['date'].max() if not civ_labor_force.empty else pd.NaT,
    non_farm_prod_df['date'].max() if not non_farm_prod_df.empty else pd.NaT,
    non_farm_employment_df['date'].max() if not non_farm_employment_df.empty else pd.NaT,
    civ_employment_df['date'].max() if not civ_employment_df.empty else pd.NaT,
    hourly_earnings_prod_emp_df['date'].max() if not hourly_earnings_prod_emp_df.empty else pd.NaT,
    non_farm_bu_cost_df['date'].max() if not non_farm_bu_cost_df.empty else pd.NaT
]


latest_date = pd.to_datetime('1900-01-01')
for date in latest_dates:
    if pd.notna(date) and date > latest_date:
        latest_date = date

if latest_date == pd.to_datetime('1900-01-01'):
    latest_date = datetime.now()


start_date = st.sidebar.date_input(
    "Start Date",
    value=(latest_date - relativedelta(months=15)).date(),
    min_value=datetime(2000, 1, 1).date(),
    max_value=latest_date.date()
)

end_date = st.sidebar.date_input(
    "End Date",
    value=latest_date.date(),
    min_value=(latest_date - relativedelta(months=15)).date(),
    max_value=latest_date.date()
)


if start_date > end_date:
    st.sidebar.error("Error: Start Date must be before End Date.")


civ_labor_force_filtered = filter_df(civ_labor_force, start_date, end_date).reset_index(drop=True)
non_farm_prod_filtered_df = filter_df(non_farm_prod_df, start_date, end_date)
non_farm_employment_filtered_df = filter_df(non_farm_employment_df, start_date, end_date)
civ_employment_filtered_df = filter_df(civ_employment_df, start_date, end_date)
hourly_earnings_prod_emp_filtered_df = filter_df(hourly_earnings_prod_emp_df, start_date, end_date)
non_farm_bu_cost_filtered_df = filter_df(non_farm_bu_cost_df, start_date, end_date)


st.subheader('Number Of Civilian Employees Over A Period')
if not civ_labor_force_filtered.empty:
    civ_labor_force_filtered_sorted = civ_labor_force_filtered.sort_values(by='date')
    st.area_chart(
        civ_labor_force_filtered_sorted[['yearMonth', 'value']],
        x="yearMonth",
        y="value",
        use_container_width=True
    )
else:
    st.warning("No Civilian Labor Force data available for the selected date range.")


st.subheader('Quarterwise Non-Farm Productivity')
if not non_farm_prod_filtered_df.empty:
    fig = px.pie(
        non_farm_prod_filtered_df,
        names='periodName',
        color_discrete_sequence=['#DAF7A6', '#900C3F', '#1ABC9C', '#7F8C8D'],
        values='value',
        title='Quarterwise Non-Farm Productivity'
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No Non-Farm Productivity data available for the selected date range.")


st.subheader('Total Nonfarm Employment - Seasonally Adjusted')
if not non_farm_employment_filtered_df.empty:
    non_farm_employment_sorted = non_farm_employment_filtered_df.sort_values(by='date')
    st.bar_chart(
        non_farm_employment_sorted[['yearMonth', 'value']],
        x="yearMonth",
        y="value",
        use_container_width=True
    )
else:
    st.warning("No Nonfarm Employment data available for the selected date range.")

st.subheader('Civilian Employment - Seasonally Adjusted')
if not civ_employment_filtered_df.empty:
    civ_employment_sorted = civ_employment_filtered_df.sort_values(by='date')
    st.bar_chart(
        civ_employment_sorted[['yearMonth', 'value']],
        x="yearMonth",
        y="value",
        use_container_width=True
    )
else:
    st.warning("No Civilian Employment data available for the selected date range.")


st.subheader('Total Private Average Hourly Earnings - Seasonally Adjusted')
if not hourly_earnings_prod_emp_filtered_df.empty:
    hourly_earnings_sorted = hourly_earnings_prod_emp_filtered_df.sort_values(by='date')
    st.bar_chart(
        hourly_earnings_sorted[['yearMonth', 'value']],
        x="yearMonth",
        y="value",
        use_container_width=True
    )
else:
    st.warning("No Hourly Earnings data available for the selected date range.")


st.subheader('Non-Farm Business Unit Cost')
if not non_farm_bu_cost_filtered_df.empty:
    fig = px.pie(
        non_farm_bu_cost_filtered_df,
        names='periodName',
        color_discrete_sequence=['#3357FF', '#16A085', '#D35400', '#BDC3C7'],
        values='value',
        title='Non-Farm Business Unit Cost'
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No Non-Farm Business Unit Cost data available for the selected date range.")



st.markdown("### Raw Data Pulled From API")

def display_raw_data(df, title):
    if not df.empty:
        st.subheader(title)
        st.dataframe(df, use_container_width=True)
    else:
        st.warning(f"No data available for {title}.")


display_raw_data(civ_labor_force_filtered[['yearMonth', 'latest', 'value']], 
                'Civilian Labor Force (Seasonally Adjusted)')

display_raw_data(non_farm_prod_filtered_df[["year", "period", "periodName", "latest", "value", "footnotes"]],
                'Non-farm Business Productivity')

display_raw_data(non_farm_employment_filtered_df[["year", "period", "periodName", "latest", "value", "footnotes"]],
                'Non-farm Employment')

display_raw_data(civ_employment_filtered_df[["year", "period", "periodName", "latest", "value", "footnotes"]],
                'Civilian Employment')

display_raw_data(hourly_earnings_prod_emp_filtered_df[["year", "period", "periodName", "latest", "value", "footnotes"]],
                'Total Private Average Hourly Earnings')

display_raw_data(non_farm_bu_cost_filtered_df[["year", "period", "periodName", "latest", "value", "footnotes"]],
                'Non-Farm Business Unit Cost')

st.markdown("""
---
*Data Source: [U.S. Bureau of Labor Statistics](https://www.bls.gov/data/)*
""")
