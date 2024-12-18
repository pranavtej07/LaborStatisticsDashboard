# dependencies

from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import pandas as pd
import os
import streamlit as st
import numpy as np
import plotly.express as px

# configs 

folder_name = "labor_statistics_api_data"


# main object

class LaborStatisticsDataPull():

    api_desc_list = [
        {
            'name': "Unemployment Rate (Seasonally Adjusted)",
            'code': "LNS14000000"
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
        'fetches all data for api in last year'

        # create a folder if it does not exist
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        # finding timestamp for last year 
        this_year = datetime.now().year
        prev_year = (datetime.now() - relativedelta(months=12)).year

        # API call        
        headers = {'Content-type': 'application/json'}
        data = json.dumps({"seriesid": self.series_ids, "startyear": str(prev_year), "endyear": this_year,
                           "registrationkey": "93ee8e3a1d66428298ca51eceb0fca71"})
        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

        # creating dataframe
        json_data = json.loads(p.text)
        seried_tables = {}

        for _id in self.series_ids:
            data_obj = [x for x in json_data['Results']['series'] if x['seriesID'] == _id]
            df_table = pd.DataFrame.from_dict(data_obj[0]['data'], orient='columns')
            seried_tables[_id] = df_table
            df_table.to_csv(folder_name + '/' + _id + '.csv', index=False)

        return {'status': "Success"}

    def pullLatestData(self):
        # API call        
        headers = {'Content-type': 'application/json'}
        data = json.dumps({"seriesid": self.series_ids, 'latest': True,
                           "registrationkey": "93ee8e3a1d66428298ca51eceb0fca71"})
        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', 
                          data=data, headers=headers)

        json_data = json.loads(p.text)

        for _id in self.series_ids:
            data_obj = [x for x in json_data['Results']['series'] if x['seriesID'] == _id]
            orig_table = pd.read_csv(folder_name + '/' + _id + '.csv')
            df_table = pd.DataFrame.from_dict(data_obj[0]['data'], orient='columns')

            # Append new data to the existing data
            combined_df = pd.concat([orig_table, df_table], ignore_index=True)
            combined_df.loc[:, 'year'] = combined_df['year'].astype(str)

            # Remove duplicates
            unique_df = combined_df.drop_duplicates(subset=['year', 'periodName'])
            unique_df.to_csv(folder_name + '/' + _id + '.csv', index=False)

            print('Updating the data...')

        return {"status": "Incremental Successful"}


api_data_pull = LaborStatisticsDataPull(name='laborstats')

if os.path.exists(folder_name) == False:
    api_data_pull.pullDataFull()

# Dashboard Layer

def AssignYM(df):
    # Mapping of month names to month numbers
    month_map = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
        'July': '07', 'August': '08', 'September': '09', 'October': '10', 
        'November': '11', 'December': '12',
        "1st Quarter": '03', "2nd Quarter": "06", "3rd Quarter": "09", "4th Quarter": "12"
    }

    df['yearMonth'] = df['year'].astype(str) + '-' + df['periodName'].map(month_map)
    df['date'] = pd.to_datetime(df['yearMonth'])
    return df

# Set the title of the dashboard
st.set_page_config(page_title="Labor Statistics - Dashboard", layout="wide")
st.title("U.S Bureau Of Labor Statistics - Dashboard")

# Sidebar for date range filter
st.sidebar.header('Filter By Date Range')
start_date = st.sidebar.date_input("Start Date", 
                                   value=datetime.now() - relativedelta(months=15), 
                                   min_value=datetime.now() - relativedelta(months=15), 
                                   max_value=datetime.now())
end_date = st.sidebar.date_input("End Date", 
                                 value=datetime.now(), 
                                 min_value=datetime.now() - relativedelta(months=15), 
                                 max_value=datetime.now())

# Chart 1: Unemployment Rate
unemployment_rate_df = pd.read_csv(folder_name + '/' + 'LNS14000000.csv')
unemployment_rate_df = AssignYM(unemployment_rate_df)
unemployment_rate_filtered_df = unemployment_rate_df.loc[(unemployment_rate_df['date'] >= pd.Timestamp(start_date)) & 
                                                          (unemployment_rate_df['date'] <= pd.Timestamp(end_date))]
unemployment_rate_filtered_df.reset_index(inplace=True, drop=True)

st.subheader('Unemployment Rate Over Time')
st.line_chart(unemployment_rate_filtered_df[['yearMonth', 'value']], 
              x='yearMonth', y='value', use_container_width=True)

# Chart 2: Output Per Hour - Non-farm Business Productivity
non_farm_prod_df = pd.read_csv(folder_name + '/' + 'PRS85006092.csv')
non_farm_prod_df = AssignYM(non_farm_prod_df)
non_farm_prod_filtered_df = non_farm_prod_df.loc[(non_farm_prod_df['date'] >= pd.Timestamp(start_date)) & 
                                                 (non_farm_prod_df['date'] <= pd.Timestamp(end_date))]

st.subheader('Non-farm Business Productivity')
st.area_chart(non_farm_prod_filtered_df[['yearMonth', 'value']], 
              x='yearMonth', y='value', use_container_width=True)

# Chart 3: Total Nonfarm Employment
non_farm_employment_df = pd.read_csv(folder_name + '/' + 'CES0000000001.csv')
non_farm_employment_df = AssignYM(non_farm_employment_df)
non_farm_employment_filtered_df = non_farm_employment_df.loc[(non_farm_employment_df['date'] >= pd.Timestamp(start_date)) & 
                                                             (non_farm_employment_df['date'] <= pd.Timestamp(end_date))]

st.subheader('Total Nonfarm Employment')
st.bar_chart(non_farm_employment_filtered_df[['yearMonth', 'value']], 
             x='yearMonth', y='value', use_container_width=True)

# Chart 4: Civilian Employment
civ_employment_df = pd.read_csv(folder_name + '/' + 'LNS12000000.csv')
civ_employment_df = AssignYM(civ_employment_df)
civ_employment_filtered_df = civ_employment_df.loc[(civ_employment_df['date'] >= pd.Timestamp(start_date)) & 
                                                   (civ_employment_df['date'] <= pd.Timestamp(end_date))]

st.subheader('Civilian Employment - Seasonally Adjusted')
st.bar_chart(civ_employment_filtered_df[['yearMonth', 'value']], 
             x='yearMonth', y='value', use_container_width=True)

# Chart 5: Total Private Average Hourly Earnings
hourly_earnings_prod_emp_df = pd.read_csv(folder_name + '/' + 'CES0500000008.csv')
hourly_earnings_prod_emp_df = AssignYM(hourly_earnings_prod_emp_df)
hourly_earnings_prod_emp_filtered_df = hourly_earnings_prod_emp_df.loc[(hourly_earnings_prod_emp_df['date'] >= pd.Timestamp(start_date)) & 
                                                                       (hourly_earnings_prod_emp_df['date'] <= pd.Timestamp(end_date))]

st.subheader('Total Private Average Hourly Earnings')
st.line_chart(hourly_earnings_prod_emp_filtered_df[['yearMonth', 'value']], 
              x='yearMonth', y='value', use_container_width=True)

# Chart 6: Nonfarm Business Unit Labor Costs
non_farm_bu_cost_df = pd.read_csv(folder_name + '/' + 'PRS85006112.csv')
non_farm_bu_cost_df = AssignYM(non_farm_bu_cost_df)
non_farm_bu_cost_filtered_df = non_farm_bu_cost_df.loc[(non_farm_bu_cost_df['date'] >= pd.Timestamp(start_date)) & 
                                                       (non_farm_bu_cost_df['date'] <= pd.Timestamp(end_date))]

st.subheader('Nonfarm Business Unit Labor Costs')
st.area_chart(non_farm_bu_cost_filtered_df[['yearMonth', 'value']], 
              x='yearMonth', y='value', use_container_width=True)

# Data Tables
st.subheader('Raw Data - Unemployment Rate')
st.dataframe(unemployment_rate_filtered_df[['yearMonth', 'value']], use_container_width=True)

st.subheader('Raw Data - Non-farm Productivity')
st.dataframe(non_farm_prod_filtered_df[['yearMonth', 'value']], use_container_width=True)

st.subheader('Raw Data - Nonfarm Employment')
st.dataframe(non_farm_employment_filtered_df[['yearMonth', 'value']], use_container_width=True)

st.subheader('Raw Data - Civilian Employment')
st.dataframe(civ_employment_filtered_df[['yearMonth', 'value']], use_container_width=True)

st.subheader('Raw Data - Private Hourly Earnings')
st.dataframe(hourly_earnings_prod_emp_filtered_df[['yearMonth', 'value']], use_container_width=True)

st.subheader('Raw Data - Nonfarm Labor Costs')
st.dataframe(non_farm_bu_cost_filtered_df[['yearMonth', 'value']], use_container_width=True)
