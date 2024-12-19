# dependecies

from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import pandas as pd
import os
import numpy as np
import plotly.express as px

try:
    import streamlit as st
except ModuleNotFoundError:
    print("streamlit is not installed in the environment. Please install it using 'pip install streamlit' and try again.")
    st = None

# configs

folder_name = "labor_statistics_api_data"

# main object

class LaborStatisticsDataPull():

    api_desc_list = [
        {
            'name':"Civilian Labor Force (Seasonally Adjusted)",
            'code':"LNS11000000"
        },
        {
            'name':"Output Per Hour - Non-farm Business Productivity",
            'code':"PRS85006092"
        },
        {
            "name":"Total Nonfarm Employment - Seasonally Adjusted",
            "code":"CES0000000001"
        },
        {
            "name":"Civilian Employment (Seasonally Adjusted)",
            "code": "LNS12000000"
        },
        {
            "name":"Total Private Average Hourly Earnings of Prod. and Nonsup. Employees - Seasonally Adjusted",
            'code': "CES0500000008"
        },
        {
            "name":"Nonfarm Business Unit Labor Costs",
            'code': "PRS85006112"
        },
        {
            "name":"Unemployment Rate",
            'code': "LNS14000000"
        },
    ]

    def __init__(self, name):

        self.name = name  

        self.series_ids = [x['code'] for x in LaborStatisticsDataPull.api_desc_list]

    def pullDataFull(self):

        'fetches all data for api in last year'

        # create a folder if it doesnot exist
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        # finding timestamp for last year

        this_year = datetime.now().year

        prev_year = (datetime.now() - relativedelta(months=12)).year

        # api call       
       
        headers = {'Content-type': 'application/json'}
        data = json.dumps({"seriesid": self.series_ids,"startyear":str(prev_year),"endyear":this_year,
        "registrationkey":"93ee8e3a1d66428298ca51eceb0fca71"})
        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)

        # creating dataframe

        json_data = json.loads(p.text)

        seried_tables={}

        for _id in self.series_ids:

            data_obj = [x for x in json_data['Results']['series'] if x['seriesID'] ==_id]

            df_table = pd.DataFrame.from_dict(data_obj[0]['data'],orient='columns')

            seried_tables[_id]=df_table

            df_table.to_csv(folder_name+'/'+_id+'.csv',index=False)
   
        return {'status':"Success"}
   
    def pullLatestData(self):

        # api call       
        headers = {'Content-type': 'application/json'}
        data = json.dumps({"seriesid": self.series_ids,'latest':True,
        "registrationkey":"93ee8e3a1d66428298ca51eceb0fca71"})
        p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/',
                    data=data, headers=headers)

        json_data = json.loads(p.text)

        for _id in self.series_ids:

            data_obj = [x for x in json_data['Results']['series'] if x['seriesID'] ==_id]

            orig_table = pd.read_csv(folder_name+'/'+_id+'.csv')

            df_table = pd.DataFrame.from_dict(data_obj[0]['data'],orient='columns')

            # Append df2 to df1
            combined_df = pd.concat([orig_table, df_table], ignore_index=True)

            combined_df.loc[:,'year']=combined_df['year'].astype(str)

            # Remove duplicates based on 'col1' and 'col2'
            unique_df = combined_df.drop_duplicates(subset=['year','periodName'])

            unique_df.to_csv(folder_name+'/'+_id+'.csv',index=False)

            print('updating the data ....')

        return {"status":"Incremental Successful"}

api_data_pull = LaborStatisticsDataPull(name='laborstats')

if os.path.exists(folder_name) == False:

    api_data_pull.pullDataFull()

# Dashboard Layer

if st:

    def AssignYM(df):

        # Mapping of month names to month numbers
        month_map = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
            'July': '07', 'August': '08', 'September': '09', 'October': '10',
            'November': '11', 'December': '12',
            "1st Quarter":'03','2nd Quarter':"06","3rd Quarter":"09","4th Quarter":"12"
        }

        # Create yearMonth column by combining 'year' and 'month' (mapped to month number)
        df['yearMonth'] = df['year'].astype(str) + '-' + df['periodName'].map(month_map)

        df['date']=pd.to_datetime(df['yearMonth'])

        return df

    # Set the title of the dashboard

    st.set_page_config(page_title="Labor Statistics - Dashboard", layout="wide")

    st.title("U.S Bureau Of Labor Statistics - Dashboard")

    # sidebar to select dates

    st.sidebar.header('Filter By Date Range')

    start_date = st.sidebar.date_input("Start Date",
                value=datetime.now() - relativedelta(months=15),
                min_value=datetime.now() - relativedelta(months=15),
                max_value=datetime.now())

    end_date = st.sidebar.date_input("End Date",
                value=datetime.now(),
                min_value=datetime.now() - relativedelta(months=15),
                max_value=datetime.now())

    # chart: Unemployment Rate

    unemployment_rate_df = pd.read_csv(folder_name+'/'+'LNS14000000.csv')

    unemployment_rate_df = AssignYM(unemployment_rate_df)

    unemployment_rate_filtered_df = unemployment_rate_df.loc[(unemployment_rate_df['date']>=pd.Timestamp(start_date))&
                                                    (unemployment_rate_df['date']<=pd.Timestamp(end_date))]

    st.subheader('Unemployment Rate - Seasonally Adjusted')

    st.line_chart(unemployment_rate_filtered_df[['yearMonth','value']],
                  x='yearMonth',
                  y='value',
                  use_container_width=True)

    # Rest of the code for other charts and dataframes stays the same

    # Display raw data for Unemployment Rate

    st.subheader('Unemployment Rate - Raw Data')

    st.dataframe(unemployment_rate_filtered_df[["year", "period", "periodName", "latest", "value"]], use_container_width=True)

