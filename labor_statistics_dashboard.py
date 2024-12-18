# dependecies

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

    ]

    

    def __init__(self,name):

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

            # print(unique_df.sort_values(by=['year','periodName']))

            unique_df.to_csv(folder_name+'/'+_id+'.csv',index=False)

            print('updating the data ....')

        
        return {"status":"Incremental Successful"}

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
        "1st Quarter":'03','2nd Quarter':"06","3rd Quarter":"09","4th Quarter":"12"
    }

    # Create yearMonth column by combining 'year' and 'month' (mapped to month number)
    df['yearMonth'] = df['year'].astype(str) + '-' + df['periodName'].map(month_map)

    # print(df['yearMonth'])

    df['date']=pd.to_datetime(df['yearMonth'])

    return df

# Set the title of the dashboard

st.set_page_config(page_title="Labor Statistics - Dashboard", layout="wide")

st.title("U.S Beaureau Of Labor Statistics - Dashboard")

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

# # Add a button to trigger the reload
# if st.sidebar.button('Pull Latest Data',):    

#     api_data_pull.pullLatestData()

#     # This will cause the app to rerun
#     st.rerun()


# chart 1 : Civilian Labor Force

civ_labor_force = pd.read_csv(folder_name+'/'+'LNS11000000.csv')

civ_labor_force = AssignYM(civ_labor_force)

civ_labor_force_filtered = civ_labor_force.loc[(civ_labor_force['date']>=pd.Timestamp(start_date))&
                                                (civ_labor_force['date']<=pd.Timestamp(end_date))]

civ_labor_force_filtered.reset_index(inplace=True,drop=True)


# civ_labor_force_filtered.loc[:,'colorcode']=['#FF0000' if x == True else "#0000FF" for x in civ_labor_force_filtered['latest']]

# print(civ_labor_force.loc[:,'color'])



st.subheader('Number Of Civilian Employees Over A Period')

st.area_chart(civ_labor_force_filtered[['yearMonth','value']], 
                x="yearMonth", 
                y="value",x_label='Month',
                color='#ffaa00',
                # color='latest',
                y_label='Number Of Laborers',use_container_width=True)





# chart 2: Output Per Hour - Non-farm Business Productivity

non_farm_prod_df = pd.read_csv(folder_name+'/'+'PRS85006092.csv')

non_farm_prod_df = AssignYM(non_farm_prod_df)

non_farm_prod_filtered_df = non_farm_prod_df.loc[(non_farm_prod_df['date']>=pd.Timestamp(start_date))&
                                                (non_farm_prod_df['date']<=pd.Timestamp(end_date))]


# chart 3: Output Per Hour - Total Nonfarm Employment

non_farm_employment_df = pd.read_csv(folder_name+'/'+'CES0000000001.csv')

non_farm_employment_df = AssignYM(non_farm_employment_df)

non_farm_employment_filtered_df = non_farm_employment_df.loc[(non_farm_employment_df['date']>=pd.Timestamp(start_date))&
                                                (non_farm_employment_df['date']<=pd.Timestamp(end_date))]


col1,col2 = st.columns(2)

with col1:


    fig = px.pie(non_farm_prod_filtered_df, names='periodName',
    color = ['#DAF7A6','#900C3F','#1ABC9C','#7F8C8D'],
    values='value', title='Quarterwise Non-Farm Productivity')

    # Display the chart in Streamlit
    st.plotly_chart(fig)

with col2:


    st.subheader('Total Nonfarm Employment - Seasonally Adjusted')

    st.bar_chart(non_farm_employment_filtered_df, 
             x="yearMonth", y="value", color="#1ABC9C")



# chart 4: Output Per Hour - Civilian Employment

civ_employment_df = pd.read_csv(folder_name+'/'+'LNS12000000.csv')

civ_employment_df = AssignYM(civ_employment_df)

civ_employment_filtered_df = civ_employment_df.loc[(civ_employment_df['date']>=pd.Timestamp(start_date))&
                                                (civ_employment_df['date']<=pd.Timestamp(end_date))]

st.subheader('Civilian Employement - Seasonally Adjusted')

st.bar_chart(civ_employment_filtered_df,horizontal=True, 
             x="yearMonth", y="value",color='#FF6F61',use_container_width=True)

# chart 5: Total Private Average Hourly Earnings of Prod. and Nonsup. Employees - Seasonally Adjusted

hourly_earnings_prod_emp_df = pd.read_csv(folder_name+'/'+'CES0500000008.csv')

hourly_earnings_prod_emp_df = AssignYM(hourly_earnings_prod_emp_df)

hourly_earnings_prod_emp_filtered_df = hourly_earnings_prod_emp_df.loc[(hourly_earnings_prod_emp_df['date']>=pd.Timestamp(start_date))&
                                                (hourly_earnings_prod_emp_df['date']<=pd.Timestamp(end_date))]

st.subheader('Total Private Average Hourly Earnings - Seasonally Adjusted')

st.bar_chart(hourly_earnings_prod_emp_filtered_df, horizontal=True,
             x="yearMonth", y="value",color='#FFC300',use_container_width=True)


# chart 6: 

non_farm_bu_cost_df = pd.read_csv(folder_name+'/'+'PRS85006112.csv')

non_farm_bu_cost_df = AssignYM(non_farm_bu_cost_df)

non_farm_bu_cost_filtered_df = non_farm_bu_cost_df.loc[(non_farm_bu_cost_df['date']>=pd.Timestamp(start_date))&
                                                (non_farm_bu_cost_df['date']<=pd.Timestamp(end_date))]


fig = px.pie(non_farm_bu_cost_filtered_df, names='periodName',
        color = ['#3357FF','#16A085','#D35400','#BDC3C7'],
        values='value', title='Non Farm Business Unit Cost')


# Display the chart in Streamlit
st.plotly_chart(fig)


# Raw Data Pulled From API

st.subheader('Data Pulled From API : Civilian Labor Force (Seasonally Adjusted)')

st.dataframe(civ_labor_force_filtered[['yearMonth','latest','value']],use_container_width=True)

st.subheader('Non-farm Business Productivity')

st.dataframe(non_farm_prod_filtered_df[["year","period","periodName","latest","value","footnotes"]],use_container_width=True)

st.subheader('Non-farm Employment')

st.dataframe(non_farm_employment_filtered_df[["year","period","periodName","latest","value","footnotes"]],use_container_width=True)

st.subheader('Civilian Employement')

st.dataframe(civ_employment_filtered_df[["year","period","periodName","latest","value","footnotes"]],use_container_width=True)


st.subheader('Total Private Average Hourly Earnings')

st.dataframe(hourly_earnings_prod_emp_filtered_df[["year","period","periodName","latest","value","footnotes"]],use_container_width=True)


st.subheader('Non Farm Business Unit Cost')

st.dataframe(non_farm_bu_cost_filtered_df[["year","period","periodName","latest","value","footnotes"]],use_container_width=True)
