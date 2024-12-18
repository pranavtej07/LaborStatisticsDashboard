import pandas as pd
import os
import streamlit as st
import plotly.express as px
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json

# ======= CONFIGURATIONS ======= #
API_KEY = '93ee8e3a1d66428298ca51eceb0fca71'
STORAGE_FOLDER = "dashboard_files"  # Directory to store fetched data
URI_API = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'  # API endpoint
DATA_SERIES = [
    "LNS14000000",  # Unemployment Rate
    "CES0000000001",  # Non-Farm Employment
    "PRS85006092",  # Output Per Hour - Non-Farm Business Productivity
    "LNS12000000",  # Civilian Employment
    "CES0500000008",  # Total Private Average Hourly Earnings
    "PRS85006112",  # Nonfarm Business Unit Labor Costs
]

# ======= DATA FETCH FUNCTIONS ======= #
def fetch_initial_data():
    """
    Fetches historical data for all data series from the API
    and saves it in the local storage folder as CSV files.
    """
    # Determine the data range
    prev_year = (datetime.now() - relativedelta(months=12)).year

    # API request setup
    headers = {'Content-type': 'application/json'}
    payload = json.dumps({
        "seriesid": DATA_SERIES,
        "startyear": str(prev_year),
        "endyear": datetime.now().year,
        "registrationkey": API_KEY
    })

    # Send the API request and process the response
    response = requests.post(URI_API, data=payload, headers=headers)
    data = json.loads(response.text)

    # Ensure the storage folder exists
    if not os.path.exists(STORAGE_FOLDER):
        os.makedirs(STORAGE_FOLDER)

    # Save each data series as a CSV
    for series in DATA_SERIES:
        series_data = [x for x in data['Results']['series'] if x['seriesID'] == series]
        if series_data:
            df = pd.DataFrame.from_dict(series_data[0]['data'], orient='columns')
            df.to_csv(f"{STORAGE_FOLDER}/{series}.csv", index=False)


def update_data():
    """
    Fetches the latest data for all series and updates the corresponding CSV files.
    """
    headers = {'Content-type': 'application/json'}
    payload = json.dumps({"seriesid": DATA_SERIES, 'latest': True, "registrationkey": API_KEY})

    # Send the API request and process the response
    response = requests.post(URI_API, data=payload, headers=headers)
    data = json.loads(response.text)

    for series in DATA_SERIES:
        series_data = [x for x in data['Results']['series'] if x['seriesID'] == series]
        if series_data:
            new_data = pd.DataFrame.from_dict(series_data[0]['data'], orient='columns')
            old_data = pd.read_csv(f"{STORAGE_FOLDER}/{series}.csv")

            # Combine old and new data, removing duplicates
            combined_data = pd.concat([old_data, new_data], ignore_index=True)
            combined_data = combined_data.drop_duplicates(subset=['year', 'periodName'])

            # Save the updated dataset
            combined_data.to_csv(f"{STORAGE_FOLDER}/{series}.csv", index=False)

# ======= DATA PROCESSING ======= #
def prepare_data(df, start_date, end_date):
    """
    Filters and formats the dataset for visualization within a given date range.
    """
    month_mapping = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12',
        "1st Quarter": '03', '2nd Quarter': "06", '3rd Quarter': "09", '4th Quarter': "12"
    }

    df['yearMonth'] = df['year'].astype(str) + '-' + df['periodName'].map(month_mapping)
    df['date'] = pd.to_datetime(df['yearMonth'])
    filtered_data = df[(df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))]
    return filtered_data.reset_index(drop=True)

# ======= DASHBOARD LAYOUT ======= #
def create_chart(data, chart_type, title, x, y, color=None, **kwargs):
    """
    Generates a Plotly chart based on the specified chart type and configuration.
    """
    chart_mapping = {
        "line": px.line,
        "bar": px.bar,
        "scatter": px.scatter,
        "area": px.area
    }

    if chart_type not in chart_mapping:
        raise ValueError(f"Unsupported chart type: {chart_type}")

    fig = chart_mapping[chart_type](data, x=x, y=y, title=title, color=color, **kwargs)
    fig.update_layout(
        title=dict(font=dict(size=18)),
        xaxis=dict(title=x),
        yaxis=dict(title=y)
    )
    return fig


def display_chart_and_data(df, chart_type, title, x, y, raw_data_title, color=None, color_sequence=None, **kwargs):
    """
    Displays a chart and its corresponding raw data table.
    """
    fig = create_chart(df, chart_type, title, x, y, color=color, color_discrete_sequence=color_sequence, **kwargs)
    st.plotly_chart(fig, use_container_width=True)
    st.write(f"### Raw Data: {raw_data_title}")
    st.dataframe(df)

# ======= MAIN DASHBOARD ======= #
if __name__ == "__main__":
    # Pull data if the storage folder doesn't exist
    if not os.path.exists(STORAGE_FOLDER):
        fetch_initial_data()

    # Set up the Streamlit page
    st.set_page_config(page_title="Dashboard of Labor Statistics", layout="wide")
    st.title("Dashboard of Labor Statistics")

    # Sidebar: Filter by Date Range
    st.sidebar.header('Filter by Date Range')
    start_date = st.sidebar.date_input("Start Date", value=datetime.now() - relativedelta(months=15))
    end_date = st.sidebar.date_input("End Date", value=datetime.now())

    # Load and preprocess datasets
    datasets = {
        "Unemployment Rate": prepare_data(pd.read_csv(f"{STORAGE_FOLDER}/LNS14000000.csv"), start_date, end_date),
        "Non-Farm Employment": prepare_data(pd.read_csv(f"{STORAGE_FOLDER}/CES0000000001.csv"), start_date, end_date),
        "Productivity": prepare_data(pd.read_csv(f"{STORAGE_FOLDER}/PRS85006092.csv"), start_date, end_date),
        "Civilian Employment": prepare_data(pd.read_csv(f"{STORAGE_FOLDER}/LNS12000000.csv"), start_date, end_date),
        "Hourly Earnings": prepare_data(pd.read_csv(f"{STORAGE_FOLDER}/CES0500000008.csv"), start_date, end_date),
        "Labor Costs": prepare_data(pd.read_csv(f"{STORAGE_FOLDER}/PRS85006112.csv"), start_date, end_date),
    }

    # Display charts and their corresponding raw data
    display_chart_and_data(
        datasets["Unemployment Rate"], "line", "Unemployment Rate Over Time", "date", "value", "Unemployment Rate",
        color_sequence=["blue"], labels={"value": "Rate", "date": "Date"}, hover_data={"value": ":.2f"}
    )

    display_chart_and_data(
        datasets["Non-Farm Employment"], "bar", "Non-Farm Employment Distribution", "date", "value", "Non-Farm Employment",
        color="periodName", color_sequence=px.colors.qualitative.Pastel, labels={"value": "Employment Count", "date": "Date"}
    )

    display_chart_and_data(
        datasets["Productivity"], "area", "Productivity Over Time", "date", "value", "Non-Farm Productivity",
        color_sequence=["green"], labels={"value": "Productivity", "date": "Date"}
    )

    display_chart_and_data(
        datasets["Civilian Employment"], "scatter", "Civilian Employment Trends", "date", "value", "Civilian Employment",
        color_sequence=["purple"], labels={"value": "Employment Count", "date": "Date"}, hover_data={"value": ":.2f"}
    )

    display_chart_and_data(
        datasets["Hourly Earnings"], "line", "Hourly Earnings Trends", "date", "value", "Hourly Earnings",
        color_sequence=["orange"], labels={"value": "Earnings (USD)", "date": "Date"}, hover_data={"value": ":.2f"}
    )

    display_chart_and_data(
        datasets["Labor Costs"], "bar", "Labor Costs", "value", "yearMonth", "Labor Costs",
        color_sequence=px.colors.sequential.Blues, labels={"value": "Cost (USD)", "yearMonth": "Period"}, orientation="h"
    )