from airflow import DAG
from airflow.operators.python import PythonOperator
from apify_client import ApifyClient
from steps.apify_dag import load, ApifyTaskOperator
import pendulum
import os
from airflow.models import Variable
from datetime import timedelta

run_input = {
    "addEmptyTrackerRecord": False,
    "deduplicateAtTaskLevel": False,
    "enableDelistingTracker": False,
    "fullPropertyDetails": True,
    "fullScrape": True,
    "includeNearestSchools": True,
    "includePriceHistory": False,
    "listUrls": [
        {
            "url": "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E61537&numberOfPropertiesPerPage=24&radius=0.0&sortType=2&index=0&includeLetAgreed=false&viewType=LIST&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false&searchLocation=Redbridge+%28London+Borough%29&useLocationIdentifier=true&previousSearchLocation=null&_includeLetAgreed=on",
            "method": "GET"
        },
        {
            "url": "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E61400&numberOfPropertiesPerPage=24&radius=0.0&sortType=2&index=0&includeLetAgreed=false&viewType=LIST&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false&searchLocation=Barking+And+Dagenham+%28London+Borough%29&useLocationIdentifier=true&previousSearchLocation=null&_includeLetAgreed=on",
            "method": "GET"
        }
    ],
    "monitoringMode": False,
    "proxy": {
        "useApifyProxy": True
    }
}

# Optional: Return the data

# Define a DAG
with DAG(
        dag_id='APIFY_CLIENT',
        schedule='@once',
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC")) as dag:
    # Task: Call an Apify Actor
    apify_task = ApifyTaskOperator(
        task_id='run_apify_actor',
        actor_id=Variable.get("RIGHTMOVE_ACTOR_ID"),
        run_input=run_input,  # Replace with your actor's input
        execution_timeout=timedelta(minutes=45),  # Allow more time for the task
        retries=2
    )

    load_step = PythonOperator(
        task_id='load',
        python_callable=load
    )

    # Set task dependencies
    apify_task >> load_step

