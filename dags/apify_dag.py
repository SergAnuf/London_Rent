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
    "fullScrape": False,
    "includeNearestSchools": True,
    "includePriceHistory": False,
    "listUrls": [
        {
            "url": "https://www.rightmove.co.uk/property-to-rent/find.html?keywords=&sortType=2&viewType=LIST&channel=RENT&index=0&radius=0.0&locationIdentifier=USERDEFINEDAREA%5E%7B%22polylines%22%3A%22kwgyHx%7Bn%40l%40HsAsOkAsH_%40iKh%40%7BR~%40%7DK_%40aHU%7BShAmEgA_Fu%40q%5ChDqGsAqFHyJhAoVHmLs%40qOk%40cGSaV%7D%40dBLqMVzAM%60Kn%40lBdAe%40_AhFhBoBcDrAjDoKcCi%40JaHwByBoX~c%40is%40nj%40%7BNdLc%5BxQ_SjLul%40b%5CqEbCkl%40lDqBLeH%60%60%40iAhGE%5CY~PcAvr%40%3FLxk%40%7C%5CxQMxPQvk%40sMre%40%7DLrCu%40Dn%40%7CDvl%40~JeCbIsBtUeWd%40i%40s%40mT%60Zv%40H%3F%7CN%60%40JM%22%7D",
            "method": "GET"
        },
        {
            "url": "https://www.rightmove.co.uk/property-to-rent/find.html?keywords=&sortType=2&viewType=LIST&channel=RENT&index=0&radius=0.0&locationIdentifier=USERDEFINEDAREA%5E%7B%22polylines%22%3A%22c%7BeyHrtc%40xSeY%40uO%5BeCZsD%40kHf%40ZNaB%60i%40%60OvShQaRv%60%40tLps%40lR~aAkQnr%40mSb%5EuNyo%40kXrVkMtHxCz%60%40sQ~EsJzEiTfQkJxZaCKW%60BMV_%40xBKo%40KLHOWoAk%40UDjAKoAGABK%7B%40yIMsOrJcO~GeZsB%7BMkBwNuEqCqAqJy%40uK_BjAo%40uKZ_H~BeApAkFkG%7BIWcHvA%7BDcAqBpDQqCcHlWkb%40tC%7DDlIeLtYs%60%40%22%7D",
            "method": "GET"
        }
    ],
    "monitoringMode": True,
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

