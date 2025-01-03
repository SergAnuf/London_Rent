from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.models import BaseOperator
from apify_client import ApifyClient
import pandas as pd
import os
from airflow.models import Variable


columns = ['id', 'url', 'title', 'displayAddress', 'countryCode', 
           'deliveryPointId', 'ukCountry', 'outcode', 'incode', 'bathrooms', 
           'bedrooms', 'agent', 'agentPhone', 'agentLogo', 'agentDisplayAddress',
           'propertyType', 'price', 'secondaryPrice', 'coordinates', 'letAvailableDate', 
           'deposit', 'minimumTermInMonths', 'letType', 'furnishType', 'type', 
           'councilTaxExempt', 'councilTaxIncluded', 'annualGroundRent', 
           'groundRentReviewPeriodInYears', 'groundRentPercentageIncrease', 'annualServiceCharge', 
           'councilTaxBand', 'domesticRates', 'description', 'descriptionHtml', 'features', 
           'tenure', 'yearsRemainingOnLease', 'images', 'brochures', 'floorplans', 
           'nearestStations', 'epc', 'published', 'archived', 'sold', 'tags', 
           'agentProfileUrl', 'agentListingsUrl', 'agentDescriptionHtml', 
           'listingUpdateReason', 'listingUpdateDate', 'firstVisibleDate', 
           'displayStatus', 'addedOn', 'nearestSchools']


class ApifyTaskOperator(BaseOperator):
    def __init__(self, actor_id: str, run_input: dict = None, **kwargs):
        super().__init__(**kwargs)
        self.actor_id = actor_id
        self.run_input = run_input or {}

    def execute(self, context: Context):
        client = ApifyClient(Variable.get("APIFY_KEY"))
        actor = client.actor(self.actor_id)

        # Call the Apify actor and wait for the result
        self.log.info(f"Running Apify Actor: {self.actor_id}")
        run = actor.call(run_input=self.run_input)
        
        dataset = client.dataset(run["defaultDatasetId"])
        items = []
        for item in dataset.iterate_items():
            items.append(item) 
        
        # Convert list of dictionaries to a DataFrame
        results_data = pd.DataFrame(items)

        self.log.info(f"Pushing data to XCom: {results_data.head()}")  # Preview the DataFrame
        context['ti'].xcom_push(key='apify_result', value=results_data.to_json(orient='records'))  # Push as JSON string



def load(**kwargs):
    # Connect to Postgres
    hook = PostgresHook('ai_agency')
    ti = kwargs['ti']

    # Pull the DataFrame from XCom
    data_json = ti.xcom_pull(task_ids='run_apify_actor', key='apify_result')

    if not data_json:
        raise ValueError("No data received from XCom for key 'apify_result'")

    # Convert JSON back to a DataFrame
    df = pd.read_json(data_json)

    if df is not None:
        rows = df.values.tolist()
        data_columns = df.columns.tolist()

        # Connect to Postgres
        hook = PostgresHook('ai_agency')

        # Insert the data into the Postgres table
        hook.insert_rows(
            table='property_listings',
            rows=rows,
            target_fields=data_columns,
            replace=True,
            replace_index=['id']  # Optional: Define how to handle conflicts
        )
    else:
        print("No data found in XCom.")

