from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from airflow.models import BaseOperator
from apify_client import ApifyClient
import pandas as pd
import os
from airflow.models import Variable


def get_stations(data):
    "Function gets 3 nearest stations for each property from data, which is list of property jsons"
    processed_data = []
    for entry in data:
        station_data = entry['nearestStations']
        flattened_data = {
            f'name{i+1}': station['name'] for i, station in enumerate(station_data)
             }
        flattened_data.update({
        f'distance{i+1}': station['distance'] for i, station in enumerate(station_data)
             })
        flattened_data.update({
        f'station_types{i+1}': station['types'] for i, station in enumerate(station_data)
             })
        processed_data.append(flattened_data)
    
    return pd.DataFrame(processed_data)


def get_coordinates(data):
    
    processed_data = []

    for entry in data:
        coordinates = entry['coordinates']
        flattened_data = {
            f'latitude': coordinates['latitude']
             }
        flattened_data.update({f'longitude': coordinates['longitude']}
           )
        processed_data.append(flattened_data)
        
    return pd.DataFrame(processed_data)


def get_columns(data):
    
    useful_columns = ['id', 'price','secondaryPrice', 'bathrooms', 'bedrooms',
       'outcode', 'incode','displayAddress', 'propertyType', 'furnishType', "title","features" ,'description',
       'councilTaxBand', 'councilTaxExempt', 'councilTaxIncluded', 'letType',
       'type', 'deposit', 'addedOn', 'listingUpdateDate',
       'listingUpdateReason', 'firstVisibleDate', 'letAvailableDate',
       'sizeSqFeetMax', 'sizeSqFeetMin']
    
    return pd.DataFrame(list(data))[useful_columns]


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
        results_data = pd.concat([get_columns(items),get_coordinates(items),get_stations(items)],axis=1)

        self.log.info(f"Pushing data to XCom: {results_data.head()}")  # Preview the DataFrame
        context['ti'].xcom_push(key='apify_result', value=results_data.to_json(orient='records'))  # Push as JSON string


    
def load(**kwargs):
    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id='ai_agency')
    ti = kwargs['ti']

    # Pull the DataFrame from XCom
    data_json = ti.xcom_pull(task_ids='run_apify_actor', key='apify_result')

    if not data_json:
        raise ValueError("No data received from XCom for key 'apify_result'")

    # Convert JSON back to a DataFrame
    df = pd.read_json(data_json)

    if df is not None and not df.empty:
        rows = df.values.tolist()

        # Construct the insert query with UPSERT logic
        insert_query = """
        INSERT INTO right_move (
            id, price, secondary_price, bathrooms, bedrooms, outcode, incode,
            display_address, property_type, furnish_type, title, features,
            description, council_tax_band, council_tax_exempt, council_tax_included,
            let_type, type, deposit, added_on, listing_update_date, listing_update_reason,
            first_visible_date, let_available_date, size_sq_feet_max, size_sq_feet_min,
            latitude, longitude, name1, name2, name3, distance1, distance2, distance3,
            station_types1, station_types2, station_types3
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET
            price = EXCLUDED.price,
            secondary_price = EXCLUDED.secondary_price,
            bathrooms = EXCLUDED.bathrooms,
            bedrooms = EXCLUDED.bedrooms,
            outcode = EXCLUDED.outcode,
            incode = EXCLUDED.incode,
            display_address = EXCLUDED.display_address,
            property_type = EXCLUDED.property_type,
            furnish_type = EXCLUDED.furnish_type,
            title = EXCLUDED.title,
            features = EXCLUDED.features,
            description = EXCLUDED.description,
            council_tax_band = EXCLUDED.council_tax_band,
            council_tax_exempt = EXCLUDED.council_tax_exempt,
            council_tax_included = EXCLUDED.council_tax_included,
            let_type = EXCLUDED.let_type,
            type = EXCLUDED.type,
            deposit = EXCLUDED.deposit,
            added_on = EXCLUDED.added_on,
            listing_update_date = EXCLUDED.listing_update_date,
            listing_update_reason = EXCLUDED.listing_update_reason,
            first_visible_date = EXCLUDED.first_visible_date,
            let_available_date = EXCLUDED.let_available_date,
            size_sq_feet_max = EXCLUDED.size_sq_feet_max,
            size_sq_feet_min = EXCLUDED.size_sq_feet_min,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            name1 = EXCLUDED.name1,
            name2 = EXCLUDED.name2,
            name3 = EXCLUDED.name3,
            distance1 = EXCLUDED.distance1,
            distance2 = EXCLUDED.distance2,
            distance3 = EXCLUDED.distance3,
            station_types1 = EXCLUDED.station_types1,
            station_types2 = EXCLUDED.station_types2,
            station_types3 = EXCLUDED.station_types3;
        """

        # Insert the data into the Postgres table
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, rows)  # Efficient batch insert
            conn.commit()
    else:
        print("No data found in XCom.")