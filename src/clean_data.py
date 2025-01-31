# Data manipulation
import pandas as pd

# File handling
import os
import json
import yaml

# Text processing
import re


def get_stations(data):
    """Extract nearest station data from a list of property JSONs."""
    processed_data = []
    for entry in data:
        station_data = entry['nearestStations']
        flattened_data = {
            f'station_name{i + 1}': station['name'] for i, station in enumerate(station_data)
        }
        flattened_data.update({
            f'distance_to_station{i + 1}': station['distance'] for i, station in enumerate(station_data)
        })
        flattened_data.update({
            f'station_type{i + 1}': station['types'] for i, station in enumerate(station_data)
        })
        processed_data.append(flattened_data)
    return pd.DataFrame(processed_data)


def get_coordinates(data):
    """Extract latitude and longitude from a list of property JSONs."""
    processed_data = []
    for entry in data:
        coordinates = entry['coordinates']
        flattened_data = {
            'latitude': coordinates['latitude'],
            'longitude': coordinates['longitude']
        }
        processed_data.append(flattened_data)
    return pd.DataFrame(processed_data)


def transform_price(price_str):
    """Convert price string to an integer."""
    string = price_str.split(" ")[0]
    numbers = re.findall(r'\d+', string)
    return int(''.join(numbers))


def clean_data(data_dir):
    # Load JSON files
    json_files = [os.path.join(data_dir, file) for file in os.listdir(data_dir) if file.endswith('.json')]

    # Process each JSON file
    datasets = []
    for batch in json_files:
        with open(batch, 'r') as file:
            js = json.load(file)
            dfs = pd.DataFrame(js)
            coord = get_coordinates(js)
            stations = get_stations(js)
            dfs = pd.concat([dfs, coord, stations], axis=1)
            datasets.append(dfs)

    # Combine all datasets
    df = pd.concat(datasets)

    # Drop processed columns
    df = df.drop(["coordinates", "nearestStations"], axis=1)

    # Remove duplicates and set index
    df = df.drop_duplicates(subset="id")
    df = df.set_index("id")

    # Drop columns with >95% missing values
    df = df.drop([
        "groundRentPercentageIncrease", "groundRentReviewPeriodInYears", "tenure",
        "annualGroundRent", "domesticRates", "annualServiceCharge",
        "yearsRemainingOnLease", "minimumTermInMonths"
    ], axis=1)

    # Drop irrelevant columns
    df = df.drop([
        'agent', 'agentPhone', 'agentLogo', 'agentDisplayAddress', 'brochures',
        'agentProfileUrl', 'agentListingsUrl', 'agentDescriptionHtml',
        "images", "deliveryPointId", "type", "ukCountry", "countryCode", "url", "nearestSchools",
        'descriptionHtml', 'epc', 'published', 'archived', 'sold', 'tags', 'displayStatus'
    ], axis=1)

    # Remove rows with missing station names
    df = df[~df["station_name1"].isna()]

    # Convert station distances to float16
    df[["distance_to_station1", "distance_to_station2", "distance_to_station3"]] = df[
        ["distance_to_station1", "distance_to_station2", "distance_to_station3"]
    ].astype("float16")

    # Handle deposit column
    df['deposit'].fillna(0, inplace=True)
    df['deposit'] = df['deposit'].apply(lambda x: True if x != 0 else False)
    df['deposit'] = df['deposit'].astype(bool)

    # Transform price columns
    df["price"] = df["price"].apply(transform_price)
    df["secondaryPrice"] = df["secondaryPrice"].apply(transform_price)

    # Convert latitude and longitude to float
    df[["latitude", "longitude"]] = df[["latitude", "longitude"]].astype(float)

    # Remove rows with missing bedrooms and bathrooms
    df = df[~(df["bedrooms"].isna() & df["bathrooms"].isna())]

    # Drop floorplans column
    df = df.drop("floorplans", axis=1)

    # Handle size columns
    df["sizeSqFeetMax"] = df["sizeSqFeetMax"].fillna(0)
    df["sizeSqFeetMin"] = df["sizeSqFeetMin"].fillna(0)
    df[["sizeSqFeetMax", "sizeSqFeetMin"]] = df[["sizeSqFeetMax", "sizeSqFeetMin"]].astype(float)

    # Fill missing bathrooms
    df["bathrooms"] = df["bathrooms"].fillna(1)

    # Remove rows with missing bedrooms (except for Studio properties)
    df = df[~((df["propertyType"] != "Studio") & (df["bedrooms"].isna()))]

    # Handle listing dates
    df = df[~df["listingUpdateDate"].isna()]
    df["listingUpdateDate"] = pd.to_datetime(df['listingUpdateDate']).dt.date
    df["firstVisibleDate"] = pd.to_datetime(df["firstVisibleDate"]).dt.date

    return df


if __name__ == "__main__":
    with open("params.yaml") as f:
        params = yaml.safe_load(f)

    cleaned_data = clean_data(params["DATA_DIR"]["RAW_DIR"])
    cleaned_data.to_parquet(params["DATA_DIR"]["CLEANED"])