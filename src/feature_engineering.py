# Data manipulation
import pandas as pd
import numpy as np

# Spatial analysis
import geopandas as gpd
from shapely.geometry import Point, Polygon
import h3
from geopy.distance import geodesic

# File handling
import os

threshold = 10000

def compute_angle(lat, lon, center_lat, center_lon):
    """Compute the angle of a point relative to a center point."""
    lat1, lon1 = np.radians(center_lat), np.radians(center_lon)
    lat2, lon2 = np.radians(lat), np.radians(lon)
    
    dlon = lon2 - lon1
    x = np.cos(lat2) * np.sin(dlon)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    
    angle = np.arctan2(x, y)
    return angle


def invert_coordinates(geometry):
    """Invert coordinates in a geometry object."""
    if isinstance(geometry, Polygon):
        inverted_coords = [(y, x) for x, y in geometry.exterior.coords]
        return Polygon(inverted_coords)
    return geometry


def create_london_h3_index(london_boundaries, resolution=9):
    """Create an H3 hexagon index for London."""
    london_polygon = london_boundaries.union_all()
    hexagons = h3.polyfill(london_polygon.__geo_interface__, res=resolution)
    
    def hex_to_polygon(hex_id):
        boundary = h3.h3_to_geo_boundary(hex_id, geo_json=True)
        return Polygon(boundary)
    
    hex_data = {
        "hex_id": list(hexagons),
        "geometry": [hex_to_polygon(h) for h in hexagons]
    }
    
    hex_df = pd.DataFrame(hex_data)
    hex_gdf = gpd.GeoDataFrame(hex_df, geometry='geometry')
    return hex_gdf


def reduce_cardinality_station_type(x, station_map):
    """Reduce the cardinality of station types."""
    new_list = [station_map[y] if y in station_map.keys() else y for y in x]
    return list(set(new_list))



def preprocess_data(LOAD_FROM):
    # Load data
    df = pd.read_parquet(LOAD_FROM)
    london_boundaries = gpd.read_file('data/external/london_boroughs.geojson')
    
    output_dir = "data/external"
    kml_file = os.path.join(output_dir, 'doc.kml')
    zone_fares = gpd.read_file(kml_file)
    road_noize = gpd.read_file("data/external/Road_LAeq_16h_London/Road_LAeq_16h_London.shp")

    # Convert to GeoDataFrame
    df["geometry"] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf = gdf.set_crs(epsg=4326, inplace=True)

    # Merge with boroughs
    gdf = gpd.sjoin(gdf, london_boundaries, how="left")
    gdf = gdf.rename(columns={"name": "borough"})
    gdf = gdf.drop("index_right", axis=1)

    # Merge with travel zones
    gdf = gpd.sjoin(gdf, zone_fares, how="left")
    gdf = gdf.rename(columns={"Name": "zone"})
    gdf = gdf.drop(columns=["index_right", "Description"])
    gdf = gdf[gdf["zone"].notna()]
    gdf["zone"] = gdf["zone"].apply(lambda x: x.split(" ")[-1])

    # Reproject road noise data
    road_noize = road_noize.set_crs(epsg=27700, allow_override=True)
    road_noize = road_noize.to_crs(epsg=4326)

    # Compute distance and angle to city center
    city_center = (51.5072, -0.1276)
    gdf["distance_to_center"] = gdf[["latitude", "longitude"]].apply(
        lambda x: geodesic((x[0], x[1]), city_center).miles, axis=1
    )
    gdf['angle_from_center'] = gdf.apply(
        lambda row: compute_angle(row['latitude'], row['longitude'], city_center[0], city_center[1]), axis=1
    )

    # Create H3 hexagon index
    hex_gdf = create_london_h3_index(london_boundaries)
    hex_gdf = hex_gdf.set_crs("EPSG:4326", allow_override=True)
    hex_gdf['geometry'] = hex_gdf['geometry'].apply(invert_coordinates)

    # Merge hexagons with road noise data
    h3_noize = gpd.sjoin(hex_gdf, road_noize, how="left")
    h3_noize = h3_noize.drop("index_right", axis=1)
    h3_noize["NoiseClass"] = h3_noize["NoiseClass"].fillna("0")
    new_df = h3_noize[["hex_id", "NoiseClass"]].groupby("hex_id").agg("max").reset_index()
    hex_gdf = pd.merge(hex_gdf, new_df, how="left", on="hex_id")

    # Merge hexagons with travel zones
    hex_zone = gpd.sjoin(hex_gdf, zone_fares, how="left")
    hex_zone = hex_zone.drop(columns=["NoiseClass", "index_right", "Description"], axis=1)
    hex_zone = hex_zone.rename(columns={"Name": "zone_hex"})
    hex_zone = hex_zone[["hex_id", "zone_hex"]].groupby("hex_id").agg("min").reset_index()
    hex_gdf = pd.merge(hex_gdf, hex_zone, how="left", on="hex_id")

    # Merge hexagons with properties
    gdf = gpd.sjoin(gdf, hex_gdf, how="left")
    gdf = gdf[gdf["NoiseClass"].notna()]
    gdf = gdf[gdf["borough"].notna()]

    # Handle missing values
    gdf["bedrooms"] = gdf["bedrooms"].fillna(1)
    gdf[['bedrooms', 'bathrooms']] = gdf[['bedrooms', 'bathrooms']].astype(int)

    # Filter property types
    accepted_pr_types = ["Flat", "Apartment", "Terraced", "House", "Semi-Detached", "Maisonette", "House Share", "Detached", "End of Terrace", "Penthouse", "Flat Share"]
    gdf["propertyType"] = gdf["propertyType"].apply(lambda x: x if x in accepted_pr_types else "other")

    # Remove outliers
    gdf = gdf[gdf["distance_to_station1"] <= 2]

    # Reduce cardinality of station types
    station_map = {"LONDON_UNDERGROUND": "TFL", "LIGHT_RAILWAY": "TFL", "CABLE_CAR": "TFL", "LONDON_OVERGROUND": "TFL"}
    gdf["station_type1"] = gdf["station_type1"].apply(lambda x: reduce_cardinality_station_type(x, station_map))
    gdf["station_type2"] = gdf["station_type2"].apply(lambda x: reduce_cardinality_station_type(x, station_map))
    gdf["station_type3"] = gdf["station_type3"].apply(lambda x: reduce_cardinality_station_type(x, station_map))

    # Create binary columns for station types
    station_types = ['station_type1', 'station_type2', 'station_type3']
    keywords = {'TFL': 'TFL', 'RAIL': 'NATIONAL_TRAIN'}
    for keyword, term in keywords.items():
        for station_type in station_types:
            gdf[f"{keyword}{station_types.index(station_type) + 1}"] = gdf[station_type].apply(lambda x: term in x)
            
    # Quick patch for letType and furnishType : TO DO investigate these NaNs further and try a better NaNs handling

    gdf["letType"] = gdf["letType"].fillna("not_specified")
    gdf["furnishType"] = gdf["furnishType"].fillna("uknown")
    
    # outlierts in target
    gdf = gdf[gdf["price"]<threshold]

    return gdf



if __name__ == "__main__":
    
    TRAVEL_ZONE_MAP = "doc.kml"
    NOISE_DATA = "data/external/Road_LAeq_16h_London/Road_LAeq_16h_London.shp"
    BOROUGH_BOUNDARY = "data/external/london_boroughs.geojson"
    LOAD_FROM = "data/processed/rent_london2.parquet"
    SAVE_TO = "data/processed/rent_london_processed2.parquet"
    processed_data = preprocess_data(LOAD_FROM)
    processed_data.to_parquet("data/processed/rent_london_processed2.parquet")