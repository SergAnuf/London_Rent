from sklearn.model_selection import StratifiedShuffleSplit
import pandas as pd
import yaml

def split_data(data_dir, test_size, random_state):
    # Load processed data
    gdf = pd.read_parquet(data_dir)

    stratified_split = StratifiedShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
    # Stratified split based on boroughs
    for train_index, test_index in stratified_split.split(gdf, gdf['borough']):
        train_df = gdf.iloc[train_index]
        test_df = gdf.iloc[test_index]
        
    return train_df, test_df
    
    
if __name__ == "__main__":
    with open("params.yaml", "r") as f:
        params = yaml.safe_load(f)
   
    train_df, test_df = split_data(params["DATA_DIR"]["PROCESSED"],params["test_size"],params["seed"])
    train_df.to_parquet(params["DATA_DIR"]["TRAIN"])
    test_df.to_parquet(params["DATA_DIR"]["TEST"])
    
    
