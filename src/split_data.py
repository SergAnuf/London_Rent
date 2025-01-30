from sklearn.model_selection import StratifiedShuffleSplit
import pandas as pd


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
    test_size = 0.2
    SEED = 42
    data_dir =  "data/processed/rent_london_processed2.parquet"
    train_df, test_df = split_data(data_dir,0.2,SEED)
    train_df.to_parquet("data/modelling/train.parquet")
    test_df.to_parquet("data/modelling/test.parquet")
    
    
