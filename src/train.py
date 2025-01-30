from catboost import CatBoostRegressor
import pandas as pd
import joblib




def train_model(train_path, output_path):
    """
    Train a CatBoostRegressor model and save it to disk.
    
    Args:
        train_path (str): Path to the training data (Parquet file).
        output_path (str): Path to save the trained model.
        model_params (dict): Hyperparameters for the CatBoostRegressor.
    """
    # Load training data
    train_df = pd.read_parquet(train_path)
    
    # Define features and target
    numerical_features = [
        "latitude", "longitude", "distance_to_center", "angle_from_center",
        "distance_to_station1", "distance_to_station2", "distance_to_station3"
    ]
    categorical_features = [
        "bedrooms", "bathrooms", "deposit", "zone", "borough", "propertyType",
        "furnishType", "NoiseClass", "letType", 
        "TFL1", "TFL2", "TFL3", "RAIL1", "RAIL2", "RAIL3"
    ]
    features = numerical_features + categorical_features
    target = "price"
    

    # Train the model
    model = CatBoostRegressor(cat_features=categorical_features)
    model.fit(train_df[features], train_df[target])
    
    # Save the model
    joblib.dump(model, output_path)
    print(f"Model saved to {output_path}")
    
    
if __name__ == "__main__":
    
    train_path = "data/modelling/train.parquet"
    model_save_path = "models/catboost_model_no_nlp.pkl"
    train_model(train_path, model_save_path)
