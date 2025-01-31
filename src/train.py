from catboost import CatBoostRegressor
import pandas as pd
import joblib
import yaml
import pickle


def load_best_params_pickle(input_path):
    """Load the best parameters from a pickle file"""
    with open(input_path, "rb") as f:
        return pickle.load(f)


def train_model(params):
    """
    Train a CatBoostRegressor model and save it to disk.
    
    Args:
        train_path (str): Path to the training data (Parquet file).
        output_path (str): Path to save the trained model.
        model_params (dict): Hyperparameters for the CatBoostRegressor.
    """

    # Load training data
    train_df = pd.read_parquet(params["DATA_DIR"]["TRAIN"])
    features = params["FEATURES"]["numerical_features"] + params["FEATURES"]["categorical_features"]
    target = "price"

    best_params = load_best_params_pickle(params["BEST_PARAMS_DIR"])
    # Train the model
    model = CatBoostRegressor(**best_params, cat_features=params["FEATURES"]["categorical_features"])
    model.fit(train_df[features], train_df[target])

    # Save the model
    joblib.dump(model, params["SAVED_MODEL"])


if __name__ == "__main__":
    with open("params.yaml") as f:
        params = yaml.safe_load(f)
    train_model(params)
