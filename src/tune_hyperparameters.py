import argparse
import pickle
import yaml
import numpy as np
from catboost import CatBoostRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import mean_squared_error
import pandas as pd

def save_best_params_pickle(params, output_path):
    """Save best parameters using pickle"""
    with open(output_path, "wb") as f:
        pickle.dump(params, f)
print("change")
def main():

    with open("params.yaml", "r") as f:
        params = yaml.safe_load(f)
    
    categorical_features = params["FEATURES"]["categorical_features"]
    numerical_features = params["FEATURES"]["numerical_features"]
    features = numerical_features+categorical_features
    target = "price"
    
    df = pd.read_parquet(params["DATA_DIR"]["TRAIN"])
  

    hyperparameters_space =  {
                  "iterations": np.random.randint(500, 2000, 10), 
                  "learning_rate": np.random.uniform(0.01, 0.3, 10), 
                  "depth": np.random.randint(4, 12, 10),  
                  "l2_leaf_reg": np.random.uniform(0.01, 10, 10),  
                  "bagging_temperature": np.random.uniform(0.0, 1.0, 10), 
                              }

    # Initialize CatBoostRegressor
    model = CatBoostRegressor(
        cat_features=categorical_features,
        loss_function="RMSE",
        verbose=0
    )
    # print change
    # Set up RandomizedSearchCV
    print("Starting hyperparameter tuning...")
    search = RandomizedSearchCV(
        model,
        param_distributions=hyperparameters_space,
        n_iter= params["n_iter"],  
        scoring=params["scorings_cv"],
        cv= params["CV_FOLDS"],
        n_jobs=-1,
        verbose=3,
        random_state=params["seed"]
    )

    # Perform the search
    search.fit(df[features], df[target])

    # Get best parameters and model
    best_params = search.best_params_

    # Save best parameters
    print("Best parameters found:")
    save_best_params_pickle(best_params, params["BEST_PARAMS_DIR"])

if __name__ == "__main__":
    main()