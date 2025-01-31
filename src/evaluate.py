import pandas as pd
import matplotlib.pyplot as plt
import joblib
import numpy as np
from sklearn.metrics import mean_absolute_error, r2_score


def load_data_and_model(test_path, model_path):
    """
    Load the test data and trained model.
    
    Args:
        test_path (str): Path to the test data (Parquet file).
        model_path (str): Path to the trained model (joblib file).
    
    Returns:
        test_df (pd.DataFrame): Test dataset.
        model: Trained CatBoostRegressor model.
    """
    test_df = pd.read_parquet(test_path)
    model = joblib.load(model_path)
    return test_df, model


def evaluate_model(model, test_df, features, target):
    """
    Evaluate the model on the test data and compute metrics.
    
    Args:
        model: Trained CatBoostRegressor model.
        test_df (pd.DataFrame): Test dataset.
        features (list): List of feature names.
        target (str): Name of the target variable.
    
    Returns:
        y_true (np.array): True target values.
        y_pred (np.array): Predicted target values.
        metrics (dict): Dictionary of evaluation metrics (MAPE, MAE, R²).
    """
    y_true = test_df[target].values
    y_pred = model.predict(test_df[features])

    # Compute metrics
    mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)

    metrics = {
        "MAPE": mape,
        "MAE": mae,
        "R²": r2
    }

    return y_true, y_pred, metrics


def plot_feature_importance(model, feature_names):
    """
    Plot feature importance for the trained model.
    
    Args:
        model: Trained CatBoostRegressor model.
        feature_names (list): List of feature names.
    """
    feature_importances = model.get_feature_importance()
    importance_df = pd.DataFrame({
        'Feature': feature_names,
        'Importance': feature_importances
    }).sort_values(by='Importance', ascending=False)

    plt.figure(figsize=(10, 6))
    plt.barh(importance_df['Feature'], importance_df['Importance'], color='skyblue')
    plt.xlabel('Importance')
    plt.ylabel('Feature')
    plt.title('Feature Importance - CatBoost')
    plt.gca().invert_yaxis()
    plt.show()


def main(test_path, model_path):
    """
    Main function to load data, evaluate the model, and display results.
    
    Args:
        test_path (str): Path to the test data (Parquet file).
        model_path (str): Path to the trained model (joblib file).
    """
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

    # Load data and model
    test_df, model = load_data_and_model(test_path, model_path)

    # Evaluate the model
    y_true, y_pred, metrics = evaluate_model(model, test_df, features, target)

    # Print metrics
    print(f"MAPE: {metrics['MAPE']:.2f}%")
    print(f"MAE: {metrics['MAE']:.2f}")
    print(f"R²: {metrics['R²']:.2f}")

    # Plot feature importance
    plot_feature_importance(model, features)


if __name__ == "__main__":
    # Define paths
    test_path = "data/modelling/test.parquet"
    model_path = "models/catboost_model_no_nlp.pkl "

    # Run evaluation
    main(test_path, model_path)
