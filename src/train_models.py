import json
import numpy as np
import polars as pl
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

def load_pipeline_config(config_path: str = "config/pipeline_config.json") -> dict:
    """Loads central parameters from the enterprise json schema configuration layer."""
    with open(config_path, "r") as file:
        return json.load(file)

def execute_chronological_training_pipeline() -> None:
    """Ingests engineered features, runs a TimeSeriesSplit validation loop, and trains the Ridge Core."""
    print("=========================================================")
    print("  INITIALIZING ENTERPRISE MODEL TRAINING ENGINECORE      ")
    print("=========================================================")
    
    # 1. Load data via Polars eager execution
    feature_matrix_path = "data/02_processed/engineered_features.parquet"
    df = pl.read_parquet(feature_matrix_path)
    print(f"[INFO] Ingested engineered feature matrix. Shape: {df.shape}")

    # 2. Separate Target Variable (Y_t) and Multi-Threaded Feature Columns (X)
    # We exclude tracking strings/metadata keys from our mathematical tensors
    feature_cols = [
        "base_demand", "trend_factor", "seasonal_factor",
        "lag_7", "lag_14", "lag_21",
        "rolling_mean_7", "rolling_mean_30", "rolling_std_7"
    ]
    target_col = "actual_demand"

    # Convert native Polars series objects to numpy contiguous memory matrices for Scikit-Learn
    X = df.select(feature_cols).to_numpy()
    y = df.select(target_col).to_numpy().ravel()

    # 3. Instantiate the Forward-Chaining Walk-Forward Cross-Validation Splitter
    # This enforces the arrow of time: training on the past, testing on the adjacent future
    tscv = TimeSeriesSplit(n_splits=3)
    
    print(f"[INFO] Initializing TimeSeriesSplit Validation Core (Splits={tscv.n_splits})")
    
    # Track cross-validation scores to check generalization performance
    fold_mae_scores = []
    fold_rmse_scores = []

    # 4. Execute the Chronological Rolling Window Training Loops
    for fold, (train_index, test_index) in enumerate(tscv.split(X), 1):
        # Extract chronological slice arrays without shuffling
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

        # Instantiate a Ridge Regression model with moderate L2 weight smoothing (alpha=1.0)
        model = Ridge(alpha=1.0)
        model.fit(X_train, y_train)

        # Generate rolling quarter forecasts on unseen future horizon points
        predictions = model.predict(X_test).clip(min=0.0) # Maintain production boundaries

        # Grade model metrics for the current temporal fold
        mae = mean_absolute_error(y_test, predictions)
        rmse = root_mean_squared_error(y_test, predictions)
        
        fold_mae_scores.append(mae)
        fold_rmse_scores.append(rmse)
        
        print(f"[FOLD {fold}] Train Samples: {len(train_index)} | Test Samples: {len(test_index)}")
        print(f"         └─> MAE (Mean Absolute Error): {mae:.2f} items")
        print(f"         └─> RMSE (Volatility Error) : {rmse:.2f} items")

    # 5. Calculate and Log Production Generalization Performance Metics
    print("=========================================================")
    print("  VALIDATION COMPLETE - COMPILING METRIC AGGREGATIONS   ")
    print("=========================================================")
    print(f"[METRICS] Mean Pipeline MAE: {np.mean(fold_mae_scores):.2f} units off per prediction.")
    print(f"[METRICS] Mean Pipeline RMSE: {np.mean(fold_rmse_scores):.2f} (Penalizes massive spikes).")

    # 6. Train the Final Production-Hardened Model on the Full Historical Array
    print("[INFO] Serializing complete historical training matrix down to model core...")
    final_production_model = Ridge(alpha=1.0)
    final_production_model.fit(X, y)
    
    # Log model feature weights to confirm L2 regularization distribution
    print("[INFO] Model Weights successfully regularized. Coefficient Trace:")
    for name, weight in zip(feature_cols, final_production_model.coef_):
        print(f"       └─> Feature [{name:16}] Weights Coefficient: {weight:+.4f}")

if __name__ == "__main__":
    execute_chronological_training_pipeline()
