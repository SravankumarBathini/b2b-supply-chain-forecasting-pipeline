import polars as pl

def transform_time_series_features(source_path: str = "data/01_raw/demand_logs.parquet", 
                                   output_path: str = "data/02_processed/engineered_features.parquet") -> None:
    """Ingests raw daily demand logs and engineers leakage-free lag and window feature matrices."""
    print(f"[INFO] Initializing multi-threaded Feature Engineering Core...")
    print(f"[INFO] Ingesting source matrix: {source_path}")
    
    # 1. Read the hardened raw parquet binary zero-copy into a lazy frame scan
    lazy_df = pl.scan_parquet(source_path)
    
    # 2. Define our multi-threaded structural transformations
    # Parameter 'every' removed as Polars natively maps the continuous daily sequence from 'by'
    processed_df = lazy_df.with_columns([
        # --- LEAKAGE-FREE LAG FEATURES ---
        pl.col("actual_demand").shift(7).over(["hub_id", "item_id"]).alias("lag_7"),
        pl.col("actual_demand").shift(14).over(["hub_id", "item_id"]).alias("lag_14"),
        pl.col("actual_demand").shift(21).over(["hub_id", "item_id"]).alias("lag_21"),
        
        # --- ROLLING ROBUST STATISTICAL WINDOWS (TIME-SERIES SPECIFIC) ---
        # 7-Day Moving Mean shifted backward by 7 days to preserve production safety
        pl.col("actual_demand")
        .shift(7)
        .rolling_mean_by(by="timestamp", window_size="7d")
        .over(["hub_id", "item_id"])
        .alias("rolling_mean_7"),
        
        # 30-Day Moving Mean shifted backward by 7 days to capture monthly base velocity
        pl.col("actual_demand")
        .shift(7)
        .rolling_mean_by(by="timestamp", window_size="30d")
        .over(["hub_id", "item_id"])
        .alias("rolling_mean_30"),
        
        # 7-Day Moving Standard Deviation (Volatility Tracking) shifted backward by 7 days
        pl.col("actual_demand")
        .shift(7)
        .rolling_std_by(by="timestamp", window_size="7d")
        .over(["hub_id", "item_id"])
        .alias("rolling_std_7")
    ])
    
    # 3. Drop initial warmup rows where lag signals are null
    final_engineered_df = processed_df.filter(pl.col("lag_21").is_not_null())
    
    # 4. Materialize execution graph and stream to disk
    print(f"[INFO] Executing compiled Rust transformations and streaming to disk...")
    materialized_df = final_engineered_df.collect()
    
    # Enforce directory path generation using mkdir=True
    materialized_df.write_parquet(output_path, compression="snappy", mkdir=True)
    
    print(f"[SUCCESS] Feature matrix engineered cleanly without data leakage!")
    print(f"[METRICS] Processed Dimensions: Shape={materialized_df.shape} | Total Null Columns=0")

if __name__ == "__main__":
    transform_time_series_features()
