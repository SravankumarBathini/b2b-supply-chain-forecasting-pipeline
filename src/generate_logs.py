import json
import numpy as np
import polars as pl
from datetime import datetime

def load_pipeline_config(config_path: str = "config/pipeline_config.json") -> dict:
    """Loads central parameters from the enterprise json schema configuration layer."""
    with open(config_path, "r") as file:
        return json.load(file)

def build_high_velocity_demand_matrix() -> pl.DataFrame:
    """Engineers a multi-year time-indexed transactional database using native Polars expressions."""
    # 1. Ingest corporate parameters
    config = load_pipeline_config()
    sim = config["simulation"]
    physics = config["signal_physics"]
    
    print(f"[INFO] Initializing multi-threaded engine across {pl.thread_pool_size()} compute threads...")
    print(f"[INFO] Generating log space from {sim['start_date']} to {sim['end_date']}.")

    # 2. Construct contiguous primary date backbone axis
    start = datetime.strptime(sim["start_date"], "%Y-%m-%d")
    end = datetime.strptime(sim["end_date"], "%Y-%m-%d")
    
    date_df = pl.date_range(
        start=start,
        end=end,
        interval="1d",
        eager=True
    ).to_frame(name="timestamp")

    # Add temporal ordinal indices for mathematical signal transformations
    date_df = date_df.with_columns([
        pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
        ((pl.col("timestamp") - start).dt.total_days()).alias("timeline_index")
    ])

    # 3. Instantiate Entity Backbones
    hub_df = pl.DataFrame({"hub_id": sim["hubs"]}).with_columns(pl.col("hub_id").cast(pl.Categorical))
    item_df = pl.DataFrame({"item_id": sim["items"]}).with_columns(pl.col("item_id").cast(pl.Categorical))

    # 4. Generate the Comprehensive Cross-Product Matrix Space (Modern Polars Syntax)
    master_matrix = date_df.join(hub_df, how="cross").join(item_df, how="cross")

    # 5. Compute Deterministic Mathematical Signal Expressions Natively in Rust Pool
    total_rows = master_matrix.height
    np.random.seed(42) # Set strict seed value for isolated deterministic reproducibility
    gaussian_noise = np.random.normal(0, physics["noise_sigma"], total_rows)

    # Map signal equations into clean columnar expressions
    calculated_matrix = master_matrix.with_columns([
        # Base Allocation
        pl.lit(physics["global_base_demand"]).alias("base_demand"),
        
        # Trend Component: beta * timeline_index
        (pl.col("timeline_index") * physics["growth_trend_beta"]).alias("trend_factor"),
        
        # Seasonality Component: alpha * sin(2 * pi * day_of_year / 365)
        (pl.lit(physics["seasonal_amplitude_alpha"]) * 
         ((pl.col("day_of_year") * 2 * np.pi / 365).sin())).alias("seasonal_factor"),
        
        # Inject standard numpy random array back into the frame
        pl.Series(name="noise", values=gaussian_noise)
    ])

    # 6. Apply Layered Corporate Overrides (Indian Autumn Festival Spike Model)
    # If the day falls in the festival window, multiply seasonal effects by the scalar
    final_logs = calculated_matrix.with_columns(
        pl.when(
            (pl.col("day_of_year") >= physics["festival_window_start_day"]) & 
            (pl.col("day_of_year") <= physics["festival_window_end_day"])
        )
        .then(pl.col("seasonal_factor") * physics["festival_spike_scalar"])
        .otherwise(pl.col("seasonal_factor"))
        .alias("seasonal_factor")
    )

    # 7. Synthesize Composite Actual Demand Target Variable (Y_t = Base + T + S + I)
    # Modern Polars clip syntax uses .clip(lower_bound, upper_bound)
    final_logs = final_logs.with_columns(
        (pl.col("base_demand") + pl.col("trend_factor") + pl.col("seasonal_factor") + pl.col("noise"))
        .clip(lower_bound=0.0, upper_bound=None)
        .alias("actual_demand")
    )

    # Enforce strict sort tracking properties across layout
    return final_logs.sort(["hub_id", "item_id", "timestamp"])

def serialize_to_hardened_parquet(df: pl.DataFrame) -> None:
    """Serializes the dataframe zero-copy via PyArrow buffers into compressed Parquet disk storage."""
    config = load_pipeline_config()
    target_path = config["storage"]["raw_output_path"]
    
    print(f"[INFO] Commencing PyArrow zero-copy binary serialization to disk...")
    df.write_parquet(target_path, compression="snappy")
    print(f"[SUCCESS] Enterprise Database Generated and Closed. Output Target: {target_path}")
    print(f"[METRICS] Structural Dimensions: Shape={df.shape} | Columns={df.columns}")

if __name__ == "__main__":
    demand_df = build_high_velocity_demand_matrix()
    serialize_to_hardened_parquet(demand_df)
