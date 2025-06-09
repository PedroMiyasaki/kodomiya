import duckdb
import pandas as pd
from unidecode import unidecode
from datetime import datetime
import logging
import os
import sys
import joblib
import json
import numpy as np

from functools import partial

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
from sklearn.compose import TransformedTargetRegressor
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error

import optuna

# --- Project Root Setup ---
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# --- Logging Setup ---
def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("run_model_training")
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

logger = setup_logging()

# --- Data Cleaning (adapted from run_pre_analysis.py) ---
def kb_data_cleaning(df):
    """
    Clean and preprocess the knowledge base dataframe for model training.
    """
    # Define required columns for dropping NAs - only the absolute essentials before imputation
    required_cols = ["preco", "tamanho", "bairro", "id"]
    
    # Drop rows where essential columns are missing
    df = df.dropna(subset=required_cols)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["id"])
    
    # Filter property size and price
    df = df.loc[(df["tamanho"] > 10) & (df["tamanho"] < 1_000)]
    df = df.loc[(df["preco"] > 10_000) & (df["preco"] < 10_000_000)]
    
    # Format neighborhood names
    df["bairro"] = df["bairro"].apply(lambda x: unidecode(x.lower().strip()))

    # Fillna bairro with "n/a"
    df["bairro"].fillna("n/a", inplace=True)
    
    return df

def objective(trial, X, y, preprocessor):
    """Optuna objective function to tune RandomForest hyperparameters."""
    
    # Define the search space for the "Pareto" parameters
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 200, 2000),
        'max_depth': trial.suggest_int('max_depth', 10, 50),
        'min_samples_split': trial.suggest_int('min_samples_split', 2, 15),
        'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
        'max_features': trial.suggest_float('max_features', 0.1, 1.0),
        'random_state': 42,
        'n_jobs': -1
    }
    
    # Create the pipeline with the suggested parameters
    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(**params))
    ])

    # Wrap in TransformedTargetRegressor
    log_transformed_model = TransformedTargetRegressor(
        regressor=model_pipeline,
        func=np.log1p,
        inverse_func=np.expm1
    )

    # Evaluate the model using cross-validation
    # Optuna minimizes the objective, so we use negative MAE (a higher score is better)
    score = cross_val_score(
        log_transformed_model, X, y, n_jobs=-1, cv=3, scoring='neg_mean_absolute_error'
    ).mean()
    
    return score

# --- Main Training Function ---
def main():
    logger.info("🚀 Starting model training script...")

    try:
        # --- 1. Load Data ---
        db_path = os.path.join(_PROJECT_ROOT, "db", "kodomiya.duckdb")
        logger.info(f"Connecting to DuckDB at {db_path}")
        con = duckdb.connect(database=db_path, read_only=True)

        # Select all columns needed for the richer model
        kb_querys = [
            "SELECT preco, tamanho, n_quartos, n_banheiros, n_garagem, bairro, latitude, longitude, id FROM kodomiya_chaves_na_mao.chaves_na_mao_register",
            "SELECT preco, tamanho, n_quartos, n_banheiros, n_garagem, bairro, latitude, longitude, id FROM kodomiya_viva_real.viva_real_register",
            "SELECT preco, tamanho, n_quartos, n_banheiros, n_garagem, bairro, latitude, longitude, id FROM kodomiya_zap_imoveis.zap_imoveis_register"
        ]

        logger.info("Fetching and concatenating knowledge base data...")
        df_kb_list = [con.execute(query).fetch_df() for query in kb_querys]
        con.close()
        
        df_kb = pd.concat(df_kb_list, ignore_index=True)
        logger.info(f"Knowledge base data loaded with {len(df_kb)} rows.")

        # --- 2. Clean Data ---
        logger.info("Cleaning knowledge base data for training...")
        df_kb = kb_data_cleaning(df_kb)
        logger.info(f"Knowledge base data cleaned. Rows remaining: {len(df_kb)}")

        # --- Impute Lat/Lon ---
        logger.info("Imputing missing latitude and longitude...")
        logger.info(f"Nulls before: Lat={df_kb['latitude'].isnull().sum()}, Lon={df_kb['longitude'].isnull().sum()}")

        # Calculate city-wide mean as a fallback
        city_mean_lat = df_kb['latitude'].mean()
        city_mean_lon = df_kb['longitude'].mean()
        
        if pd.notna(city_mean_lat) and pd.notna(city_mean_lon):
            logger.info(f"City-wide mean coordinates: Lat={city_mean_lat:.4f}, Lon={city_mean_lon:.4f}")

            # Impute using bairro mean where available
            df_kb['latitude'].fillna(df_kb.groupby('bairro')['latitude'].transform('mean'), inplace=True)
            df_kb['longitude'].fillna(df_kb.groupby('bairro')['longitude'].transform('mean'), inplace=True)

            # Fallback to city-wide mean for any remaining NaNs
            df_kb['latitude'].fillna(city_mean_lat, inplace=True)
            df_kb['longitude'].fillna(city_mean_lon, inplace=True)
        else:
            logger.warning("Could not calculate city-wide mean coordinates. Skipping lat/lon imputation.")
        
        # Final drop for safety to remove any rows that still couldn't be imputed
        df_kb.dropna(subset=['latitude', 'longitude'], inplace=True)

        logger.info(f"Nulls after: Lat={df_kb['latitude'].isnull().sum()}, Lon={df_kb['longitude'].isnull().sum()}")
        logger.info(f"Rows remaining after lat/lon imputation: {len(df_kb)}")
        logger.info("Latitude and longitude imputation complete.")

        if df_kb.empty:
            logger.error("No data available for training after cleaning. Aborting.")
            return

        # --- 3. Impute Missing Values ---
        logger.info("Imputing missing values for n_quartos, n_banheiros, and n_garagem...")
        imputers = {}

        # Impute n_quartos and n_banheiros with Linear Regression
        for col in ['n_quartos', 'n_banheiros']:
            # Train imputer
            lr = LinearRegression()
            train_data = df_kb.dropna(subset=[col, 'tamanho'])
            lr.fit(train_data[['tamanho']], train_data[col])
            imputers[f'{col}_imputer'] = lr
            
            # Apply imputer to fill missing values
            missing_mask = df_kb[col].isna()
            if missing_mask.any():
                predicted_values = lr.predict(df_kb.loc[missing_mask, [['tamanho']]])
                # Round, clip at a minimum of 1, and cast to integer
                df_kb.loc[missing_mask, col] = np.round(predicted_values).clip(1).astype(int)

        # Impute n_garagem with the median value
        garagem_median = df_kb['n_garagem'].median()
        imputers['n_garagem_imputer'] = garagem_median
        df_kb['n_garagem'].fillna(garagem_median, inplace=True)
        
        # Ensure all imputed columns are integer type
        df_kb[['n_quartos', 'n_banheiros', 'n_garagem']] = df_kb[['n_quartos', 'n_banheiros', 'n_garagem']].astype(int)

        # Apply filters after imputation
        df_kb = df_kb.loc[(df_kb["n_quartos"] > 0) & (df_kb["n_quartos"] < 10)]
        df_kb = df_kb.loc[(df_kb["n_banheiros"] > 0) & (df_kb["n_banheiros"] < 10)]
        logger.info(f"Rows remaining after post-imputation filtering: {len(df_kb)}")
        logger.info("Imputation complete.")

        # --- 4. Feature Engineering ---
        logger.info("Starting state-of-the-art feature engineering...")

        # A. Geospatial Clustering with K-Means
        logger.info("Creating geospatial clusters with K-Means...")
        kmeans = KMeans(n_clusters=20, random_state=42, n_init='auto')
        df_kb['bairro_cluster'] = kmeans.fit_predict(df_kb[['latitude', 'longitude']])
        df_kb['bairro_cluster'] = df_kb['bairro_cluster'].astype('category')
        logger.info("Geospatial clusters created.")
        
        # B. Distance to Points of Interest
        logger.info("Calculating distances to points of interest...")
        poi_path = os.path.join(_PROJECT_ROOT, "data", "model_training", "points_of_interest.json")
        distance_features = []
        try:
            with open(poi_path, 'r', encoding='utf-8') as f:
                points_of_interest = json.load(f)
            
            valid_pois = [p for p in points_of_interest if p.get('latitude') is not None and p.get('longitude') is not None]
            logger.info(f"Successfully loaded {len(valid_pois)} valid points of interest.")

            for poi in valid_pois:
                # Sanitize point name for column name
                point_name = poi['point_name']
                sanitized_name = ''.join(e for e in point_name if e.isalnum() or e.isspace()).replace(' ', '_').lower()
                feature_name = f"dist_{sanitized_name}"
                distance_features.append(feature_name)
                
                poi_lat, poi_lon = poi['latitude'], poi['longitude']
                
                df_kb[feature_name] = np.sqrt(
                    (df_kb['latitude'] - poi_lat)**2 + (df_kb['longitude'] - poi_lon)**2
                )
            logger.info(f"Created {len(distance_features)} distance features.")

        except Exception as e:
            logger.error(f"Could not load or parse points_of_interest.json: {e}. Skipping POI distance features.")

        # C. Interaction and Ratio Features
        logger.info("Creating interaction and ratio features...")
        # Use a small epsilon to avoid division by zero, though our filters should prevent it
        epsilon = 1e-6 
        df_kb['tamanho_por_quarto'] = df_kb['tamanho'] / (df_kb['n_quartos'] + epsilon)
        df_kb['banheiros_por_quarto'] = df_kb['n_banheiros'] / (df_kb['n_quartos'] + epsilon)
        logger.info("Interaction features created.")

        # --- 5. Feature Engineering & Model Definition ---
        numeric_features = [
            "tamanho", "n_quartos", "n_banheiros", "n_garagem", "latitude", "longitude",
            "tamanho_por_quarto", "banheiros_por_quarto"
        ] + distance_features
        categorical_features = ["bairro", "bairro_cluster"]
        features = numeric_features + categorical_features
        target = "preco"

        X = df_kb[features]
        y = df_kb[target]

        # Split data for evaluation before final training
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Create a preprocessor with ColumnTransformer
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numeric_features),
                ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features)
            ],
            remainder='passthrough'
        )
        
        # --- 6. Hyperparameter Tuning with Optuna ---
        logger.info("🚀 Starting hyperparameter tuning with Optuna...")
        
        # We need to pass additional arguments (X, y, preprocessor) to the objective function
        objective_with_data = partial(objective, X=X_train, y=y_train, preprocessor=preprocessor)
        
        # Create a study object and optimize the objective function.
        study = optuna.create_study(direction='maximize') # Maximize because neg_mean_absolute_error is negative
        study.optimize(objective_with_data, n_trials=50) # Using 50 trials for a good balance
        
        logger.info("Hyperparameter tuning finished.")
        logger.info(f"Best trial score: {study.best_value}")
        logger.info("Best parameters found: ")
        best_params = study.best_params
        logger.info(best_params)


        # --- 7. Train Final Model on Full Dataset with Best Parameters ---
        logger.info("Training final model on the full dataset with best parameters...")
        
        # Create the final pipeline with the best hyperparameters found by Optuna
        final_model_pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('regressor', RandomForestRegressor(random_state=42, n_jobs=-1, **best_params))
        ])

        final_log_transformed_model = TransformedTargetRegressor(
            regressor=final_model_pipeline,
            func=np.log1p,
            inverse_func=np.expm1
        )
        final_log_transformed_model.fit(X, y) # Fit on all data
        logger.info("Final optimized model trained successfully.")
        
        # --- 8. Evaluate Final Model on Hold-Out Test Set ---
        logger.info("Evaluating final model on the hold-out test set...")
        y_pred = final_log_transformed_model.predict(X_test)
        
        final_metrics = {
            'mae': mean_absolute_error(y_test, y_pred),
            'mse': mean_squared_error(y_test, y_pred),
            'r2': r2_score(y_test, y_pred),
            'timestamp': datetime.now().isoformat()
        }
        logger.info(f"Final Model Evaluation Metrics: {final_metrics}")


        # --- 9. Save Artifacts ---
        output_dir = os.path.join(_PROJECT_ROOT, "data", "model_training", datetime.now().strftime("%Y-%m-%d"))
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Saving artifacts to {output_dir}")

        # Save model pipeline
        model_path = os.path.join(output_dir, 'model_pipeline.joblib')
        joblib.dump(final_log_transformed_model, model_path)
        logger.info(f"Model pipeline saved to {model_path}")

        # Save K-Means model
        kmeans_path = os.path.join(output_dir, 'kmeans_model.joblib')
        joblib.dump(kmeans, kmeans_path)
        logger.info(f"K-Means model saved to {kmeans_path}")

        # Save imputers
        imputers_path = os.path.join(output_dir, 'imputers.joblib')
        joblib.dump(imputers, imputers_path)
        logger.info(f"Imputers saved to {imputers_path}")
        
        # Save best parameters
        params_path = os.path.join(output_dir, 'best_params.json')
        with open(params_path, 'w') as f:
            json.dump(best_params, f, indent=4)
        logger.info(f"Best parameters saved to {params_path}")

        # Save metrics
        metrics_path = os.path.join(output_dir, 'metrics.json')
        with open(metrics_path, 'w') as f:
            json.dump(final_metrics, f, indent=4)
        logger.info(f"Metrics saved to {metrics_path}")

        logger.info("✅ Model training script finished successfully!")

    except Exception as e:
        logger.error(f"An error occurred during model training: {e}", exc_info=True)
        
if __name__ == "__main__":
    main() 