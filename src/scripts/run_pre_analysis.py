import duckdb
from unidecode import unidecode
from datetime import datetime
import logging
import os
import sys
import requests
import time
import joblib
import numpy as np
import json
import pandas as pd

from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler


_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.pipelines.resources.config_loader import config
from src.scripts.run_scrapping_pipelines import escape_markdown


TELEGRAM_CONFIG = config.get_telegram_config()
TELEGRAM_BOT_TOKEN = TELEGRAM_CONFIG.get('bot_token')
TELEGRAM_CHAT_ID = TELEGRAM_CONFIG.get('chat_id')
SELIC_RATE_ANNUAL = 14.50 / 100  # Annual SELIC rate (14.50%)


def impute_lat_lon(df_to_impute, df_knowledge_base, logger):
    """
    Imputes missing lat/lon in a dataframe using means from a knowledge base dataframe.
    If imputing a dataframe on itself, pass it for both arguments.
    """
    logger.info(f"Starting lat/lon imputation. Nulls before: Lat={df_to_impute['latitude'].isnull().sum()}, Lon={df_to_impute['longitude'].isnull().sum()}")
    
    df = df_to_impute.copy()

    # 1. Calculate means from the knowledge base
    city_mean_lat = df_knowledge_base['latitude'].mean()
    city_mean_lon = df_knowledge_base['longitude'].mean()

    if not (pd.notna(city_mean_lat) and pd.notna(city_mean_lon)):
        logger.warning("Could not calculate city-wide mean coordinates from knowledge base. Skipping imputation.")
        df.dropna(subset=['latitude', 'longitude'], inplace=True)
        return df

    logger.info(f"Using KB for imputation. City-wide mean: Lat={city_mean_lat:.4f}, Lon={city_mean_lon:.4f}")
    bairro_lat_map = df_knowledge_base.groupby('bairro')['latitude'].mean()
    bairro_lon_map = df_knowledge_base.groupby('bairro')['longitude'].mean()
    
    # 2. Map bairro-specific means for rows with missing lat/lon
    lat_na_mask = df['latitude'].isna()
    if lat_na_mask.any():
        df.loc[lat_na_mask, 'latitude'] = df.loc[lat_na_mask, 'bairro'].map(bairro_lat_map)

    lon_na_mask = df['longitude'].isna()
    if lon_na_mask.any():
        df.loc[lon_na_mask, 'longitude'] = df.loc[lon_na_mask, 'bairro'].map(bairro_lon_map)
    
    # 3. Fallback to city-wide mean for any remaining NaNs
    df['latitude'].fillna(city_mean_lat, inplace=True)
    df['longitude'].fillna(city_mean_lon, inplace=True)

    # 4. Final drop for safety
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    
    logger.info(f"Lat/lon imputation complete. Nulls after: Lat={df['latitude'].isnull().sum()}, Lon={df['longitude'].isnull().sum()}")

    return df


def calculate_adjusted_roi(purchase_price, estimated_resale_value, logger):
    """
    Calculates a more realistic ROI by subtracting fixed and variable costs,
    including opportunity cost based on the SELIC rate.

    Args:
        purchase_price (float): The price paid for the property.
        estimated_resale_value (float): The estimated value the property will be sold for.
        logger (logging.Logger): The logger instance.

    Returns:
        float: The adjusted ROI as a percentage, or 0.0 if costs exceed profit.
    """
    try:
        # 1. Transactional Costs on Purchase
        documentation_cost = purchase_price * 0.055
        auctioneer_commission = purchase_price * 0.05
        legal_fee = 5000  # Imissão na posse
        other_costs = 300
        
        total_purchase_costs = documentation_cost + auctioneer_commission + legal_fee + other_costs
        total_investment = purchase_price + total_purchase_costs

        # 2. Transactional Costs on Resale
        resale_broker_commission = estimated_resale_value * 0.06 # Using 6% as a standard broker fee

        # 3. Opportunity Cost (over 10 months)
        selic_10_months = ((1 + SELIC_RATE_ANNUAL)**(10/12)) - 1
        opportunity_cost = total_investment * selic_10_months

        # 4. Net Profit Calculation
        gross_profit = estimated_resale_value - total_investment
        net_profit = gross_profit - resale_broker_commission - opportunity_cost
            
        adjusted_roi = (net_profit / total_investment) * 100
        
        return adjusted_roi

    except Exception as e:
        logger.error(f"Error in calculate_adjusted_roi: {e}", exc_info=True)
        return 0.0


def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("run_pre_analysis")
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


logger = setup_logging()


def load_latest_artifacts(logger):
    """Finds and loads the latest model, imputers, and other artifacts."""
    model_dir = os.path.join(_PROJECT_ROOT, "data", "model_training")
    if not os.path.exists(model_dir):
        logger.warning("Model training directory does not exist. Skipping model-based analysis.")
        return None, None, None
    
    subdirs = [d for d in os.listdir(model_dir) if os.path.isdir(os.path.join(model_dir, d))]
    if not subdirs:
        logger.warning("No trained models found in the model directory. Skipping model-based analysis.")
        return None, None, None
        
    latest_subdir_path = os.path.join(model_dir, sorted(subdirs, reverse=True)[0])
    
    model_path = os.path.join(latest_subdir_path, 'model_pipeline.joblib')
    imputers_path = os.path.join(latest_subdir_path, 'imputers.joblib')
    kmeans_path = os.path.join(latest_subdir_path, 'kmeans_model.joblib')

    if not all(os.path.exists(p) for p in [model_path, imputers_path, kmeans_path]):
        logger.error(f"One or more artifact files not found in {latest_subdir_path}. Skipping model-based analysis.")
        return None, None, None
    
    try:
        logger.info(f"Loading artifacts from {latest_subdir_path}...")
        model = joblib.load(model_path)
        imputers = joblib.load(imputers_path)
        kmeans = joblib.load(kmeans_path)
        logger.info("Model, imputers, and k-means loaded successfully.")
        return model, imputers, kmeans
    except Exception as e:
        logger.error(f"Failed to load artifacts: {e}", exc_info=True)
        return None, None, None


def build_knn_model(df_kb, logger):
    """Builds an in-memory K-Nearest Neighbors model for finding comparable properties."""
    logger.info("Building K-Nearest Neighbors 'comps' model...")
    try:
        # Define the core features for finding 'comparable' properties
        knn_features = ['tamanho', 'n_quartos', 'n_banheiros', 'n_garagem', 'latitude', 'longitude']
        
        # Ensure the KB has the required data and is clean
        df_kb_knn = df_kb.dropna(subset=knn_features + ['preco']).copy()

        if df_kb_knn.empty:
            logger.warning("Not enough data in knowledge base to build KNN model.")
            return None, None

        X_knn = df_kb_knn[knn_features]
        y_knn = df_kb_knn['preco']
        
        # A scaler is crucial for distance-based algorithms like KNN
        scaler = StandardScaler()
        X_knn_scaled = scaler.fit_transform(X_knn)
        
        # Initialize and "fit" the KNN model (it just stores the data)
        knn_model = KNeighborsRegressor(n_neighbors=7, n_jobs=-1)
        knn_model.fit(X_knn_scaled, y_knn)
        
        logger.info(f"KNN 'comps' model built successfully with {len(df_kb_knn)} properties.")
        return knn_model, scaler
    except Exception as e:
        logger.error(f"Failed to build KNN model: {e}", exc_info=True)
        return None, None


def impute_leilao_data(df, imputers, logger):
    """Imputes missing values in auction data using pre-trained imputers."""
    df_copy = df.copy()
    logger.info("Imputing missing values in auction data...")

    # Impute n_quartos and n_banheiros using the loaded Linear Regression models
    for col in ['n_quartos', 'n_banheiros']:
        if col not in df_copy.columns:
            logger.warning(f"Column '{col}' not found in auction data. Creating it for imputation.")
            df_copy[col] = np.nan
            
        imputer_model = imputers.get(f'{col}_imputer')
        if imputer_model:
            # Predict only for rows with missing values in the target column but valid 'tamanho'
            missing_mask = df_copy[col].isna() & df_copy['tamanho'].notna()
            if missing_mask.any():
                # The imputer model was trained on 'tamanho'
                features_for_imputation = df_copy.loc[missing_mask, ['tamanho']]
                predicted_values = imputer_model.predict(features_for_imputation)
                df_copy.loc[missing_mask, col] = np.round(predicted_values).clip(1)

    # Impute n_garagem using the saved median value
    if 'n_garagem' not in df_copy.columns:
        df_copy['n_garagem'] = np.nan
    garagem_imputer = imputers.get('n_garagem_imputer')
    if garagem_imputer is not None:
        df_copy['n_garagem'].fillna(garagem_imputer, inplace=True)

    # Fill any remaining NaNs after imputation (e.g., if area_util was NaN) with the median
    for col in ['n_quartos', 'n_banheiros', 'n_garagem']:
        if col in df_copy.columns and df_copy[col].isna().any():
            fallback_median = imputers.get('n_garagem_imputer', 1) # Use garage median or 1 as a fallback
            df_copy[col].fillna(fallback_median, inplace=True)
        # Ensure integer types
        df_copy[col] = df_copy[col].astype(int)
        
    logger.info("Auction data imputation complete.")
    return df_copy


def feature_engineer_leilao_data(df, kmeans_model, logger):
    """Applies feature engineering to auction data."""
    df_copy = df.copy()
    logger.info("Applying feature engineering to auction data...")

    # A. Geospatial Clustering (using the loaded K-Means model)
    if 'latitude' in df_copy.columns and 'longitude' in df_copy.columns:
        # Ensure no NaN values in coordinates before predicting
        coord_mask = df_copy[['latitude', 'longitude']].notna().all(axis=1)
        if coord_mask.any():
            df_copy.loc[coord_mask, 'bairro_cluster'] = kmeans_model.predict(df_copy.loc[coord_mask, ['latitude', 'longitude']])
            df_copy['bairro_cluster'] = df_copy['bairro_cluster'].astype('category')
            logger.info("Geospatial clusters assigned.")
    else:
        logger.warning("Latitude/Longitude not available, skipping K-Means clustering.")

    # B. Distance to Points of Interest
    logger.info("Calculating distances to points of interest for auction data...")
    poi_path = os.path.join(_PROJECT_ROOT, "data", "model_training", "points_of_interest.json")
    try:
        with open(poi_path, 'r', encoding='utf-8') as f:
            points_of_interest = json.load(f)
        
        valid_pois = [p for p in points_of_interest if p.get('latitude') is not None and p.get('longitude') is not None]
        logger.info(f"Loaded {len(valid_pois)} valid POIs for distance calculation.")

        if 'latitude' in df_copy.columns and 'longitude' in df_copy.columns:
            for poi in valid_pois:
                point_name = poi['point_name']
                sanitized_name = ''.join(e for e in point_name if e.isalnum() or e.isspace()).replace(' ', '_').lower()
                feature_name = f"dist_{sanitized_name}"
                
                poi_lat, poi_lon = poi['latitude'], poi['longitude']
                
                df_copy[feature_name] = np.sqrt(
                    (df_copy['latitude'] - poi_lat)**2 + (df_copy['longitude'] - poi_lon)**2
                )
            logger.info(f"Created {len(valid_pois)} distance features for auction data.")
        else:
            logger.warning("Latitude/Longitude not available in auction data, skipping POI distance calculation.")

    except Exception as e:
        logger.error(f"Could not calculate POI distances for auction data: {e}. Some features will be missing.")

    # C. Interaction and Ratio Features
    if 'tamanho' in df_copy.columns and 'n_quartos' in df_copy.columns and 'n_banheiros' in df_copy.columns:
        epsilon = 1e-6
        df_copy['tamanho_por_quarto'] = df_copy['tamanho'] / (df_copy['n_quartos'] + epsilon)
        df_copy['banheiros_por_quarto'] = df_copy['n_banheiros'] / (df_copy['n_quartos'] + epsilon)
        logger.info("Interaction features created.")
    else:
        logger.warning("Missing columns for interaction features, skipping.")

    return df_copy


def send_telegram_message(message):
    """Sends a message to a Telegram chat using the requests library."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram bot token or chat ID is not configured. Cannot send message.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }

    try:
        response = requests.post(url, data=payload, timeout=20)
        response.raise_for_status()
        logger.info(f"Telegram notification sent (first 100 chars): {message[:100]}...")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Telegram message: {e}")


def kb_data_cleaning(df):
    """
    Clean and preprocess the knowledge base dataframe.
    """
    df = df.drop_duplicates(subset=["id"])
    df = df.loc[(df["n_quartos"] > 0) & (df["n_quartos"] < 10)]
    df = df.loc[(df["n_banheiros"] > 0) & (df["n_banheiros"] < 10)]
    df = df.loc[(df["tamanho"] > 10) & (df["tamanho"] < 1000)]
    df = df.loc[(df["preco"] > 10_000) & (df["preco"] < 10_000_000)]
    df = df.loc[df["bairro"].notna()]
    df["bairro"] = df["bairro"].apply(lambda x: unidecode(x.lower().strip()))
    df["preco_m2"] = df["preco"] / df["tamanho"]
    df = df.loc[df["preco_m2"] < 30_000]
    return df


def leilao_data_cleaning(df):
    """
    Clean and preprocess the auction dataframe based on the new schema.
    """
    df_leilao = df.copy()
    df_leilao = df_leilao.applymap(lambda x: None if str(x) == "<NA>" else x)
    df_leilao = df_leilao.drop_duplicates(subset=["id"])

    # Just maintain properties that acept financing (or are not available)
    df_leilao["aceita_financiamento"] = df_leilao["aceita_financiamento"].apply(str).fillna("N/A")
    df_leilao = df_leilao.loc[(df_leilao["aceita_financiamento"] == "True") | (df_leilao["aceita_financiamento"] == "N/A")]

    # Rename columns to match the historical schema used across the script
    df_leilao = df_leilao.rename(columns={
        'preco_primeira_praca': 'preco',
        'area_util': 'tamanho',
        'link_detalhes': 'url'
    })

    # Ensure essential numeric columns are correctly typed, coercing errors to NaN
    for col in ['preco', 'tamanho', 'preco_atual']:
        if col in df_leilao.columns:
            df_leilao[col] = pd.to_numeric(df_leilao[col], errors='coerce')
    
    # Drop rows that are unusable for analysis due to missing core information
    df_leilao.dropna(subset=['preco', 'tamanho', 'bairro', 'preco_atual'], inplace=True)

    # Calculate price per square meter
    df_leilao['preco_m2'] = df_leilao['preco'] / df_leilao['tamanho']

    # Standardize neighborhood names
    df_leilao["bairro"] = df_leilao["bairro"].apply(lambda x: unidecode(str(x).lower().strip()))
    return df_leilao


def main() -> None:
    logger.info("🚀 Starting pre-analysis script...")
    con = None
    try:
        # Database connection
        con = duckdb.connect(database=os.path.join(_PROJECT_ROOT, "db", "kodomiya.duckdb"), read_only=False)
        
        # Select all columns needed for knowledge base
        kb_querys = [
            "SELECT preco, tamanho, n_quartos, n_banheiros, n_garagem, bairro, latitude, longitude, id FROM kodomiya_chaves_na_mao.chaves_na_mao_register",
            "SELECT preco, tamanho, n_quartos, n_banheiros, n_garagem, bairro, latitude, longitude, id FROM kodomiya_viva_real.viva_real_register",
            "SELECT preco, tamanho, n_quartos, n_banheiros, n_garagem, bairro, latitude, longitude, id FROM kodomiya_zap_imoveis.zap_imoveis_register"
        ]

        # Fetch knowledge base data
        df_kb_list = [con.execute(query).fetch_df() for query in kb_querys]

        # Concatenate knowledge base data
        df_kb = pd.concat(df_kb_list, ignore_index=True)
        
        # Fetch leilao data
        today = datetime.now().strftime("%Y-%m-%d")
        df_leilao = con.execute(f"SELECT * FROM kodomiya_leilao_imovel.leilao_imovel_register WHERE data_primeira_praca	> '{today}' OR data_segunda_praca > '{today}'").fetchdf()
        
        logger.info(f"Loaded {len(df_kb)} properties from knowledge base and {len(df_leilao)} open auctions.")

        if df_leilao.empty:
            logger.info("No open auctions to analyze. Exiting.")
            return

        # Data Cleaning
        df_kb = kb_data_cleaning(df_kb)
        df_leilao = leilao_data_cleaning(df_leilao)
        
        # Impute Lat/Lon for both dataframes using the knowledge base as the source of truth
        logger.info("--- Imputing coordinates for Knowledge Base ---")
        df_kb = impute_lat_lon(df_kb, df_kb, logger) # Impute KB on itself
        logger.info("--- Imputing coordinates for Auction Data ---")
        df_leilao = impute_lat_lon(df_leilao, df_kb, logger) # Impute auction data using KB stats

        if df_leilao.empty:
            logger.info("No open auctions remain after data cleaning and imputation. Exiting.")
            return
            
        # Load model and other artifacts
        model, imputers, kmeans_model = load_latest_artifacts(logger)
        
        # Build KNN 'comps' model from knowledge base
        knn_model, knn_scaler = build_knn_model(df_kb, logger)

        # A. Z-Score Based Analysis (Undervalued Properties)
        # ----------------------------------------------------------------
        logger.info("Starting Z-Score based analysis...")
        df_leilao_zscore = df_leilao.copy()
        
        neighborhood_stats = df_kb.groupby('bairro').agg(
            mean_preco_m2=('preco_m2', 'mean'),
            std_preco_m2=('preco_m2', 'std')
        ).reset_index()

        df_leilao_zscore = df_leilao_zscore.merge(neighborhood_stats, on='bairro', how='left')
        
        # Create a flag for when we fall back to city-wide stats
        df_leilao_zscore['z_score_fallback'] = df_leilao_zscore['mean_preco_m2'].isnull()
        
        # Get city-wide stats for fallback
        city_mean_preco_m2 = df_kb['preco_m2'].mean()
        city_std_preco_m2 = df_kb['preco_m2'].std()

        # Log if fallback is happening
        if df_leilao_zscore['z_score_fallback'].any():
            num_fallbacks = df_leilao_zscore['z_score_fallback'].sum()
            logger.info(f"{num_fallbacks} properties missing neighborhood stats, using city-wide fallback...")
            logger.info(f"Fallback stats: Mean m² Price=${city_mean_preco_m2:.2f}, Std Dev=${city_std_preco_m2:.2f}")
        
        # Apply fallback for mean and std
        df_leilao_zscore['mean_preco_m2'].fillna(city_mean_preco_m2, inplace=True)
        df_leilao_zscore['std_preco_m2'].fillna(city_std_preco_m2, inplace=True)
            
        # Also, replace std of 0 (which gives infinite z-score) with city-wide std
        # This can happen if a neighborhood has only one listing in the KB
        df_leilao_zscore.loc[df_leilao_zscore['std_preco_m2'] == 0, 'std_preco_m2'] = city_std_preco_m2

        df_leilao_zscore['z_score'] = (
            (df_leilao_zscore['preco_m2'] - df_leilao_zscore['mean_preco_m2']) / 
             df_leilao_zscore['std_preco_m2']
        )
        
        undervalued_properties = df_leilao_zscore.copy()
        undervalued_properties.loc[:, 'z_score_rank'] = undervalued_properties['z_score'].rank(ascending=True)
        undervalued_properties.loc[:, 'estimated_market_value_zscore'] = undervalued_properties['mean_preco_m2'] * undervalued_properties['tamanho']

        undervalued_properties['z_score_roi'] = undervalued_properties.apply(
            lambda row: calculate_adjusted_roi(row['preco_atual'], row['estimated_market_value_zscore'], logger),
            axis=1
        )
        
        logger.info(f"Found {len(undervalued_properties)} potentially undervalued properties (Z-score).")
        
        all_opportunities = undervalued_properties[['id', 'preco_atual', 'bairro', 'tamanho', 'url', 'rua', 'z_score', 'z_score_rank', 'z_score_roi', 'z_score_fallback']].copy()

        # B. Regression Model Analysis
        # ----------------------------------------------------------------
        if model is not None and imputers is not None and kmeans_model is not None:
            logger.info("Starting regression model-based analysis...")
            df_leilao_model = df_leilao.copy()
            
            df_leilao_model = impute_leilao_data(df_leilao_model, imputers, logger)
            df_leilao_model = feature_engineer_leilao_data(df_leilao_model, kmeans_model, logger)

            model_features = model.feature_names_in_            
            df_leilao_model['predicted_price'] = model.predict(df_leilao_model[model_features])

            df_leilao_model['model_roi'] = df_leilao_model.apply(
                lambda row: calculate_adjusted_roi(row['preco_atual'], row['predicted_price'], logger),
                axis=1
            )

            df_leilao_model.loc[:, 'regression_model_rank'] = df_leilao_model['model_roi'].rank(ascending=False)

            logger.info(f"Found {len(df_leilao_model)} potentially undervalued properties (Regression Model).")
            
            model_results = df_leilao_model[['id', 'model_roi', 'regression_model_rank']].copy()
            all_opportunities = all_opportunities.merge(model_results, on='id', how='outer')
        else:
            logger.warning("Skipping regression analysis as model artifacts are not loaded.")

        # C. K-Nearest Neighbors ('Comps') Analysis
        # ----------------------------------------------------------------
        if knn_model is not None and knn_scaler is not None and imputers is not None:
            logger.info("Starting K-Nearest Neighbors 'comps' analysis...")
            df_leilao_knn = df_leilao.copy()
            
            knn_features = ['tamanho', 'n_quartos', 'n_banheiros', 'n_garagem', 'latitude', 'longitude']
            
            df_leilao_knn_imputed = impute_leilao_data(df_leilao_knn, imputers, logger)

                
            X_leilao_knn = df_leilao_knn_imputed[knn_features]
            X_leilao_knn_scaled = knn_scaler.transform(X_leilao_knn)

            df_leilao_knn_imputed['knn_predicted_price'] = knn_model.predict(X_leilao_knn_scaled)
            
            df_leilao_knn_imputed['knn_roi'] = df_leilao_knn_imputed.apply(
                lambda row: calculate_adjusted_roi(row['preco_atual'], row['knn_predicted_price'], logger),
                axis=1
            )
            
            df_leilao_knn_imputed.loc[:, 'knn_rank'] = df_leilao_knn_imputed['knn_roi'].rank(ascending=False)
            
            logger.info(f"Found {len(df_leilao_knn_imputed)} potentially undervalued properties (KNN 'Comps').")

            knn_results = df_leilao_knn_imputed[['id', 'knn_roi', 'knn_rank']].copy()
            all_opportunities = all_opportunities.merge(knn_results, on='id', how='outer')
        
        else:
            logger.warning("Skipping KNN 'comps' analysis as model is not built.")
            
        # D. Final Score Calculation & Ranking
        # ----------------------------------------------------------------
        if all_opportunities.empty:
            logger.info("No potentially undervalued properties found by any method.")
            return
            
        # Re-populate descriptive data for all opportunities to fix NaNs from outer merges
        analysis_cols = ['id', 'z_score', 'z_score_rank', 'z_score_roi', 'z_score_fallback', 'model_roi', 'regression_model_rank', 'knn_roi', 'knn_rank']
        detail_cols = ['preco_atual', 'bairro', 'tamanho', 'url', 'rua', 'descricao']
        
        analysis_cols_present = [c for c in analysis_cols if c in all_opportunities.columns]
        detail_cols_present = ['id'] + [c for c in detail_cols if c in df_leilao.columns]
        
        all_opportunities = all_opportunities[analysis_cols_present].merge(df_leilao[detail_cols_present], on='id', how='left')
            
        all_opportunities['z_score_rank'].fillna(999, inplace=True)
        all_opportunities['regression_model_rank'].fillna(999, inplace=True)
        all_opportunities['knn_rank'].fillna(999, inplace=True)
        all_opportunities.fillna({'z_score_roi': 0, 'model_roi': 0, 'knn_roi': 0, 'z_score_fallback': False}, inplace=True)

        all_opportunities['final_score'] = (
            all_opportunities['z_score_rank'] + 
            all_opportunities['regression_model_rank'] +
            all_opportunities['knn_rank']
        ).astype(int)
        
        final_opportunities = all_opportunities[
            (all_opportunities['z_score_rank'] != 999) | 
            (all_opportunities['regression_model_rank'] != 999) |
            (all_opportunities['knn_rank'] != 999)
        ].sort_values(by='final_score', ascending=True)

        logger.info(f"Generated a final ranked list of {len(final_opportunities)} properties.")
        if final_opportunities.empty:
            logger.info("No promising opportunities found after final analysis. Exiting.")
            return

        # Filter step (Only maintain properties on wich at least one of the methods ROI's are > SELIC_RATE_ANNUAL + 5%)
        final_opportunities = final_opportunities.loc[
            (final_opportunities['z_score_roi'] > (SELIC_RATE_ANNUAL * 100) + 5) |
            (final_opportunities['model_roi'] > (SELIC_RATE_ANNUAL * 100) + 5) |
            (final_opportunities['knn_roi'] > (SELIC_RATE_ANNUAL * 100) + 5)
        ]
        
        # E. Send Telegram Notifications
        # ----------------------------------------------------------------
        logger.info("Sending Telegram notifications for top properties...")
        for _, row in final_opportunities.iterrows():
            try:
                z_score_rank_text = str(int(row['z_score_rank'])) if row['z_score_rank'] != 999 else "N/A"
                if row.get('z_score_fallback') and z_score_rank_text != "N/A":
                    z_score_rank_text += " (Fallback)"

                model_rank_text = str(int(row['regression_model_rank'])) if row['regression_model_rank'] != 999 else "N/A"
                knn_rank_text = str(int(row['knn_rank'])) if row['knn_rank'] != 999 else "N/A"

                zscore_roi_text, model_roi_text, knn_roi_text = "", "", ""
                mean_roi_values = []
                
                if row['z_score_roi']:
                    fallback_text = " (Fallback)" if row.get('z_score_fallback') else ""
                    zscore_roi_text = f"- ROI (Z-Score{fallback_text}): {row['z_score_roi']:.2f}%\n"
                    mean_roi_values.append(row['z_score_roi'])
                    
                if row['model_roi']:
                    model_roi_text = f"- ROI (Modelo ML): {row['model_roi']:.2f}%\n"
                    mean_roi_values.append(row['model_roi'])
                    
                if row['knn_roi']:
                    knn_roi_text = f"- ROI (KNN Comps): {row['knn_roi']:.2f}%\n"
                    mean_roi_values.append(row['knn_roi'])

                mean_roi = np.mean(mean_roi_values) if mean_roi_values else 0.0

                message = (
                    f"🚨 NOVA OPORTUNIDADE DE LEILÃO DETECTADA 🚨\n\n"
                    f"Score Final: {row['final_score']}\n"
                    f"ROI Médio (Ajustado): {mean_roi:.2f}%\n\n"
                    f"--- DETALHES ---\n"
                    f"ID do Imóvel: {escape_markdown(str(row['id']))}\n"
                    f"Rua: {escape_markdown(str(row['rua']))}\n"
                    f"Bairro: {escape_markdown(row['bairro'])}\n"
                    f"Tamanho: {row['tamanho']:.0f} m²\n"
                    f"Lance mínimo: R$ {row['preco_atual']:,.2f}\n\n"
                    f"--- ANÁLISE ---\n"
                    f"{zscore_roi_text}"
                    f"{model_roi_text}"
                    f"{knn_roi_text}\n"
                    f"Link: [Clique aqui para ver]({row['url']})\n\n"
                    f"Ranking Z-Score: {z_score_rank_text}\n"
                    f"Ranking Modelo ML: {model_rank_text}\n"
                    f"Ranking KNN Comps: {knn_rank_text}\n"
                )
                
                send_telegram_message(message)
                time.sleep(2)
            except Exception as e:
                logger.error(f"Failed to generate/send message for property {row.get('id', 'N/A')}: {e}", exc_info=True)
                continue
    
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main execution: {e}", exc_info=True)
        send_telegram_message(f"🚨 The pre-analysis script failed with error: {escape_markdown(str(e))}")
    
    
    finally:
        if con:
            con.close()
            logger.info("Database connection closed.")
        logger.info("✅ Pre-analysis script finished.")


if __name__ == "__main__":
    main()