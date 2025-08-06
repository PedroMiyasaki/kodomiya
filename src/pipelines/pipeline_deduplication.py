import duckdb
import os
import sys
import logging

# Add project root to sys.path for import resolution
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.pipelines.resources.config_loader import config

def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("deduplication_pipeline")
    LOGGING_CONFIG = config.get_logging_config()
    logger.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    formatter = logging.Formatter(LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger

def deduplicate_properties():
    """
    Removes properties from viva_real, zap_imoveis, and chaves_na_mao
    that are also found in leilao_imovel.
    """
    logger = setup_logging()
    logger.info("Starting deduplication pipeline...")

    DATABASE_CONFIG = config.get_database_config()
    db_path = os.path.join(DATABASE_CONFIG['path'], "kodomiya.duckdb")
    logger.info(f"Connecting to database at: {db_path}")

    try:
        con = duckdb.connect(database=db_path, read_only=False)
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB database: {e}")
        return

    tables_to_clean = {
        'viva_real': ('viva_real_register', 'viva_real_history', 'kodomiya_viva_real'),
        'zap_imoveis': ('zap_imoveis_register', 'zap_imoveis_history', 'kodomiya_zap_imoveis'),
        'chaves_na_mao': ('chaves_na_mao_register', 'chaves_na_mao_history', 'kodomiya_chaves_na_mao')
    }
    leilao_table = 'kodomiya_leilao_imovel.leilao_imovel_register'

    try:
        # Check if leilao table exists
        con.execute(f"SELECT 1 FROM {leilao_table} LIMIT 1")
    except duckdb.CatalogException:
        logger.warning(f"Auction table '{leilao_table}' not found. Skipping deduplication.")
        con.close()
        return

    for source, (register_table_name, history_table_name, schema_name) in tables_to_clean.items():
        register_table_full_name = f"{schema_name}.{register_table_name}"
        history_table_full_name = f"{schema_name}.{history_table_name}"
        
        try:
            # Check if source tables exist before proceeding
            con.execute(f"SELECT 1 FROM {register_table_full_name} LIMIT 1")
            logger.info(f"Processing source: {source}")
        except duckdb.CatalogException:
            logger.warning(f"Table '{register_table_full_name}' not found for source '{source}'. Skipping.")
            continue

        # Rule 1: Match on street, neighborhood, city and price
        query1 = f"""
        SELECT t1.id
        FROM {register_table_full_name} AS t1
        JOIN {leilao_table} AS t2 ON
            LOWER(TRIM(t1.rua)) = LOWER(TRIM(t2.rua)) AND
            LOWER(TRIM(t1.bairro)) = LOWER(TRIM(t2.bairro)) AND
            LOWER(TRIM(t1.cidade)) = LOWER(TRIM(t2.cidade)) AND
            (t1.preco = t2.preco_primeira_praca OR t1.preco = t2.preco_segunda_praca)
        """

        # Rule 2: Match on street, city and price
        query2 = f"""
        SELECT t1.id
        FROM {register_table_full_name} AS t1
        JOIN {leilao_table} AS t2 ON
            LOWER(TRIM(t1.rua)) = LOWER(TRIM(t2.rua)) AND
            LOWER(TRIM(t1.cidade)) = LOWER(TRIM(t2.cidade)) AND
            (t1.preco = t2.preco_primeira_praca OR t1.preco = t2.preco_segunda_praca)
        """
        
        ids_to_delete_query = f"{query1} UNION {query2}"
        
        try:
            logger.info(f"Querying for duplicate IDs in '{source}'...")
            ids_df = con.execute(ids_to_delete_query).df()
            
            if ids_df.empty:
                logger.info(f"No duplicate entries found for '{source}'.")
                continue

            ids_to_delete = tuple(ids_df['id'].unique())
            logger.info(f"Found {len(ids_to_delete)} duplicate propertie(s) in '{source}'.")

            if ids_to_delete:
                placeholder = ', '.join(['?'] * len(ids_to_delete))

                # Begin transaction
                con.begin()

                # Delete from register table
                delete_register_query = f"DELETE FROM {register_table_full_name} WHERE id IN ({placeholder})"
                con.execute(delete_register_query, list(ids_to_delete))
                logger.info(f"Deleted {con.fetchnone()} rows from {register_table_full_name}")

                # Delete from history table
                delete_history_query = f"DELETE FROM {history_table_full_name} WHERE id IN ({placeholder})"
                con.execute(delete_history_query, list(ids_to_delete))
                logger.info(f"Deleted {con.fetchnone()} rows from {history_table_full_name}")

                # Commit transaction
                con.commit()
                logger.info(f"Successfully deleted entries for '{source}'.")

        except Exception as e:
            logger.error(f"An error occurred while processing '{source}': {e}")
            con.rollback()
            continue

    con.close()
    logger.info("Deduplication pipeline finished.")

if __name__ == "__main__":
    deduplicate_properties() 