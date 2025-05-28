# KodoMiya (Trading Properties)

A real estate analysis tool that scrapes property listings from Brazilian real estate websites, stores the data in a DuckDB database, and performs clustering analysis to identify patterns and potential investment opportunities.

## Project Overview

KodoMiya is a data pipeline and analytics project that:

1. Scrapes property listings from multiple Brazilian real estate websites (Zap Imóveis, Viva Real, Chaves na Mão)
2. Processes and stores the data in a DuckDB database
3. Performs clustering analysis to group similar properties
4. Identifies properties that may be undervalued based on statistical analysis

## Features

- **Data Collection**: Web scrapers for multiple real estate websites
- **Data Storage**: DuckDB for efficient storage and querying
- **Data Analysis**: 
  - K-means clustering to group similar properties
  - Statistical analysis to identify potential investment opportunities
  - Properties are clustered by physical characteristics (size, rooms, bathrooms, parking spaces)
- **Visualization**: Power BI dashboard for data exploration
- **Easy Maintenance**: Centralized configuration for all web scrapers

## Project Structure

- **src/**: Main source code
  - **main.py**: Entry point for running pipelines
  - **pipelines/**: Data pipeline modules for each source (e.g., `pipeline_viva_real.py`, `pipeline_zap_imoveis.py`)
  - **scripts/**: Analysis scripts
  - **llm/**: Modules related to LLM integration (if any)
- **configs/**: Configuration files
  - **config.yml**: Main configuration file with all pipeline settings
- **data/**: Data storage
  - **raw/**: Raw data from scrapers
  - **processed/**: Processed data after analysis
- **logs/**: Log files
  - **intraday/**: Logs from daily runs
- **analytics/**: Output files for analysis
  - Excel files with clustering results
  - Power BI dashboard
- **db/**: Database files
  - **kodomiya.duckdb**: DuckDB database (actual name may vary based on config)
- **tests/**: Test files

## Configuration System

The project now uses a centralized configuration system based on a YAML file:

- All website scraping parameters are defined in `configs/config.yml`
- HTML selectors, tags, and attributes are configurable per source
- This makes it easy to update the system when websites change their HTML structure
- Database and geocoding settings are also configurable

For detailed information about the configuration system, see the [Configuration Guide](configs/README.md).

## Data Analysis Methodology

The project performs clustering analysis at two levels:
1. **General clustering**: Groups similar properties across the entire dataset
2. **Neighborhood-specific clustering**: Groups similar properties within each neighborhood

For each property, the system:
- Calculates the mean price within each cluster
- Determines the percentage difference between the property's price and the cluster mean
- Calculates z-scores to identify statistically significant price deviations

## Requirements

The project requires the following dependencies (see `requirements.txt`):

- Python 3.x
- dlt (Data Loading Tool)
- pandas
- numpy
- scikit-learn
- duckdb
- beautifulsoup4
- geopy
- openpyxl
- requests
- pyyaml
- scipy
- matplotlib
- ipykernel

## How to Use

1. Clone the repository:
   ```bash
   git clone https://github.com/PedroMiyasaki/kodomiya.git
   cd kodomiya
   ```
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Run the pipelines using the main entry point:
   ```
   # Run all scraping pipelines
   python src/main.py --pipeline scraping --source all
   
   # Run specific scraper
   python src/main.py --pipeline scraping --source zap_imoveis
   
   # Run clustering analysis
   python src/main.py --pipeline clustering
   ```
4. View the results in the analytics folder
5. Open the Power BI dashboard to visualize the data

## Maintaining the Scrapers

When a website changes its HTML structure:

1. Inspect the website to identify the new HTML structure.
2. Update the corresponding sections in `configs/config.yml`.
3. No code changes are required in most cases.

## License

This project is for personal use and was created approximately 2 years ago.

## Author

Pedro Miyasaki 