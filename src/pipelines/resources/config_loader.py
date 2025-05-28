import os
import yaml
from typing import Dict, Any, Optional

class ConfigLoader:
    """
    A class for loading and accessing configuration from the YAML file.
    """
    _instance = None
    _config = None

    def __new__(cls):
        """Singleton pattern to ensure only one instance of ConfigLoader exists."""
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self) -> None:
        """Load the configuration from the YAML file."""
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                                  "configs", "config.yml")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                self._config = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")

    def get_source_config(self, source_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific source.
        
        Args:
            source_name: Name of the source (e.g., 'zap_imoveis', 'viva_real', 'chaves_na_mao')
            
        Returns:
            Dict containing the source configuration
        """
        if not self._config or 'sources' not in self._config:
            raise ValueError("Configuration not loaded or invalid")
        
        if source_name not in self._config['sources']:
            raise ValueError(f"Source '{source_name}' not found in configuration")
        
        return self._config['sources'][source_name]
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        if not self._config or 'database' not in self._config:
            raise ValueError("Database configuration not found")
        
        return self._config['database']
    
    def get_geocoding_config(self) -> Dict[str, Any]:
        """Get geocoding configuration."""
        if not self._config or 'geocoding' not in self._config:
            raise ValueError("Geocoding configuration not found")
        
        return self._config['geocoding']
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        if not self._config or 'logging' not in self._config:
            raise ValueError("Logging configuration not found")
        
        return self._config['logging']
    
    def get_scraper_settings(self) -> Dict[str, Any]:
        """Get scraper settings configuration."""
        if not self._config or 'scraper_settings' not in self._config:
            raise ValueError("Scraper settings configuration not found")
        
        return self._config['scraper_settings']
    
    def get_config_value(self, *keys: str, default: Optional[Any] = None) -> Any:
        """
        Get a specific configuration value using a chain of keys.
        
        Args:
            *keys: Chain of keys to access nested config values
            default: Default value to return if keys not found
            
        Returns:
            The configuration value or the default
        """
        value = self._config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

# Create a singleton instance for easy import
config = ConfigLoader() 