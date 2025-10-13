"""
Configuration Management Module

Handles application configuration across different environments
with validation and type safety.
"""

import os
import yaml
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "public"
    pool_size: int = 20
    max_overflow: int = 30
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string"""
        return (
            f"postgresql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class ETLConfig:
    """ETL pipeline configuration"""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5
    enable_monitoring: bool = True
    data_quality_threshold: float = 0.95
    

@dataclass
class MonitoringConfig:
    """Monitoring and alerting configuration"""
    enable_prometheus: bool = True
    prometheus_port: int = 8000
    log_level: str = "INFO"
    alert_email: Optional[str] = None


@dataclass
class CacheConfig:
    """Caching configuration"""
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    cache_ttl: int = 3600


@dataclass
class AppConfig:
    """Main application configuration"""
    database: DatabaseConfig
    etl: ETLConfig
    monitoring: MonitoringConfig
    cache: CacheConfig
    environment: str = "development"
    debug: bool = False


class ConfigManager:
    """
    Configuration manager with environment-specific settings
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._get_default_config_path()
        self._config: Optional[AppConfig] = None
    
    def _get_default_config_path(self) -> str:
        """Get default configuration file based on environment"""
        env = os.getenv("ENVIRONMENT", "development")
        base_path = Path(__file__).parent.parent
        return str(base_path / "config" / f"{env}.yaml")
    
    def load_config(self) -> AppConfig:
        """Load and validate configuration from file"""
        if self._config is None:
            self._config = self._load_from_file()
        return self._config
    
    def _load_from_file(self) -> AppConfig:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as file:
                config_data = yaml.safe_load(file)
            
            # Override with environment variables
            config_data = self._override_with_env(config_data)
            return self._create_config_object(config_data)
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
    
    def _override_with_env(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Override configuration with environment variables"""
        # Database overrides
        if "DATABASE_URL" in os.environ:
            # Parse DATABASE_URL if provided
            db_url = os.environ["DATABASE_URL"]
            # Simple parsing - in production, you'd use sqlparse or similar
            config_data.setdefault("database", {})
        
        # Standard environment variable overrides
        env_mappings = {
            "DB_HOST": ["database", "host"],
            "DB_PORT": ["database", "port"],
            "DB_NAME": ["database", "database"],
            "DB_USER": ["database", "username"],
            "DB_PASSWORD": ["database", "password"],
            "LOG_LEVEL": ["monitoring", "log_level"],
            "REDIS_HOST": ["cache", "redis_host"],
            "REDIS_PORT": ["cache", "redis_port"],
        }
        
        for env_var, config_path in env_mappings.items():
            if env_var in os.environ:
                value = os.environ[env_var]
                # Convert port numbers to integers
                if "port" in config_path[-1].lower():
                    value = int(value)
                
                # Navigate nested dictionary
                target = config_data
                for key in config_path[:-1]:
                    target = target.setdefault(key, {})
                target[config_path[-1]] = value
        
        return config_data
    
    def _create_config_object(self, config_data: Dict[str, Any]) -> AppConfig:
        """Create typed configuration object from dictionary"""
        return AppConfig(
            database=DatabaseConfig(**config_data["database"]),
            etl=ETLConfig(**config_data.get("etl", {})),
            monitoring=MonitoringConfig(**config_data.get("monitoring", {})),
            cache=CacheConfig(**config_data.get("cache", {})),
            environment=config_data.get("environment", "development"),
            debug=config_data.get("debug", False)
        )

# Global configuration instance
config_manager = ConfigManager()

def get_config() -> AppConfig:
    """Get application configuration"""
    return config_manager.load_config()