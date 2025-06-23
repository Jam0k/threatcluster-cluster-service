import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables
load_dotenv()

class Settings:
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent.parent
        self.config_dir = self.base_dir / "config"
        
        # Database Configuration
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME'),
            'username': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'sslmode': os.getenv('DB_SSLMODE', 'require')
        }
        
        # Validate required database settings
        required_db_fields = ['host', 'database', 'username', 'password']
        missing_fields = [field for field in required_db_fields if not self.db_config[field]]
        if missing_fields:
            raise ValueError(f"Missing required database configuration: {missing_fields}")
        
        # Load YAML configurations
        self.app_config = self._load_yaml('config.yaml')
        self.keywords_config = self._load_yaml('keywords.yaml')
        
        # Application Settings
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.log_file = os.getenv('LOG_FILE')
        self.environment = os.getenv('ENVIRONMENT', 'production')
        
        # Cache directories (must be set in .env)
        self.transformers_cache = os.getenv('TRANSFORMERS_CACHE')
        self.hf_home = os.getenv('HF_HOME')
        
        # Validate required settings
        if not self.log_file:
            raise ValueError("LOG_FILE must be set in .env")
        if not self.transformers_cache:
            raise ValueError("TRANSFORMERS_CACHE must be set in .env")
        if not self.hf_home:
            raise ValueError("HF_HOME must be set in .env")
        
        # Create cache directories if they don't exist
        self._setup_cache_directories()
    
    def _setup_cache_directories(self):
        """Create cache directories if they don't exist"""
        cache_dirs = [self.transformers_cache, self.hf_home]
        
        for cache_dir in cache_dirs:
            cache_path = Path(cache_dir)
            try:
                cache_path.mkdir(parents=True, exist_ok=True)
            except (PermissionError, OSError) as e:
                raise PermissionError(f"Cannot create cache directory {cache_dir}: {e}")
    
    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        config_path = self.config_dir / filename
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    @property
    def database_url(self) -> str:
        """Build PostgreSQL connection string"""
        return (
            f"postgresql://{self.db_config['username']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}"
            f"/{self.db_config['database']}?sslmode={self.db_config['sslmode']}"
        )

# Global settings instance
settings = Settings()