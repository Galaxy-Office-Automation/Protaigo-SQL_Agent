"""
SQL Query Optimizer Agent - Configuration
Loads credentials from .env file for security.
"""

import os
from pathlib import Path

# Load .env file
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip())

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "app_db"),
    "user": os.getenv("DB_USER", "app_user"),
    "password": os.getenv("DB_PASSWORD", "")
}

# LLM Configuration
LLM_CONFIG = {
    "api_base_url": os.getenv("LLM_API_BASE_URL", ""),
    "api_key": os.getenv("LLM_API_KEY", ""),
    "model": os.getenv("LLM_MODEL", "gpt-4o-mini"),
    "temperature": float(os.getenv("LLM_TEMPERATURE", "0.1")),
    "max_tokens": int(os.getenv("LLM_MAX_TOKENS", "4096"))
}

# Optimization Settings
OPTIMIZATION_CONFIG = {
    "max_suggestions": 10,
    "explain_timeout": 30,
    "validate_output": True
}
