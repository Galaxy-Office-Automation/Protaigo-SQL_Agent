"""
SQL Query Optimizer Agent - Configuration
Loads credentials from .env file for security.
"""

import os
from pathlib import Path

# Build the path to the .env file in the current directory
env_path = Path(__file__).parent / '.env'

# Check if the .env file exists before attempting to read it
if env_path.exists():
    # Open and read the .env file
    with open(env_path) as f:
        for line in f:
            # Remove leading/trailing whitespace
            line = line.strip()
            # Skip empty lines and comments (starting with '#')
            if line and not line.startswith('#') and '=' in line:
                # Split the line into key and value at the first '='
                key, value = line.split('=', 1)
                # Set the environment variable only if it isn't already set
                os.environ.setdefault(key.strip(), value.strip())

# Dictionary containing database connection parameters
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),      # Database host, defaults to localhost
    "port": int(os.getenv("DB_PORT", "5432")),      # Database port, defaults to 5432
    "database": os.getenv("DB_NAME", "app_db"),     # Database name, defaults to app_db
    "user": os.getenv("DB_USER", "app_user"),       # Database user, defaults to app_user
    "password": os.getenv("DB_PASSWORD", "")        # Database password, defaults to empty string
}

# Dictionary containing Language Model API configuration
LLM_CONFIG = {
    "api_base_url": os.getenv("LLM_API_BASE_URL", ""),       # Base URL for the LLM API
    "api_key": os.getenv("LLM_API_KEY", ""),                 # Secret API key for authentication
    "model": os.getenv("LLM_MODEL", "gpt-4o-mini"),          # The specific model to use (default: gpt-4o-mini)
    "temperature": float(os.getenv("LLM_TEMPERATURE", "0.1")),# Controls randomness (0.1 = very deterministic)
    "max_tokens": int(os.getenv("LLM_MAX_TOKENS", "4096"))   # Maximum number of tokens in the response
}

# Agent optimization behavior settings
OPTIMIZATION_CONFIG = {
    "max_suggestions": 10,       # Limit the number of optimization suggestions
    "explain_timeout": 30,       # Timeout for EXPLAIN ANALYZE queries in seconds
    "validate_output": True      # Whether to validate if optimized query is equivalent
}
