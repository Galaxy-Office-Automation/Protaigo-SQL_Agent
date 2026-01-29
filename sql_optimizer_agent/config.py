"""
SQL Query Optimizer Agent - Configuration
"""

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}

# LLM Configuration
LLM_CONFIG = {
    "api_base_url": "http://10.10.90.94:2026/v1",
    "api_key": "4c8c56fede640bf281a7e36128fef8c18ad0b5f97a5a6bbb2e41e29d4f5a895d",
    "model": "gpt-4o-mini",
    "temperature": 0.1,
    "max_tokens": 4096
}

# Optimization Settings
OPTIMIZATION_CONFIG = {
    "max_suggestions": 10,
    "explain_timeout": 30,  # seconds
    "validate_output": True
}
