
from pathlib import Path


# ========================================
# 1. Project Paths
# ========================================

ROOT_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = ROOT_DIR / "data" / "week12"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

LOG_DIR = ROOT_DIR / "logs" / "week12"
RESULTS_DIR = ROOT_DIR / "week12" / "results"


# ========================================
# 2. Dataset Configuration
# ========================================

ORDER_COUNT = 1_000_000
CUSTOMER_COUNT = 50_000
PRODUCT_COUNT = 5_000

START_DATE = "2024-01-01"

# Deterministic dataset generation.
# Same parameters -> same logical records.
DATA_SEED = 42


# ========================================
# 3. Spark Configuration
# ========================================

SPARK_MASTER = "local[4]"

SHUFFLE_PARTITIONS = 8

GENERATE_PARTITIONS = 8

APP_NAME = "Week12LargeDataGeneration"


# ========================================
# 4. Dataset Paths
# ========================================

ORDERS_PATH = BRONZE_DIR / "orders"

CUSTOMERS_PATH = BRONZE_DIR / "customers"

PRODUCTS_PATH = BRONZE_DIR / "products"

PIPELINE_CONFIG = {
    "join_strategy": "broadcast",
    "shuffle_partitions": 8,
    "compression": "snappy",
    "run_validation": True,
    "run_consistency_test": True
}

# ========================================
# 5. Create Directories
# ========================================

def create_directories():
    directories = [
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        LOG_DIR
    ]

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True
        )
