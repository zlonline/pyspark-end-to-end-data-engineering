
import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from week12.config import (
    ROOT_DIR,
    RESULTS_DIR,
    PIPELINE_CONFIG
)


# ========================================

# 1. Logging
# ========================================

def setup_logging():
    RESULTS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    log_path = RESULTS_DIR / "pipeline.log"

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(message)s"
        ),
        handlers=[
            logging.FileHandler(
                log_path,
                encoding="utf-8"
            ),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )

    return logging.getLogger(
        "Week12Pipeline"
    )


# ========================================
# 2. Run Python Module
# ========================================

def run_module(
    module_name,
    logger,
    extra_args=None
):
    command = [
        sys.executable,
        "-m",
        module_name
    ]

    if extra_args:
        command.extend(extra_args)

    logger.info(
        "Starting module: %s",
        module_name
    )

    start = time.perf_counter()

    result = subprocess.run(
        command,
        cwd=str(ROOT_DIR),
        check=False
    )

    elapsed = time.perf_counter() - start

    if result.returncode != 0:
        logger.error(
            "Module failed: %s | exit_code=%s",
            module_name,
            result.returncode
        )

        raise RuntimeError(
            f"{module_name} failed "
            f"with exit code {result.returncode}"
        )

    logger.info(
        "Completed: %s | %.2f seconds",
        module_name,
        elapsed
    )

    return elapsed


# ========================================
# 3. Pipeline Stages
# ========================================

def get_pipeline_stages(
    include_benchmark=False
):
    stages = [
        ("Generate Bronze", "week12.generate_data"),
        ("Ingestion", "week12.ingestion"),
        ("Clean Silver", "week12.cleaning"),
        ("Validate Silver", "week12.validation"),
        ("Build Sales Detail", "week12.transform"),
        ("Build Gold Analytics", "week12.aggregation")
    ]

    if include_benchmark:
        stages.append(
            (
                "Performance Benchmark",
                "week12.performance_test"
            )
        )

    return stages


# ========================================
# 4. Run Pipeline
# ========================================

def run_pipeline(
    logger,
    include_benchmark=False
):
    start = time.perf_counter()

    logger.info("=" * 60)
    logger.info("WEEK 12 PIPELINE STARTED")
    logger.info(
        "Start time: %s",
        datetime.now().isoformat()
    )
    logger.info(
        "Configuration: %s",
        PIPELINE_CONFIG
    )

    stage_results = []

    stages = get_pipeline_stages(
        include_benchmark=include_benchmark
    )

    for stage_name, module_name in stages:
        logger.info("-" * 60)
        logger.info(
            "STAGE: %s",
            stage_name
        )

        elapsed = run_module(
            module_name,
            logger
        )

        stage_results.append(
            (stage_name, elapsed)
        )

    logger.info("-" * 60)
    logger.info("Running result consistency test")

    run_module(
        "pytest",
        logger,
        [
            "-q",
            "week12/tests/"
            "test_result_consistency.py"
        ]
    )

    total = time.perf_counter() - start

    logger.info("=" * 60)
    logger.info("PIPELINE SUCCEEDED")
    logger.info(
        "Total elapsed: %.2f seconds",
        total
    )

    for name, seconds in stage_results:
        logger.info(
            "%s: %.2f seconds",
            name,
            seconds
        )

    return total


# ========================================
# 5. CLI
# ========================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Week 12 End-to-End "
            "PySpark Pipeline"
        )
    )

    parser.add_argument(
        "--benchmark",
        action="store_true",
        help=(
            "Run the optional "
            "performance benchmark"
        )
    )

    args = parser.parse_args()

    logger = setup_logging()

    try:
        run_pipeline(
            logger,
            include_benchmark=args.benchmark
        )

    except Exception:
        logger.exception(
            "PIPELINE FAILED"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
