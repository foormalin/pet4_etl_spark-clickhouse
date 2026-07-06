from run_pipeline import STAGES


def test_pipeline_stage_order() -> None:
    assert STAGES == [
        "init_clickhouse.py",
        "01_load_bronze.py",
        "02_transform_silver.py",
        "03_build_gold_and_load_clickhouse.py",
    ]
