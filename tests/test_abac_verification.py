from dataclasses import dataclass
import pytest
from pyspark.sql import SparkSession

@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str

def run_abac_verification(spark: SparkSession, catalog: str = "dbx_maven_market"):
    results: list[CheckResult] = []

    # ── 1. Governed tags applied to PII columns ──
    pii_cols = spark.sql(f"""
        SELECT schema_name, table_name, column_name
        FROM {catalog}.information_schema.column_tags
        WHERE tag_name = 'classification' AND tag_value = 'PII'
    """).collect()

    expected_pii = {
        ("bronze", "raw_customers", "customer_address"),
        ("silver", "silver_customers", "customer_address"),
    }
    actual_pii = {(r.schema_name, r.table_name, r.column_name) for r in pii_cols}
    missing_pii = expected_pii - actual_pii
    results.append(CheckResult(
        "PII column tags",
        len(missing_pii) == 0,
        f"Missing: {missing_pii}" if missing_pii else f"{len(expected_pii)} columns tagged"
    ))

    # ── 2. Gold structural compliance ──
    non_compliant = spark.sql(f"""
        SELECT t.table_name
        FROM {catalog}.information_schema.tables t
        LEFT JOIN {catalog}.information_schema.table_tags ot
          ON t.table_catalog = ot.catalog_name AND t.table_schema = ot.schema_name
          AND t.table_name = ot.table_name AND ot.tag_name = 'owner_team'
        LEFT JOIN {catalog}.information_schema.table_tags ds
          ON t.table_catalog = ds.catalog_name AND t.table_schema = ds.schema_name
          AND t.table_name = ds.table_name AND ds.tag_name = 'data_sensitivity'
        WHERE t.table_schema = 'gold' AND t.table_catalog = '{catalog}'
          AND NOT t.table_name LIKE '__materialization%'
          AND NOT t.table_name LIKE 'event_log%'
          AND (ot.tag_value IS NULL OR ds.tag_value IS NULL)
    """).collect()
    results.append(CheckResult(
        "Gold structural compliance",
        len(non_compliant) == 0,
        f"Non-compliant: {[r.table_name for r in non_compliant]}" if non_compliant else "All gold tables tagged"
    ))

    # ── 3. ABAC policies exist ──
    policies = spark.sql(f"SHOW EFFECTIVE POLICIES ON CATALOG {catalog}").collect()
    policy_names = {r["Policy Name"] for r in policies}

    for expected in ["pii_column_mask", "region_row_filter"]:
        results.append(CheckResult(
            f"ABAC policy: {expected}",
            expected in policy_names,
            "Found" if expected in policy_names else "MISSING"
        ))

    # ── 4. Mask and filter UDFs exist ──
    for fn in ["mask_pii_fn", "filter_by_region_fn"]:
        try:
            spark.sql(f"DESCRIBE FUNCTION {catalog}.audit.{fn}").collect()
            results.append(CheckResult(f"UDF: {fn}", True, "Exists"))
        except Exception as e:
            results.append(CheckResult(f"UDF: {fn}", False, str(e)[:120]))

    # ── 5. audit_logs table exists ──
    try:
        cols = {r.col_name for r in spark.sql(f"DESCRIBE TABLE {catalog}.audit.audit_logs").collect()}
        required = {"log_id", "pipeline_name", "execution_status", "start_time", "log_timestamp"}
        missing = required - cols
        results.append(CheckResult(
            "audit_logs table",
            len(missing) == 0,
            f"Missing columns: {missing}" if missing else "Schema valid"
        ))
    except Exception as e:
        results.append(CheckResult("audit_logs table", False, str(e)[:120]))

    # ── 6. Runtime masking check ──
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    exempt_check = spark.sql("""
        SELECT
          is_account_group_member('Admin') OR
          is_account_group_member('Data_Compliance') OR
          is_account_group_member('pipeline_service_principals') AS is_exempt
    """).collect()[0][0]
    sample = spark.sql(f"""
        SELECT customer_address FROM {catalog}.silver.silver_customers LIMIT 1
    """).collect()[0][0]

    if exempt_check:
        is_visible = sample is not None and "MASKED" not in sample
        results.append(CheckResult(
            "Runtime: data visible to exempt user",
            is_visible,
            f"Exempt user ({current_user}) sees: '{str(sample)[:40]}'"
        ))
    else:
        is_masked = sample is None or "MASKED" in sample
        results.append(CheckResult(
            "Runtime: masking enforced",
            is_masked,
            f"Non-exempt ({current_user}) sees: '{str(sample)[:40]}'"
        ))

    return results


@pytest.mark.remote
def test_abac_verification(spark):
    """Executes the verification check against Unity Catalog."""
    results = run_abac_verification(spark, catalog="dbx_maven_market")
    failed = [r for r in results if not r.passed]
    assert len(failed) == 0, f"{len(failed)} ABAC verification checks failed."