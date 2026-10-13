USE CATALOG maven_catalog;

-- 1. COLUMN-LEVEL MASKING FUNCTION (CLS)
CREATE OR REPLACE FUNCTION audit.mask_pii_string(val STRING)
RETURNS STRING
RETURN CASE 
    WHEN is_account_group_member('Admin') OR is_account_group_member('Data_Compliance') THEN val
    ELSE '***MASKED***'
END;

-- Apply Masking Policy to PII Columns
ALTER TABLE maven_catalog.silver.silver_customers 
ALTER COLUMN first_name SET MASK audit.mask_pii_string;

ALTER TABLE maven_catalog.silver.silver_customers 
ALTER COLUMN last_name SET MASK audit.mask_pii_string;

-- 2. ROW-LEVEL SECURITY FUNCTION (RLS)
CREATE OR REPLACE FUNCTION audit.rls_user_region(region_col STRING)
RETURNS BOOLEAN
RETURN 
    is_account_group_member('Admin') OR 
    EXISTS (
        SELECT 1 FROM maven_catalog.audit.user_region_mapping map
        WHERE map.user_email = current_user()
          AND map.assigned_region = region_col
    );

-- Apply RLS Policy to Gold Store Dimension
ALTER TABLE maven_catalog.gold.dim_stores 
SET ROW FILTER audit.rls_user_region ON (region_country);