# Schema Mapping CSV Format

The schema mapping file is a CSV that maps **source database schema names** to **target Snowflake DATABASE.SCHEMA** references. It is used in Step 4 of the migration workflow to replace legacy schema references in SQL files.

## Format

```csv
SOURCE_SCHEMA,TARGET_DB_SCHEMA
```

| Column | Description |
|--------|-------------|
| `SOURCE_SCHEMA` | The schema (or table) name as it appears in the source SQL |
| `TARGET_DB_SCHEMA` | The fully-qualified Snowflake target: `DATABASE.SCHEMA` or `DATABASE.SCHEMA.TABLE` |

## Example

```csv
SOURCE_SCHEMA,TARGET_DB_SCHEMA
ODS,DBMIG_POC.DBMIG_POC
STG,DBMIG_POC.DBMIG_POC
EDW,DBMIG_POC.DBMIG_POC
MART,DBMIG_POC.DBMIG_POC
LEGACY,DB_NOT_FOUND.SCHEMA_NOT_FOUND
CUSTOMERS,DBMIG_POC.DBMIG_POC.CUSTOMERS
ORDERS,DBMIG_POC.DBMIG_POC.ORDERS
PRODUCTS,DBMIG_POC.DBMIG_POC.PRODUCTS
```

## How Replacement Works

1. **Schema entries** (names followed by `.` in SQL): `ODS.MY_TABLE` → `DBMIG_POC.DBMIG_POC.MY_TABLE`
2. **Table entries** (standalone names): `CUSTOMERS` → `DBMIG_POC.DBMIG_POC.CUSTOMERS`
3. **Processing order**: Longer names are processed first to avoid partial-match conflicts
4. Replacements use **regex** with word boundaries to avoid replacing substrings within other identifiers

## Notes

- The CSV must have a header row: `SOURCE_SCHEMA,TARGET_DB_SCHEMA`
- Both columns are required for every row
- Leading/trailing whitespace is trimmed
- If no schema mapping CSV is provided, Step 4 is skipped entirely
