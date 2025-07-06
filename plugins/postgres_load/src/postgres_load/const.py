CREATE_INDEX_COMMAND = """
CREATE {unique_str}INDEX IF NOT EXISTS "{index_name}"
ON "{table}" ({columns_str})
"""

INSERT_UPSERT_COMMAND = """
INSERT INTO "{table}"
({columns_str}) VALUES ({placeholders})
ON CONFLICT ({conflict_columns_str})
DO UPDATE SET {update_set}
"""

INSERT_BATCH_COMMAND = """
INSERT INTO "{table}"
({columns_str}) VALUES ({placeholders})
"""

TRUNCATE_TABLE = 'TRUNCATE TABLE "{table}"'

CREATE_TABLE = """
CREATE TABLE "{table}" (
    {column_definitions}
) {table_options}
"""

SELECT_EXISTS_TABLE = """
SELECT EXISTS (
    SELECT FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
)
"""
