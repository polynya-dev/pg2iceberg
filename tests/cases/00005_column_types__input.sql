-- SETUP --
CREATE TABLE e2e_column_types (
    id SERIAL PRIMARY KEY,

    -- integer types
    col_smallint     SMALLINT,
    col_integer      INTEGER,
    col_bigint       BIGINT,

    -- floating point
    col_real         REAL,
    col_double       DOUBLE PRECISION,

    -- decimal/numeric: precision scenarios
    col_numeric      NUMERIC,
    col_numeric_10_2 NUMERIC(10, 2),
    col_numeric_38_0 NUMERIC(38, 0),
    col_numeric_50_10 NUMERIC(50, 10),

    -- boolean
    col_bool         BOOLEAN,

    -- text types
    col_text         TEXT,
    col_varchar      VARCHAR(255),
    col_char         CHAR(10),

    -- binary
    col_bytea        BYTEA,

    -- date/time
    col_date         DATE,
    col_timestamp    TIMESTAMP,
    col_timestamptz  TIMESTAMPTZ,

    -- uuid
    col_uuid         UUID,

    -- json
    col_json         JSON,
    col_jsonb        JSONB,

    -- types that fall back to string
    col_inet         INET
);
ALTER TABLE e2e_column_types REPLICA IDENTITY FULL;
CREATE PUBLICATION pg2iceberg_pub_e2e_column_types FOR TABLE e2e_column_types;
-- DATA --
INSERT INTO e2e_column_types (
    id,
    col_smallint, col_integer, col_bigint,
    col_real, col_double,
    col_numeric, col_numeric_10_2, col_numeric_38_0, col_numeric_50_10,
    col_bool,
    col_text, col_varchar, col_char,
    col_bytea,
    col_date, col_timestamp, col_timestamptz,
    col_uuid,
    col_json, col_jsonb,
    col_inet
) VALUES
(
    1,
    32767, 2147483647, 9223372036854775807,
    1.5, 2.718281828459045,
    0.12345678901234567890123456789012345678, 12345678.99, 99999999999999999999999999999999999999, 1234567890123456789012345678901234567890.1234567890,
    true,
    'hello world', 'varchar value', 'padded',
    '\xDEADBEEF',
    '2025-06-15', '2025-06-15 14:30:45.123456', '2025-06-15 14:30:45.123456+00',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '{"key": "value"}', '{"nested": {"a": 1}}',
    '192.168.1.1'
),
(
    2,
    NULL, NULL, NULL,
    NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL,
    NULL, NULL, NULL,
    NULL,
    NULL, NULL, NULL,
    NULL,
    NULL, NULL,
    NULL
),
(
    3,
    -1, -100, -9223372036854775808,
    0.25, -1.0,
    0.00000000000000000000000000000000000001, -99999999.99, 12345678901234567890123456789012345678, 0.0000000001,
    false,
    '', 'special chars: ''quotes'' & "double"', '0123456789',
    '\x00FF',
    '1970-01-01', '1970-01-01 00:00:00', '2025-12-31 23:59:59.999999+00',
    '00000000-0000-0000-0000-000000000000',
    '[]', '{"a": null}',
    '::1'
);
