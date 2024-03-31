

IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('{{ target_table }}')
    AND is_identity = 1
)
BEGIN
    SET IDENTITY_INSERT {{ target_table }} ON;
END;



MERGE INTO {{ target_table }} AS target
USING {{ source_table }} AS source
ON
{% for column in unique_key_columns %}
    target.{{ column }} = source.{{ column }}{% if not loop.last %} AND{% endif %}
{% endfor %}
WHEN MATCHED THEN
    UPDATE SET
    {% for column in update_columns %}
        target.{{ column }} = source.{{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
WHEN NOT MATCHED THEN
    INSERT
    (
    {% for column in insert_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
    )
    VALUES
    (
    {% for column in insert_columns %}
        source.{{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
    );

    
IF EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('{{ target_table }}')
    AND is_identity = 1
)
BEGIN
    SET IDENTITY_INSERT  {{ target_table }} OFF;
END;
