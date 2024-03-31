{% if local_context_dict.dialect == 'mysql' %}
SHOW KEYS FROM {{  local_context_dict.basedatabase }}.your_table_name WHERE Key_name = 'PRIMARY';
{% endif %}


{% if local_context_dict.dialect == 'tsql' %}
SELECT column_name
FROM [{{ local_context_dict.basedatabase }}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
AND TABLE_CATALOG = '{{ local_context_dict.basedatabase }}'  AND TABLE_SCHEMA = '{{ local_context_dict.dbschema }}' AND TABLE_NAME = '{{ name }}';
{% endif %}