{% if  local_context_dict.dialect == 'mysql' %}
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')  AND TABLE_SCHEMA = '{{ local_context_dict.basedatabase }}'
AND table_name = '{{ name }}' ;
{% endif %}
{% if  local_context_dict.dialect == 'tsql' %}
SELECT column_name, data_type
FROM [{{ local_context_dict.basedatabase }}].information_schema.columns
WHERE TABLE_CATALOG = '{{ local_context_dict.basedatabase  }}'  AND TABLE_SCHEMA = '{{ local_context_dict.dbschema }}' AND TABLE_NAME = '{{ name }}';
{% endif %}