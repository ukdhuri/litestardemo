
{% set suffix = '_mtmp' %}

{% if t_dialect == 'TSQL' %}
    {% if 'dbo' in name %}
        IF OBJECT_ID('{{ name + suffix }}') IS NOT NULL
            DROP TABLE {{ name + suffix }};
    {% endif %}
{% else %}
    IF OBJECT_ID('{{ local_context_dict.dbschema }}.{{ name + suffix }}') IS NOT NULL
        DROP TABLE {{ local_context_dict.dbschema }}.{{ name + suffix }};
{% endif %}

{% if t_dialectc== 'MYSQL' %}
DROP TABLE IF EXISTS {{ name + suffix }}
{% endif %}
 

