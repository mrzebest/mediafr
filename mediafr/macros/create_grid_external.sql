{% macro create_grid_external() %}
  {% set ddl %}
  CREATE OR REPLACE EXTERNAL TABLE `{{ target.project }}.filmfr.grid_external_filmfr`
	OPTIONS (
    format = 'CSV',
    uris = ['gs://mediafrance/filmfrancais1957/*.csv'],
     skip_leading_rows = 1,
    field_delimiter = ';',
    allow_quoted_newlines = true,
    encoding = 'UTF-8'
  );

  {% endset %}

  {% do run_query(ddl) %}
{% endmacro %}
