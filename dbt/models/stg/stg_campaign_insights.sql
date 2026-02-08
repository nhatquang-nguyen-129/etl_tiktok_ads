{{ 
  config(
    materialized = 'ephemeral',
    tags = ['stg', 'tiktok', 'campaign']
  ) 
}}

{% set company = var('company') %}
{% set raw_schema = company ~ '_dataset_tiktok_api_raw' %}
{% set table_prefix = company ~ '_table_tiktok_' %}

{% if execute %}

    {% set tables_query %}
        select table_name
        from `{{ target.project }}.{{ raw_schema }}.INFORMATION_SCHEMA.TABLES`
        where table_name like '{{ table_prefix }}%_campaign_m______'
    {% endset %}

    {% set results = run_query(tables_query) %}
    {% set table_names = results.columns[0].values() if results is not none else [] %}

{% else %}
    {% set table_names = [] %}
{% endif %}

{% if table_names | length == 0 %}

select
    cast(null as string)  as department,
    cast(null as string)  as account,

    cast(null as string)  as advertiser_id,
    cast(null as string)  as campaign_id,
    cast(null as date)    as date,

    cast(null as int64)   as impressions,
    cast(null as int64)   as clicks,
    cast(null as numeric) as spend,

    cast(null as int64)   as result,

    cast(null as int64)   as engaged_view_15s,
    cast(null as int64)   as purchase,

    cast(null as int64)   as year,
    cast(null as string)  as month

where false

{% else %}

{% for table_name in table_names %}

select
    split('{{ table_name }}', '_')[offset(3)] as department,
    split('{{ table_name }}', '_')[offset(4)] as account,

    DATE(date) as date,
    year,
    month,

    advertiser_id,
    campaign_id,

    impressions,
    clicks,
    spend,

    result,

    engaged_view_15s,
    purchase

from `{{ target.project }}.{{ raw_schema }}.{{ table_name }}`
{% if not loop.last %} union all {% endif %}

{% endfor %}

{% endif %}