{{ 
  config(
    materialized = 'ephemeral',
    tags = ['stg', 'facebook', 'ad']
  ) 
}}

{% set company = var('company') %}
{% set raw_schema = company ~ '_dataset_facebook_api_raw' %}
{% set table_prefix = company ~ '_table_facebook_' %}

{% if execute %}

    {% set tables_query %}
        select table_name
        from `{{ target.project }}.{{ raw_schema }}.INFORMATION_SCHEMA.TABLES`
        where table_name like '{{ table_prefix }}%_ad_m______'
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

    cast(null as string)  as account_id,
    cast(null as string)  as campaign_id,
    cast(null as string)  as adset_id,
    cast(null as string)  as ad_id,

    cast(null as int64)   as impressions,
    cast(null as int64)   as clicks,
    cast(null as numeric) as spend,

    cast(null as int64)   as result,
    cast(null as string)  as result_type,

    cast(null as int64)   as messaging_conversations_started,
    cast(null as int64)   as purchase,

    cast(null as date)    as date,
    cast(null as int64)   as year,
    cast(null as string)  as month

where false

{% else %}

{% for table_name in table_names %}

select
    split('{{ table_name }}', '_')[offset(3)] as department,
    split('{{ table_name }}', '_')[offset(4)] as account,

    account_id,
    campaign_id,
    adset_id,
    ad_id,

    date,
    year,
    month,

    impressions,
    clicks,
    spend,

    result,
    result_type,

    messaging_conversations_started,
    purchase

from `{{ target.project }}.{{ raw_schema }}.{{ table_name }}`
{% if not loop.last %} union all {% endif %}

{% endfor %}

{% endif %}