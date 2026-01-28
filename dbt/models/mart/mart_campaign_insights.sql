{{ 
  config(
    alias = var('company') ~ '_table_facebook_all_all_campaign_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["account_id", "campaign_id"],
    tags = ['mart', 'facebook', 'ad']    
  ) 
}}

select
    date,
    month,
    year,

    department,
    account,

    account_id,
    campaign_id,
    campaign_name,
    campaign_status,
    
    impressions,
    clicks,
    spend,
    
    result,
    result_type,
    messaging_conversations_started,
    purchase,

    platform,
    objective,
    budget_group,
    region,
    category_level_1,
    track_group,
    pillar_group,
    content_group
from {{ ref('int_campaign_insights') }}