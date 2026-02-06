{{ 
  config(
    alias = var('company') ~ '_table_tiktok_all_all_campaign_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["advertiser_id", "campaign_id"],
    tags = ['mart', 'tiktok', 'ad']    
  ) 
}}

select
    date,
    month,
    year,

    department,
    account,

    advertiser_id,
    campaign_id,
    campaign_name,
    campaign_status,
    
    impressions,
    clicks,
    spend,
    
    result,
    objective_type,
    engaged_view_15s,
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