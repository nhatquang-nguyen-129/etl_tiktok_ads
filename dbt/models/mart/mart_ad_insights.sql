{{ 
  config(
    alias = var('company') ~ '_table_tiktok_all_all_ad_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["advertiser_id", "ad_id"],
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
    adgroup_id,
    ad_id,

    ad_name,
    ad_status,
    video_id,
    video_cover_url,

    impressions,
    clicks,
    spend,

    result,
    optimization_event,
    engaged_view_15s,
    purchase,

    campaign_name,
    platform,
    objective,
    budget_group_1,
    budget_group_2,
    region,
    category_level_1,
    track_group,
    pillar_group,
    content_group,

    adgroup_name,
    location,
    gender,
    age,
    audience,
    format,
    strategy,
    type,
    pillar,
    content

from {{ ref('int_ad_insights') }}