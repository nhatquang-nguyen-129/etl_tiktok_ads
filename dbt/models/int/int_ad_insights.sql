{{ 
  config(
    materialized = 'ephemeral',
    tags = ['int', 'tiktok', 'ad']
  ) 
}}

select
    date(insights.date) as date,
    month,
    year,

    insights.department,
    insights.account,
    insights.advertiser_id,
    insights.ad_id,
    
    insights.impressions,
    insights.clicks,
    insights.spend,

    insights.result,

    insights.engaged_view_15s,
    insights.purchase,

    case
        when ad.operation_status in (
            'ENABLE', 
            'AUDIT'
            ) then '🟢'

        when ad.operation_status in (
            'DISABLE', 
            'PAUSED'
            ) then '⚪'

        when ad.operation_status in (
            'REJECT', 
            'DELETE', 
            'INVALID', 
            'FROZEN'
            ) then '🔴'

        else '❓'
    end as ad_status,

    ad.ad_name,
    ad.adgroup_id,
    ad.adgroup_name,
    ad.optimization_event,
    ad.location,
    ad.gender,
    ad.age,
    ad.audience,
    ad.format,
    ad.strategy,
    ad.type,
    ad.pillar,
    ad.content,

    campaign.campaign_id,
    campaign.campaign_name,
    campaign.platform,
    campaign.objective,
    campaign.budget_group_1,
    campaign.budget_group_2,
    campaign.region,
    campaign.category_level_1,
    campaign.track_group,
    campaign.pillar_group,
    campaign.content_group,

    creative.video_id,
    creative.video_cover_url

from {{ ref('stg_ad_insights') }} insights

left join `{{ target.project }}.{{ var('company') }}_dataset_tiktok_api_raw.{{ var('company') }}_table_tiktok_{{ var('department') }}_{{ var('account') }}_ad_metadata` ad
    on insights.advertiser_id = ad.advertiser_id
   and insights.ad_id         = ad.ad_id

left join `{{ target.project }}.{{ var('company') }}_dataset_tiktok_api_raw.{{ var('company') }}_table_tiktok_{{ var('department') }}_{{ var('account') }}_campaign_metadata` campaign
    on ad.advertiser_id = campaign.advertiser_id
   and ad.campaign_id   = campaign.campaign_id

left join `{{ target.project }}.{{ var('company') }}_dataset_tiktok_api_raw.{{ var('company') }}_table_tiktok_{{ var('department') }}_{{ var('account') }}_ad_creative` creative
    on ad.advertiser_id = creative.advertiser_id
   and ad.video_id      = creative.video_id
