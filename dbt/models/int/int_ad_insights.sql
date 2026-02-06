{{ 
  config(
    materialized = 'ephemeral',
    tags = ['int', 'tiktok', 'ad']
  ) 
}}

select
    date(insights.date) as date,

    insights.advertiser_id,
    insights.campaign_id,
    insights.adgroup_id,
    insights.ad_id,

    ad.ad_name,

    case
        when ad.operation_status = 'ACTIVE'                 then '🟢'
        when ad.operation_status = 'PAUSED'                then '⚪'
        when ad.operation_status in ('ARCHIVED','DELETED')  then '🔴'
        else '❓'
    end as ad_status,

    ad.location,
    ad.gender,
    ad.age,
    ad.audience,
    ad.format,
    ad.strategy,
    ad.type,
    ad.pillar,
    ad.content,

    campaign.campaign_name,
    campaign.platform,
    campaign.objective,
    campaign.budget_group,
    campaign.region,
    campaign.category_level_1,
    campaign.track_group,
    campaign.pillar_group,
    campaign.content_group,

    creative.video_cover_url,

    insights.impressions,
    insights.clicks,
    insights.spend,

    insights.result,
    insights.optimization_event,

    insights.engaged_view_15s,
    insights.purchase

from {{ ref('stg_ad_insights') }} insights

left join `{{ target.project }}.{{ var('company') }}_dataset_tiktok_api_raw.{{ var('company') }}_table_tiktok_{{ var('department') }}_{{ var('account') }}_ad_metadata` ad
    on insights.advertiser_id = ad.advertiser_id
   and insights.ad_id         = ad.ad_id

left join `{{ target.project }}.{{ var('company') }}_dataset_tiktok_api_raw.{{ var('company') }}_table_tiktok_{{ var('department') }}_{{ var('account') }}_campaign_metadata` campaign
    on insights.advertiser_id = campaign.advertiser_id
   and insights.campaign_id  = campaign.campaign_id

left join `{{ target.project }}.{{ var('company') }}_dataset_tiktok_api_raw.{{ var('company') }}_table_tiktok_{{ var('department') }}_{{ var('account') }}_ad_creative` creative
    on ad.advertiser_id = creative.advertiser_id
   and ad.video_id      = creative.video_id