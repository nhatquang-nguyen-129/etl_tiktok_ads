{{ 
  config(
    materialized = 'ephemeral',
    tags = ['int', 'facebook', 'ad']
  ) 
}}

select
    date(insights.date) as date,

    insights.account_id,
    insights.campaign_id,
    insights.adset_id,
    insights.ad_id,

    ad.ad_name,

    case
        when ad.status = 'ACTIVE'                 then '🟢'
        when ad.status = 'PAUSED'                 then '⚪'
        when ad.status in ('ARCHIVED','DELETED')  then '🔴'
        else '❓'
    end as ad_status,

    campaign.campaign_name,
    campaign.platform,
    campaign.objective,
    campaign.budget_group,
    campaign.region,
    campaign.category_level_1,
    campaign.track_group,
    campaign.pillar_group,
    campaign.content_group,

    adset.location,
    adset.gender,
    adset.age,
    adset.audience,
    adset.format,
    adset.strategy,
    adset.type,
    adset.pillar,
    adset.content,

    creative.thumbnail_url,

    insights.impressions,
    insights.clicks,
    insights.spend,

    insights.result,
    insights.result_type,

    insights.messaging_conversations_started,
    insights.purchase

from {{ ref('stg_ad_insights') }} insights

left join {{ ref('stg_ad_metadata') }} ad
    on insights.account_id = ad.account_id
   and insights.ad_id      = ad.ad_id

left join {{ ref('stg_campaign_metadata') }} campaign
    on insights.account_id  = campaign.account_id
   and insights.campaign_id = campaign.campaign_id

left join {{ ref('stg_adset_metadata') }} adset
    on insights.account_id = adset.account_id
   and insights.adset_id   = adset.adset_id

left join {{ ref('stg_ad_creative') }} creative
    on insights.account_id = creative.account_id
   and insights.ad_id      = creative.ad_id