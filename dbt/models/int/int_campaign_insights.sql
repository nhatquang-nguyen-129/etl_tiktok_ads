{{ 
  config(
    materialized = 'ephemeral',
    tags = ['int', 'tiktok', 'campaign']
  ) 
}}

select
    date,
    month,
    year,

    insights.department,
    insights.account,
    insights.advertiser_id,
    insights.campaign_id,

    insights.impressions,
    insights.clicks,
    insights.spend,

    insights.result,

    insights.engaged_view_15s,
    insights.purchase,    

    campaign.campaign_name,
    campaign.advertiser_name,
    campaign.operation_status,
    campaign.objective_type,

    case
        when campaign.operation_status = 'ACTIVE'                    then '🟢'
        when campaign.operation_status = 'PAUSED'                    then '⚪'
        when campaign.operation_status in ('ARCHIVED', 'DELETED')    then '🔴'
        else '❓'
    end as campaign_status,

    campaign.platform,
    campaign.objective,
    campaign.region,
    campaign.budget_group_1,
    campaign.budget_group_2,
    campaign.category_level_1,
    campaign.personnel,
    campaign.track_group,
    campaign.pillar_group,
    campaign.content_group

from {{ ref('stg_campaign_insights') }} insights
left join `{{ target.project }}.{{ var('company') }}_dataset_tiktok_api_raw.{{ var('company') }}_table_tiktok_{{ var('department') }}_{{ var('account') }}_campaign_metadata` campaign
    on insights.advertiser_id = campaign.advertiser_id
   and insights.campaign_id = campaign.campaign_id