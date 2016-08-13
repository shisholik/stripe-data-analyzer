with periods as (select date_month, i.*, 
case when 
  m.date_month = date_trunc('month', i.period_start)
  then 1
  else 0
end as is_start_period
from months m
  left outer join stripe_subscriptions_periods i
    on m.date_month >= date_trunc('month', i.period_start)
    and m.date_month < date_trunc('month', i.period_end)),

subs as (
select *,
case min(date_month) over(partition by customer)
    when date_month then 1
    else 0
    end as first_payment,
  case max(date_month) over(partition by customer)
    when date_month then 1
    else 0
  end as last_payment,
  case 
    when 
    lag(period_end) over(partition by customer order by date_month) != period_start 
    and lag(period_end) over(partition by customer order by date_month) is not Null
    and is_start_period=1 then 1
    else 0
  end as resubscribed,
  lag(period_end) over(partition by customer order by date_month)  as asdas,
  period_start
  
from periods),


plan_changes as (

  select
    *,
    lag(mrr,1,0) over (partition by customer order by date_month) as prior_month_total,
    mrr - lag(mrr,1,0) over (partition by customer order by date_month) as change,
    lag(period_end) over (partition by customer order by date_month) as prior_month_period_end
  from subs

),

data as (

  select *,
    case
      when first_payment = 1
        then 'new'
      when last_payment = 1
        and period_end < current_date
        then 'churn'
      when resubscribed = 1
        then 'resubscribed'
      when change > 0
        then 'upgrade'
      when change < 0
        then 'downgrade'
      when interval != 'month'
        and date_month < date_trunc('month', prior_month_period_end)
        then 'prepaid renewal'
      else
        'renewal'
      end revenue_category,
      case
        when prior_month_total < mrr then prior_month_total
        else mrr
      end renewal_component_of_change
  from plan_changes

),

subscriptions as (
select date_month, count(*) as value from data group by 1
),
last_month_subscriptions as (
select date_month,lag(value) over(order by date_month) as value from subscriptions
),
subscriptions_by_pro_monthly as (
select date_month,count(*) as value from data
where (plan='Professional_monthly' or plan='pro5_monthly')
group by 1
),

subscriptions_by_pro_yearly as (
select date_month,count(*) as value from data
where (plan='pro5_yearly' or plan='Professional_yearly' or plan='Professional_yearly_30off')
group by 1
),

subscriptions_by_team_yearly as (
select date_month,count(*) as value from data
where (plan='Team_monthly')
group by 1
),

subscriptions_by_team_monthly as (
select date_month, count(*) as value from data
where (plan='Team_yearly' or plan='Team_yearly_30off')
group by 1
),

new_subscriptions as (
select date_month, count(*) as value from data
where revenue_category = 'new'
group by 1
),
resubscribed_subscriptions as (
  select date_month, count(*) as value
  from data
  where revenue_category = 'resubscribed'
  group by 1
), 
new_subscriptions_by_pro_monthly as (
select date_month,count(*) as value from data
where revenue_category = 'new' and (plan='Professional_monthly' or plan='pro5_monthly')
group by 1
),

new_subscriptions_by_pro_yearly as (
select date_month,count(*) as value from data
where revenue_category = 'new' and (plan='pro5_yearly' or plan='Professional_yearly' or plan='Professional_yearly_30off')
group by 1
),

new_subscriptions_by_team_yearly as (
select date_month,count(*) as value from data
where revenue_category = 'new' and (plan='Team_monthly')
group by 1
),

new_subscriptions_by_team_monthly as (
select date_month, count(*) as value from data
where revenue_category = 'new' and (plan='Team_yearly' or plan='Team_yearly_30off')
group by 1
),


lost_subscriptions as (
select date_month, count(*) as value from data
where revenue_category = 'churn'
group by 1
),
lost_subscriptions_by_pro_monthly as (
select date_month,count(*) as value from data
where revenue_category = 'churn' and (plan='Professional_monthly' or plan='pro5_monthly')
group by 1
),

lost_subscriptions_by_pro_yearly as (
select date_month,count(*) as value from data
where revenue_category = 'churn' and (plan='pro5_yearly' or plan='Professional_yearly' or plan='Professional_yearly_30off')
group by 1
),

lost_subscriptions_by_team_yearly as (
select date_month,count(*) as value from data
where revenue_category = 'churn' and (plan='Team_monthly')
group by 1
),

lost_subscriptions_by_team_monthly as (
select date_month, count(*) as value from data
where revenue_category = 'churn' and (plan='Team_yearly' or plan='Team_yearly_30off')
group by 1
),

mrr as (
select date_month, (sum(mrr)/100)::float  as value from data group by 1
),
mrr2 as (
select date_month, (sum(mrr)/100)::float  as value from data
where revenue_category in ('renewal', 'downgrade', 'upgrade', 'new', 'prepaid renewal', 'churn')
 group by 1
),
news as (

  select date_month, (sum(mrr)/100)::float as value
  from data
  where revenue_category = 'new'
  group by 1

), renewals as (

  select date_month, (sum(mrr)/100)::float as value
  from data
  where revenue_category in ('renewal', 'downgrade', 'upgrade')
  group by 1

), resubscribed as (
  select date_month, (sum(mrr)/100)::float as value
  from data
  where revenue_category = 'resubscribed'
  group by 1
), 

prepaids as (

  select date_month, (sum(mrr)/100)::float as value
  from data
  where revenue_category = 'prepaid renewal'
  group by 1

), churns as (

  select date_month, (sum(mrr)/100)::float as value
  from data
  where revenue_category = 'churn'
  group by 1

), upgrades as (

  select date_month, (sum(change)/100)::float as value
  from data
  where revenue_category = 'upgrade'
  group by 1

), downgrades as (

  select date_month, (sum(change)/100)::float as value
  from data
  where revenue_category = 'downgrade'
  group by 1

),

result as(
select months.date_month,
  last_month_subscriptions.value as "Subscriptions beginning of the month",
  subscriptions.value as "Subscriptions end of the month",
  new_subscriptions.value  as "New subscriptions",
  resubscribed_subscriptions.value as "Resubscribed subscriptions",
  lost_subscriptions.value as "Lost subscriptions",
  mrr.value as "MRR",
  mrr2.value as "MRR2",
  news.value as new,
  resubscribed.value as resubscribed,
  renewals.value as renewal,
  prepaids.value as committed,
  churns.value * -1 as churned,
  upgrades.value as upgrades,
  downgrades.value as downgrades,
 
  subscriptions_by_pro_monthly.value as "S Premium monthly",
  subscriptions_by_pro_yearly.value as "S Premium annual",
  subscriptions_by_team_monthly.value as "S Team monthly",
  subscriptions_by_team_yearly.value as "S Team annual",

  new_subscriptions_by_pro_monthly.value as "NS Premium monthly",
  new_subscriptions_by_pro_yearly.value as "NS Premium annual",
  new_subscriptions_by_team_monthly.value as "NS Team monthly",
  new_subscriptions_by_team_yearly.value as "NS Team annual",

  lost_subscriptions_by_pro_monthly.value as "LC Premium monthly",
  lost_subscriptions_by_pro_yearly.value as "LC Premium annual",
  lost_subscriptions_by_team_monthly.value as "LC Team monthly",
  lost_subscriptions_by_team_yearly.value as "LC Team annual"
from months
  left outer join subscriptions on months.date_month = subscriptions.date_month
  left outer join last_month_subscriptions on months.date_month = last_month_subscriptions.date_month
  left outer join resubscribed_subscriptions on months.date_month = resubscribed_subscriptions.date_month
  left outer join mrr on months.date_month = mrr.date_month
  left outer join mrr2 on months.date_month = mrr2.date_month
  left outer join news on months.date_month = news.date_month
  left outer join resubscribed on months.date_month = resubscribed.date_month
  left outer join renewals on months.date_month = renewals.date_month
  left outer join prepaids on months.date_month = prepaids.date_month
  left outer join churns on months.date_month = churns.date_month
  left outer join upgrades on months.date_month = upgrades.date_month
  left outer join downgrades on months.date_month = downgrades.date_month
  left outer join new_subscriptions on months.date_month = new_subscriptions.date_month
  left outer join lost_subscriptions on months.date_month = lost_subscriptions.date_month

  left outer join subscriptions_by_pro_monthly on months.date_month = subscriptions_by_pro_monthly.date_month
  left outer join subscriptions_by_pro_yearly on months.date_month = subscriptions_by_pro_yearly.date_month
  left outer join subscriptions_by_team_monthly on months.date_month = subscriptions_by_team_monthly.date_month
  left outer join subscriptions_by_team_yearly on months.date_month = subscriptions_by_team_yearly.date_month

  
  left outer join new_subscriptions_by_pro_monthly on months.date_month = new_subscriptions_by_pro_monthly.date_month
  left outer join new_subscriptions_by_pro_yearly on months.date_month = new_subscriptions_by_pro_yearly.date_month
  left outer join new_subscriptions_by_team_monthly on months.date_month = new_subscriptions_by_team_monthly.date_month
  left outer join new_subscriptions_by_team_yearly on months.date_month = new_subscriptions_by_team_yearly.date_month

  left outer join lost_subscriptions_by_pro_monthly on months.date_month = lost_subscriptions_by_pro_monthly.date_month
  left outer join lost_subscriptions_by_pro_yearly on months.date_month = lost_subscriptions_by_pro_yearly.date_month
  left outer join lost_subscriptions_by_team_monthly on months.date_month = lost_subscriptions_by_team_monthly.date_month
  left outer join lost_subscriptions_by_team_yearly on months.date_month = lost_subscriptions_by_team_yearly.date_month
order by 1)

select * from result
--select * from data where revenue_category = 'churn' and date_month='2016-06-01'
--select * from subs where resubscribed=1 and date_month='2016-06-01'

