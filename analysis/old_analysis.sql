SET params.begin_date = '2016-06-01';
SET params.end_date = '2016-07-01';

with subscriptions_periods as (
    select
      s.customer,
      s.invoice,
      s.date,
      s.subscription,
      s.plan,
      s.quantity,
      s.period_start,
      case when lead(s.period_start) OVER (partition by subscription ORDER BY period_start) < period_end then lead(s.period_start) OVER (partition by subscription ORDER BY period_start) else period_end end period_end,
      s.amount,
      s.amount_charged,
      p.mrr*quantity as mrr,
      s.paid,
      s.refunded,
      s.is_main
    from stripe_subscriptions as s
      left join public.stripe_plan_cleaned as p on s.plan=p.id
    order by period_start
),

    subs as(
      select
        *
      from subscriptions_periods where
        (refunded = false or refunded is Null)
  ),
    old_customers as(
      select DISTINCT customer from subscriptions_periods where current_setting('params.begin_date')::date > date
  ),
    old_subscriptions as(
      select DISTINCT subscription from subscriptions_periods where current_setting('params.begin_date')::date > date
  ),
    active_customers_at_beginning as(
      select * from subs where current_setting('params.begin_date')::date BETWEEN period_start and period_end
  ),
    active_customers_at_end as(
      select * from subs where current_setting('params.end_date')::date BETWEEN period_start and period_end
  ),
    new_customers as(
      select DISTINCT subscription from subs where
        date BETWEEN current_setting('params.begin_date')::date and current_setting('params.end_date')::date and
        customer not in (select * from old_customers)
  ),
    new_customers1 as(
      select * from subs where
        date BETWEEN current_setting('params.begin_date')::date and current_setting('params.end_date')::date and
        customer not in (select * from old_customers)
  ),
    mrr_change as (
      select coalesce(b.mrr,0) as start_amount, coalesce(e.mrr,0) as end_amount, coalesce(e.mrr,0) - coalesce(b.mrr,0) as total
      from active_customers_at_beginning b
        full join active_customers_at_end e on b.subscription=e.subscription
  )

select
  (select count(subscription) from active_customers_at_beginning) as "Subscriptions beginning of the month",
  (select count(*) from new_customers) as "New subscriptions",
  (select count(subscription) from active_customers_at_beginning) + (select count(*) from new_customers) - (select count(subscription) from (select DISTINCT subscription from active_customers_at_end) as t) as "Lost subscriptions",
  to_char(((select 1.0*count(subscription) from active_customers_at_beginning) + (select 1.0*count(*) from new_customers) - (select 1.0*count(subscription) from (select DISTINCT subscription from active_customers_at_end) as t)) / (select 1.0*count(subscription) from active_customers_at_beginning) * 100,'99.99%') as "Churn rate",
  (select count(subscription) from (select DISTINCT subscription from active_customers_at_end) as t) as "Subscriptions end of the month",
  to_char(((select 1.0 * count(subscription) from (select DISTINCT subscription from active_customers_at_end) as t) / (select 1.0 * count(subscription) from active_customers_at_beginning) -1)*100,'99.99%') as "m/m growth subscriptions",
  (select (sum(mrr)/100)::money from active_customers_at_beginning) as "MRR beginning of the month",
  (select (sum(total)/100)::money from mrr_change where total > 0) as "New MRR",
  (select (sum(total)/100)::money from mrr_change where total > 0 and start_amount = 0) as "New MRR from new subscriptions",
  (select (sum(total)/100)::money from mrr_change where total > 0 and start_amount != 0 and end_amount !=0) as "New MRR from account expansions",
  (select (ABS(coalesce(sum(total),0))/100)::money from mrr_change where total < 0) as "Lost MRR",
  (select (ABS(coalesce(sum(total),0))/100)::money from mrr_change where total < 0 and end_amount=0) as "Lost MRR from churned subscriptions",
  (select (ABS(coalesce(sum(total),0))/100)::money from mrr_change where total < 0 and end_amount!=0) as "Lost MRR from contractions",
  to_char(ABS((select (sum(total)/100)::money from mrr_change where total < 0) / (select (sum(mrr)/100)::money from active_customers_at_beginning))*100,'99.99%') as "MRR churn rate",
  (select (sum(total)/100)::money from mrr_change where total > 0) + (select (sum(total)/100)::money from mrr_change where total < 0) as "Net new MRR",
  (select (sum(mrr)/100)::money from active_customers_at_end) as "MRR end of month",
  to_char(((select (sum(mrr)/100)::money from active_customers_at_end) / (select (sum(mrr)/100)::money from active_customers_at_beginning)-1)*100.0,'99.99%') as "m/m growth MRR",
  (select (sum(amount_charged)/100)::money from subs where
    date BETWEEN current_setting('params.begin_date')::date and current_setting('params.end_date')::date) as "Revenue",
  (select (sum(amount_charged)/100)::money from new_customers1) as "Revenue from new subscriptions"
