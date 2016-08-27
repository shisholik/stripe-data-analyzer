select
  s.customer,
  s.invoice,
  s.date,
  date_trunc('month',s.date) as invoice_date_month,
  s.subscription,
  s.plan,
  s.quantity,
  s.period_start,
  case
    when lead(s.period_start) OVER (partition by subscription ORDER BY period_start) < period_end
      then lead(s.period_start) OVER (partition by subscription ORDER BY period_start)
    else period_end
  end period_end,
  s.amount,
  s.amount_charged,
  p.mrr*quantity as mrr,
  p.interval,
  s.paid,
  s.refunded,
  s.is_main
from {{ref('stripe_subscriptions')}} as s
  left join {{ref('stripe_plan_cleaned')}} as p on s.plan=p.id
order by period_start
