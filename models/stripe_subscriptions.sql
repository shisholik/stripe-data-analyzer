select 
  customer, 
  invoice,
  date,
  case when type='subscription' then id else subscription end subscription, 
  case when type='subscription' then true else false end is_main,
  plan, 
  quantity, 
  period_start, 
  real_end as period_end,
  amount,
  amount_charged - amount_refunded as amount_charged,
  paid,
  refunded
from {{ref('sufficient_invoice_items')}} t where amount>0 and paid=true
order by subscription, period_start
