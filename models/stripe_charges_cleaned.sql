
select
  charge.id,
  charge.invoice,
  charge.paid,
  charge.amount,
  charge.refunded,
  charge.amount_refunded,
  refunds.created as refunded_at,
  charge.customer,
  timestamp 'epoch' + charge.created * interval '1 Second' as created
from
  public.charge
left join {{ref('stripe_refunds_cleaned')}} as refunds on charge.id = refunds.charge


