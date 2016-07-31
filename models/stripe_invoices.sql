
select
  id,
  customer,
  date,
  forgiven,
  subscription as subscription_id,
  paid,
  total,
  period_start,
  period_end
from
  public.invoice

