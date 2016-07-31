
select
  id,
  charge,
  amount,
  timestamp 'epoch' + created * interval '1 Second' as created
from
  public.refund
where 
  status='succeeded'


