
select
  id,
  description,
  subscription,
  proration,
  amount,
  plan,
  invoice,
  quantity,
  type,
  timestamp 'epoch' + period_start * interval '1 Second' as period_start,
  timestamp 'epoch' + period_end * interval '1 Second' as period_end
from
  public.line_item


