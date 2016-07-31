select 
  id,
  name,
  timestamp 'epoch' + p.created * interval '1 Second' as created,
  currency,
  amount,
  case when interval='year' then amount / 12 else amount end mrr,
  interval
from plan as p