select DISTINCT
subscription,
date,
period_start,
period_end
from {{ref('sufficient_invoice_items')}}