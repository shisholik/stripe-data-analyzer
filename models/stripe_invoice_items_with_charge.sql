select 
  invoice_items.*,
  charges.id as charge_id, 
  charges.paid, 
  charges.amount as amount_charged, 
  charges.refunded, 
  charges.amount_refunded, 
  charges.refunded_at
from 
  {{ref('stripe_invoice_items_cleaned')}} as invoice_items 
left join 
  {{ref('stripe_charges_cleaned')}} as charges 
on 
  charges.invoice = invoice_items.invoice