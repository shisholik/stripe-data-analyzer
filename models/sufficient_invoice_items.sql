select 
  invoices.customer,
  invoices.date,
  invoice_items.*,
  charges.id as charge_id, 
  charges.paid, 
  charges.amount as amount_charged, 
  charges.refunded, 
  charges.amount_refunded, 
  charges.refunded_at,
  charges.created,
  date_trunc('month',charges.created) as created_month,
  case when refunded_at is null
    then invoice_items.period_end
    else refunded_at
  end real_end 
from 
  {{ref('stripe_invoice_items_cleaned')}} as invoice_items 
left join 
  {{ref('stripe_charges_cleaned')}} as charges 
on 
  charges.invoice = invoice_items.invoice
left join 
  {{ref('stripe_invoices_cleaned')}} as invoices
on
  invoices.id = invoice_items.invoice