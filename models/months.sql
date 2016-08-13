select
date_trunc('month',date) as date_month
from (select generate_series( min(date), max(date), '1 month'::interval) as date
       from {{ref('stripe_invoices_cleaned')}}) i