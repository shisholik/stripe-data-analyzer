WITH periods AS (SELECT
                   m.date_month,
                   i.*,
                   CASE WHEN
                     m.date_month = date_trunc('month', i.period_start)
                     THEN 1
                   ELSE 0
                   END                              AS is_start_period,
                   min(period_start)
                   OVER (PARTITION BY subscription) AS subscription_start,
                   max(period_end)
                   OVER (PARTITION BY subscription) AS subscription_end
                 FROM months m
                   LEFT OUTER JOIN stripe_subscriptions_periods i
                     ON m.date_month >= date_trunc('month', i.period_start)
                        AND m.date_month < date_trunc('month', i.period_end)),

    subs AS (
      SELECT
        *,
        CASE WHEN min(date_month)
                  OVER (PARTITION BY customer) = date_month
          THEN 1
        ELSE 0
        END AS first_payment,
        CASE max(date_month)
        OVER (PARTITION BY customer)
        WHEN date_month
          THEN 1
        ELSE 0
        END AS last_payment,
        CASE
        WHEN
          lag(date_month)
          OVER (PARTITION BY customer
            ORDER BY date_month) != date_month - INTERVAL '1 month' AND lag(date_month)
                                                                        OVER (PARTITION BY customer
                                                                          ORDER BY date_month) IS NOT NULL
          THEN 1
        ELSE 0
        END AS resubscribed,
        CASE
        WHEN plan = 'Professional_monthly' OR plan = 'pro5_monthly'
          THEN 'Professional_monthly'
        WHEN plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off'
          THEN 'Professional_yearly'
        WHEN plan = 'Team_yearly' OR plan = 'Team_yearly_30off'
          THEN 'Team_yearly'
        ELSE plan
        END AS new_plan
      FROM periods),


    plan_changes AS (

      SELECT
        *,
        CASE WHEN resubscribed = 0
          THEN lag(mrr, 1, 0)
          OVER (PARTITION BY customer
            ORDER BY date_month)
        ELSE 0
        END                    AS prior_month_total,
        CASE
        WHEN resubscribed = 0 AND lag(mrr, 1, 0)
                                  OVER (PARTITION BY customer
                                    ORDER BY date_month) != 0
          THEN mrr - lag(mrr, 1, 0)
          OVER (PARTITION BY customer
            ORDER BY date_month)
        ELSE 0
        END                    AS change,
        lag(period_end)
        OVER (PARTITION BY customer
          ORDER BY date_month) AS prior_month_period_end,
        lead(resubscribed, 1, 1)
        OVER (PARTITION BY customer
          ORDER BY date_month) AS last_payment_in_period
      FROM subs

  ),

    data AS (

      SELECT
        *,
        CASE
        WHEN first_payment = 1 AND resubscribed = 0
          THEN 'new'
        WHEN last_payment = 1
             AND period_end < current_date
          THEN 'churn'
        WHEN resubscribed = 1
          THEN 'resubscribed'
        WHEN change > 0
          THEN 'upgrade'
        WHEN change < 0
          THEN 'downgrade'
        WHEN interval != 'month'
             AND date_month < date_trunc('month', prior_month_period_end)
          THEN 'prepaid renewal'
        ELSE
          'renewal'
        END revenue_category,
        CASE
        WHEN prior_month_total < mrr
          THEN prior_month_total
        ELSE mrr
        END renewal_component_of_change
      FROM plan_changes

  ),

    subscriptions AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      GROUP BY 1
  ),
    last_month_subscriptions AS (
      SELECT
        date_month,
        lag(value)
        OVER (
          ORDER BY date_month) AS value
      FROM subscriptions
  ),
    subscriptions_by_pro_monthly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
      GROUP BY 1
  ),

    subscriptions_by_pro_yearly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    subscriptions_by_team_monthly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE (plan = 'Team_monthly')
      GROUP BY 1
  ),

    subscriptions_by_team_yearly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

  -- Reactivated subscriptions
    reactivated_subscriptions AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE resubscribed = 1
      GROUP BY 1
  ),
    reactivated_subscriptions_by_pro_monthly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE resubscribed = 1 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
      GROUP BY 1
  ),

    reactivated_subscriptions_by_pro_yearly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE resubscribed = 1 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    reactivated_subscriptions_by_team_monthly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE resubscribed = 1 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    reactivated_subscriptions_by_team_yearly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE resubscribed = 1 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

  -- New subsciptions --
    new_subscriptions AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE first_payment = 1
      GROUP BY 1
  ),

    new_subscriptions_by_pro_monthly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE first_payment = 1 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
      GROUP BY 1
  ),

    new_subscriptions_by_pro_yearly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE first_payment = 1 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    new_subscriptions_by_team_monthly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE first_payment = 1 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    new_subscriptions_by_team_yearly AS (
      SELECT
        date_month,
        count(*) AS value
      FROM data
      WHERE first_payment = 1 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

  -- Lost subscriptions --
    lost_subscriptions AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        count(*)                        AS value
      FROM data
      WHERE last_payment_in_period = 1
      GROUP BY 1
  ),

    lost_subscriptions_by_pro_monthly AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        count(*)                        AS value
      FROM data
      WHERE last_payment_in_period = 1 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
      GROUP BY 1
  ),

    lost_subscriptions_by_pro_yearly AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        count(*)                        AS value
      FROM data
      WHERE last_payment_in_period = 1 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    lost_subscriptions_by_team_monthly AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        count(*)                        AS value
      FROM data
      WHERE last_payment_in_period = 1 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    lost_subscriptions_by_team_yearly AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        count(*)                        AS value
      FROM data
      WHERE last_payment_in_period = 1 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

  -- MRR --
    mrr AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      GROUP BY 1
  ), mrr_by_pro_monthly AS (
    SELECT
      date_month,
      (sum(mrr) :: FLOAT / 100) AS value
    FROM data
    WHERE (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
    GROUP BY 1
),

    mrr_by_pro_yearly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    mrr_by_team_monthly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE (plan = 'Team_monthly')
      GROUP BY 1
  ),

    mrr_by_team_yearly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),


    last_month_mrr AS (
      SELECT
        date_month,
        lag(value)
        OVER (
          ORDER BY date_month) AS value
      FROM mrr
  ),
  -- New MRR
    new_mrr AS (

      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE first_payment = 1
      GROUP BY 1

  ), new_mrr_by_pro_monthly AS (
    SELECT
      date_month,
      (sum(mrr) :: FLOAT / 100) AS value
    FROM data
    WHERE first_payment = 1 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
    GROUP BY 1
),

    new_mrr_by_pro_yearly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE first_payment = 1 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    new_mrr_by_team_monthly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE first_payment = 1 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    new_mrr_by_team_yearly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE first_payment = 1 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

  -- Reactivation MRR
    reactivation_mrr AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE resubscribed = 1
      GROUP BY 1
  ), reactivation_mrr_by_pro_monthly AS (
    SELECT
      date_month,
      (sum(mrr) :: FLOAT / 100) AS value
    FROM data
    WHERE resubscribed = 1 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
    GROUP BY 1
),

    reactivation_mrr_by_pro_yearly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE resubscribed = 1 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    reactivation_mrr_by_team_monthly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE resubscribed = 1 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    reactivation_mrr_by_team_yearly AS (
      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE resubscribed = 1 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

  -- Churn MRR
    churn_mrr AS (

      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        (sum(mrr) :: FLOAT / 100)       AS value
      FROM data
      WHERE last_payment_in_period = 1
      GROUP BY 1

  ), churn_mrr_by_pro_monthly AS (
    SELECT
      date_month + INTERVAL '1 month' AS date_month,
      (sum(mrr) :: FLOAT / 100)       AS value
    FROM data
    WHERE last_payment_in_period = 1 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
    GROUP BY 1
),

    churn_mrr_by_pro_yearly AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        (sum(mrr) :: FLOAT / 100)       AS value
      FROM data
      WHERE last_payment_in_period = 1 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    churn_mrr_by_team_monthly AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        (sum(mrr) :: FLOAT / 100)       AS value
      FROM data
      WHERE last_payment_in_period = 1 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    churn_mrr_by_team_yearly AS (
      SELECT
        date_month + INTERVAL '1 month' AS date_month,
        (sum(mrr) :: FLOAT / 100)       AS value
      FROM data
      WHERE last_payment_in_period = 1 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),
  -- Expansion MRR
    expansion_mrr AS (

      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change > 0
      GROUP BY 1

  ), expansion_mrr_by_pro_monthly AS (
    SELECT
      date_month,
      (sum(change) :: FLOAT / 100) AS value
    FROM data
    WHERE change > 0 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
    GROUP BY 1
),

    expansion_mrr_by_pro_yearly AS (
      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change > 0 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    expansion_mrr_by_team_monthly AS (
      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change > 0 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    expansion_mrr_by_team_yearly AS (
      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change > 0 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),
  -- Contraction MRR
    contraction_mrr AS (

      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change < 0
      GROUP BY 1
  ),
    contraction_mrr_by_pro_monthly AS (
      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change < 0 AND (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
      GROUP BY 1
  ),

    contraction_mrr_by_pro_yearly AS (
      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change < 0 AND (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    contraction_mrr_by_team_monthly AS (
      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change < 0 AND (plan = 'Team_monthly')
      GROUP BY 1
  ),

    contraction_mrr_by_team_yearly AS (
      SELECT
        date_month,
        (sum(change) :: FLOAT / 100) AS value
      FROM data
      WHERE change < 0 AND (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

    renewals AS (

      SELECT
        date_month,
        (sum(mrr) :: FLOAT / 100) AS value
      FROM data
      WHERE revenue_category IN ('renewal', 'downgrade', 'upgrade')
      GROUP BY 1

  ), prepaids AS (

    SELECT
      date_month,
      (sum(mrr) :: FLOAT / 100) AS value
    FROM data
    WHERE revenue_category = 'prepaid renewal'
    GROUP BY 1

),
  -- Revenue
    revenue AS (
      SELECT
        invoice_date_month,
        (sum(amount_charged) :: FLOAT / 100) AS value
      FROM stripe_subscriptions_periods
      GROUP BY 1
  ),
    revenue_by_pro_monthly AS (
      SELECT
        invoice_date_month,
        (sum(amount_charged) :: FLOAT / 100) AS value
      FROM stripe_subscriptions_periods
      WHERE (plan = 'Professional_monthly' OR plan = 'pro5_monthly')
      GROUP BY 1
  ),

    revenue_by_pro_yearly AS (
      SELECT
        invoice_date_month,
        (sum(amount_charged) :: FLOAT / 100) AS value
      FROM stripe_subscriptions_periods
      WHERE (plan = 'pro5_yearly' OR plan = 'Professional_yearly' OR plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    revenue_by_team_monthly AS (
      SELECT
        invoice_date_month,
        (sum(amount_charged) :: FLOAT / 100) AS value
      FROM stripe_subscriptions_periods
      WHERE (plan = 'Team_monthly')
      GROUP BY 1
  ),

    revenue_by_team_yearly AS (
      SELECT
        invoice_date_month,
        (sum(amount_charged) :: FLOAT / 100) AS value
      FROM stripe_subscriptions_periods
      WHERE (plan = 'Team_yearly' OR plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

  -- New revenue
    new_revenue AS (
      SELECT
        s.invoice_date_month,
        (sum(s.amount_charged) :: FLOAT / 100) AS value
      FROM data
        LEFT JOIN stripe_subscriptions_periods s ON data.customer = s.customer AND data.date_month = s.invoice_date_month
      WHERE first_payment = 1
      GROUP BY 1
  ),
    new_revenue_by_pro_monthly AS (
      SELECT
        s.invoice_date_month,
        (sum(s.amount_charged) :: FLOAT / 100) AS value
      FROM data
        LEFT JOIN stripe_subscriptions_periods s ON data.customer = s.customer AND data.date_month = s.invoice_date_month
      WHERE first_payment = 1 AND (s.plan = 'Professional_monthly' OR s.plan = 'pro5_monthly')
      GROUP BY 1
  ),

    new_revenue_by_pro_yearly AS (
      SELECT
        s.invoice_date_month,
        (sum(s.amount_charged) :: FLOAT / 100) AS value
      FROM data
        LEFT JOIN stripe_subscriptions_periods s ON data.customer = s.customer AND data.date_month = s.invoice_date_month
      WHERE first_payment = 1 AND (s.plan = 'pro5_yearly' OR s.plan = 'Professional_yearly' OR s.plan = 'Professional_yearly_30off')
      GROUP BY 1
  ),

    new_revenue_by_team_monthly AS (
      SELECT
        s.invoice_date_month,
        (sum(s.amount_charged) :: FLOAT / 100) AS value
      FROM data
        LEFT JOIN stripe_subscriptions_periods s ON data.customer = s.customer AND data.date_month = s.invoice_date_month
      WHERE first_payment = 1 AND (s.plan = 'Team_monthly')
      GROUP BY 1
  ),

    new_revenue_by_team_yearly AS (
      SELECT
        s.invoice_date_month,
        (sum(s.amount_charged) :: FLOAT / 100) AS value
      FROM data
        LEFT JOIN stripe_subscriptions_periods s ON data.customer = s.customer AND data.date_month = s.invoice_date_month
      WHERE first_payment = 1 AND (s.plan = 'Team_yearly' OR s.plan = 'Team_yearly_30off')
      GROUP BY 1
  ),

    result AS (
      SELECT
        to_char(months.date_month, 'YYYY-MM-DD')                                                               AS month,
        last_month_subscriptions.value                                                                         AS "Subscriptions beginning of the month",
        subscriptions.value                                                                                    AS "Subscriptions end of the month",
        new_subscriptions.value                                                                                AS "New subscriptions",
        reactivated_subscriptions.value                                                                        AS "Reactivated subscriptions",
        lost_subscriptions.value                                                                               AS "Lost subscriptions",

        mrr.value                                                                                              AS "MRR",
        last_month_mrr.value                                                                                   AS "Last month MRR",
        new_mrr.value + reactivation_mrr.value - churn_mrr.value + expansion_mrr.value + contraction_mrr.value AS "Net new MRR",
        new_mrr.value                                                                                          AS New,
        expansion_mrr.value                                                                                    AS Expansion,
        reactivation_mrr.value                                                                                 AS Reactivation,
        contraction_mrr.value                                                                                  AS Contraction,
        churn_mrr.value * -1                                                                                   AS Churn,

        revenue.value                                                                                          AS revenue,
        revenue_by_pro_monthly.value                                                                           AS "Revenue Premium monthly",
        revenue_by_pro_yearly.value                                                                            AS "Revenue Premium annual",
        revenue_by_team_monthly.value                                                                          AS "Revenue Team monthly",
        revenue_by_team_yearly.value                                                                           AS "Revenue Team annual",

        new_revenue.value                                                                                      AS "New revenue",
        new_revenue_by_pro_monthly.value                                                                       AS "New revenue Premium monthly",
        new_revenue_by_pro_yearly.value                                                                        AS "New revenue Premium annual",
        new_revenue_by_team_monthly.value                                                                      AS "New revenue Team monthly",
        new_revenue_by_team_yearly.value                                                                       AS "New revenue Team annual",


        subscriptions_by_pro_monthly.value                                                                     AS "S Premium monthly",
        subscriptions_by_pro_yearly.value                                                                      AS "S Premium annual",
        subscriptions_by_team_monthly.value                                                                    AS "S Team monthly",
        subscriptions_by_team_yearly.value                                                                     AS "S Team annual",

        new_subscriptions_by_pro_monthly.value                                                                 AS "NS Premium monthly",
        new_subscriptions_by_pro_yearly.value                                                                  AS "NS Premium annual",
        new_subscriptions_by_team_monthly.value                                                                AS "NS Team monthly",
        new_subscriptions_by_team_yearly.value                                                                 AS "NS Team annual",

        reactivated_subscriptions_by_pro_monthly.value                                                         AS "R Premium monthly",
        reactivated_subscriptions_by_pro_yearly.value                                                          AS "R Premium annual",
        reactivated_subscriptions_by_team_monthly.value                                                        AS "R Team monthly",
        reactivated_subscriptions_by_team_yearly.value                                                         AS "R Team annual",

        lost_subscriptions_by_pro_monthly.value                                                                AS "LC Premium monthly",
        lost_subscriptions_by_pro_yearly.value                                                                 AS "LC Premium annual",
        lost_subscriptions_by_team_monthly.value                                                               AS "LC Team monthly",
        lost_subscriptions_by_team_yearly.value                                                                AS "LC Team annual",

        mrr_by_pro_monthly.value                                                                               AS "MRR Premium monthly",
        mrr_by_pro_yearly.value                                                                                AS "MRR Premium annual",
        mrr_by_team_monthly.value                                                                              AS "MRR Team monthly",
        mrr_by_team_yearly.value                                                                               AS "MRR Team annual",

        new_mrr_by_pro_monthly.value                                                                           AS "NEW MRR Premium monthly",
        new_mrr_by_pro_yearly.value                                                                            AS "NEW MRR Premium annual",
        new_mrr_by_team_monthly.value                                                                          AS "NEW MRR Team monthly",
        new_mrr_by_team_yearly.value                                                                           AS "NEW MRR Team annual",

        reactivation_mrr_by_pro_monthly.value                                                                  AS "Reactivation MRR Premium monthly",
        reactivation_mrr_by_pro_yearly.value                                                                   AS "Reactivation MRR Premium annual",
        reactivation_mrr_by_team_monthly.value                                                                 AS "Reactivation MRR Team monthly",
        reactivation_mrr_by_team_yearly.value                                                                  AS "Reactivation MRR Team annual",

        churn_mrr_by_pro_monthly.value * -1                                                                    AS "Churn MRR Premium monthly",
        churn_mrr_by_pro_yearly.value * -1                                                                     AS "Churn MRR Premium annual",
        churn_mrr_by_team_monthly.value * -1                                                                   AS "Churn MRR Team monthly",
        churn_mrr_by_team_yearly.value * -1                                                                    AS "Churn MRR Team annual",

        expansion_mrr_by_pro_monthly.value                                                                     AS "Expansion MRR Premium monthly",
        expansion_mrr_by_pro_yearly.value                                                                      AS "Expansion MRR Premium annual",
        expansion_mrr_by_team_monthly.value                                                                    AS "Expansion MRR Team monthly",
        expansion_mrr_by_team_yearly.value                                                                     AS "Expansion MRR Team annual",

        contraction_mrr_by_pro_monthly.value                                                                   AS "Contraction MRR Premium monthly",
        contraction_mrr_by_pro_yearly.value                                                                    AS "Contraction MRR Premium annual",
        contraction_mrr_by_team_monthly.value                                                                  AS "Contraction MRR Team monthly",
        contraction_mrr_by_team_yearly.value                                                                   AS "Contraction MRR Team annual"
      FROM months
        LEFT OUTER JOIN subscriptions ON months.date_month = subscriptions.date_month
        LEFT OUTER JOIN last_month_subscriptions ON months.date_month = last_month_subscriptions.date_month
        LEFT OUTER JOIN new_subscriptions ON months.date_month = new_subscriptions.date_month
        LEFT OUTER JOIN reactivated_subscriptions ON months.date_month = reactivated_subscriptions.date_month
        LEFT OUTER JOIN lost_subscriptions ON months.date_month = lost_subscriptions.date_month


        LEFT OUTER JOIN mrr ON months.date_month = mrr.date_month
        LEFT OUTER JOIN last_month_mrr ON months.date_month = last_month_mrr.date_month
        LEFT OUTER JOIN new_mrr ON months.date_month = new_mrr.date_month
        LEFT OUTER JOIN reactivation_mrr ON months.date_month = reactivation_mrr.date_month
        LEFT OUTER JOIN renewals ON months.date_month = renewals.date_month
        LEFT OUTER JOIN prepaids ON months.date_month = prepaids.date_month
        LEFT OUTER JOIN churn_mrr ON months.date_month = churn_mrr.date_month
        LEFT OUTER JOIN expansion_mrr ON months.date_month = expansion_mrr.date_month
        LEFT OUTER JOIN contraction_mrr ON months.date_month = contraction_mrr.date_month

        LEFT OUTER JOIN revenue ON months.date_month = revenue.invoice_date_month
        LEFT OUTER JOIN new_revenue ON months.date_month = new_revenue.invoice_date_month

        LEFT OUTER JOIN revenue_by_pro_monthly ON months.date_month = revenue_by_pro_monthly.invoice_date_month
        LEFT OUTER JOIN revenue_by_pro_yearly ON months.date_month = revenue_by_pro_yearly.invoice_date_month
        LEFT OUTER JOIN revenue_by_team_monthly ON months.date_month = revenue_by_team_monthly.invoice_date_month
        LEFT OUTER JOIN revenue_by_team_yearly ON months.date_month = revenue_by_team_yearly.invoice_date_month

        LEFT OUTER JOIN new_revenue_by_pro_monthly ON months.date_month = new_revenue_by_pro_monthly.invoice_date_month
        LEFT OUTER JOIN new_revenue_by_pro_yearly ON months.date_month = new_revenue_by_pro_yearly.invoice_date_month
        LEFT OUTER JOIN new_revenue_by_team_monthly ON months.date_month = new_revenue_by_team_monthly.invoice_date_month
        LEFT OUTER JOIN new_revenue_by_team_yearly ON months.date_month = new_revenue_by_team_yearly.invoice_date_month


        LEFT OUTER JOIN subscriptions_by_pro_monthly ON months.date_month = subscriptions_by_pro_monthly.date_month
        LEFT OUTER JOIN subscriptions_by_pro_yearly ON months.date_month = subscriptions_by_pro_yearly.date_month
        LEFT OUTER JOIN subscriptions_by_team_monthly ON months.date_month = subscriptions_by_team_monthly.date_month
        LEFT OUTER JOIN subscriptions_by_team_yearly ON months.date_month = subscriptions_by_team_yearly.date_month


        LEFT OUTER JOIN new_subscriptions_by_pro_monthly ON months.date_month = new_subscriptions_by_pro_monthly.date_month
        LEFT OUTER JOIN new_subscriptions_by_pro_yearly ON months.date_month = new_subscriptions_by_pro_yearly.date_month
        LEFT OUTER JOIN new_subscriptions_by_team_monthly ON months.date_month = new_subscriptions_by_team_monthly.date_month
        LEFT OUTER JOIN new_subscriptions_by_team_yearly ON months.date_month = new_subscriptions_by_team_yearly.date_month

        LEFT OUTER JOIN reactivated_subscriptions_by_pro_monthly ON months.date_month = reactivated_subscriptions_by_pro_monthly.date_month
        LEFT OUTER JOIN reactivated_subscriptions_by_pro_yearly ON months.date_month = reactivated_subscriptions_by_pro_yearly.date_month
        LEFT OUTER JOIN reactivated_subscriptions_by_team_monthly ON months.date_month = reactivated_subscriptions_by_team_monthly.date_month
        LEFT OUTER JOIN reactivated_subscriptions_by_team_yearly ON months.date_month = reactivated_subscriptions_by_team_yearly.date_month

        LEFT OUTER JOIN lost_subscriptions_by_pro_monthly ON months.date_month = lost_subscriptions_by_pro_monthly.date_month
        LEFT OUTER JOIN lost_subscriptions_by_pro_yearly ON months.date_month = lost_subscriptions_by_pro_yearly.date_month
        LEFT OUTER JOIN lost_subscriptions_by_team_monthly ON months.date_month = lost_subscriptions_by_team_monthly.date_month
        LEFT OUTER JOIN lost_subscriptions_by_team_yearly ON months.date_month = lost_subscriptions_by_team_yearly.date_month

        LEFT OUTER JOIN mrr_by_pro_monthly ON months.date_month = mrr_by_pro_monthly.date_month
        LEFT OUTER JOIN mrr_by_pro_yearly ON months.date_month = mrr_by_pro_yearly.date_month
        LEFT OUTER JOIN mrr_by_team_monthly ON months.date_month = mrr_by_team_monthly.date_month
        LEFT OUTER JOIN mrr_by_team_yearly ON months.date_month = mrr_by_team_yearly.date_month

        LEFT OUTER JOIN new_mrr_by_pro_monthly ON months.date_month = new_mrr_by_pro_monthly.date_month
        LEFT OUTER JOIN new_mrr_by_pro_yearly ON months.date_month = new_mrr_by_pro_yearly.date_month
        LEFT OUTER JOIN new_mrr_by_team_monthly ON months.date_month = new_mrr_by_team_monthly.date_month
        LEFT OUTER JOIN new_mrr_by_team_yearly ON months.date_month = new_mrr_by_team_yearly.date_month

        LEFT OUTER JOIN reactivation_mrr_by_pro_monthly ON months.date_month = reactivation_mrr_by_pro_monthly.date_month
        LEFT OUTER JOIN reactivation_mrr_by_pro_yearly ON months.date_month = reactivation_mrr_by_pro_yearly.date_month
        LEFT OUTER JOIN reactivation_mrr_by_team_monthly ON months.date_month = reactivation_mrr_by_team_monthly.date_month
        LEFT OUTER JOIN reactivation_mrr_by_team_yearly ON months.date_month = reactivation_mrr_by_team_yearly.date_month

        LEFT OUTER JOIN churn_mrr_by_pro_monthly ON months.date_month = churn_mrr_by_pro_monthly.date_month
        LEFT OUTER JOIN churn_mrr_by_pro_yearly ON months.date_month = churn_mrr_by_pro_yearly.date_month
        LEFT OUTER JOIN churn_mrr_by_team_monthly ON months.date_month = churn_mrr_by_team_monthly.date_month
        LEFT OUTER JOIN churn_mrr_by_team_yearly ON months.date_month = churn_mrr_by_team_yearly.date_month

        LEFT OUTER JOIN expansion_mrr_by_pro_monthly ON months.date_month = expansion_mrr_by_pro_monthly.date_month
        LEFT OUTER JOIN expansion_mrr_by_pro_yearly ON months.date_month = expansion_mrr_by_pro_yearly.date_month
        LEFT OUTER JOIN expansion_mrr_by_team_monthly ON months.date_month = expansion_mrr_by_team_monthly.date_month
        LEFT OUTER JOIN expansion_mrr_by_team_yearly ON months.date_month = expansion_mrr_by_team_yearly.date_month

        LEFT OUTER JOIN contraction_mrr_by_pro_monthly ON months.date_month = contraction_mrr_by_pro_monthly.date_month
        LEFT OUTER JOIN contraction_mrr_by_pro_yearly ON months.date_month = contraction_mrr_by_pro_yearly.date_month
        LEFT OUTER JOIN contraction_mrr_by_team_monthly ON months.date_month = contraction_mrr_by_team_monthly.date_month
        LEFT OUTER JOIN contraction_mrr_by_team_yearly ON months.date_month = contraction_mrr_by_team_yearly.date_month


      ORDER BY 1)

SELECT *
FROM result