/*
TASK:
Monthly Threshold Classification & IMSS Defense Scenario Mapping 2026

PURPOSE:
This query classifies earners under a threshold-based platform worker model
and maps each earner to the relevant defense scenario:

1. IVA Clean Sample
2. Referral Defense
3. Presumptive Risk
4. Standard Threshold Classification

IMPORTANT:
This is a simulated demo query. It uses artificial data and business assumptions
for presentation purposes only.
*/

DROP VIEW IF EXISTS final_results;

CREATE VIEW final_results AS

WITH Parameters AS (
    SELECT
        9582.47 AS threshold_2026,
        0.52 AS category_a_multiplier,
        0.68 AS category_b_multiplier,
        0.97 AS category_c_multiplier,
        0.05 AS tax_discrepancy_tolerance,
        0.08 AS iva_reference_rate,
        4 * 9582.47 AS presumptive_4_smgv_base
),

Cleansed_Earnings AS (
    SELECT
        earner_uuid,
        period,
        vehicle_category,

        gross_monthly_income,
        service_income,
        tips_amount,
        referral_bonuses,
        promotional_incentives,
        cash_collections_adjustment,

        iva_withheld,
        isr_withheld,

        payout_date,
        service_period_start,
        service_period_end,

        gross_monthly_income
            - COALESCE(tips_amount, 0)
            - COALESCE(cash_collections_adjustment, 0)
        AS income_without_tips

    FROM earnings_core
    WHERE period = '2026-03'
),

Scenario_Base AS (
    SELECT
        ce.*,

        CASE
            WHEN COALESCE(referral_bonuses, 0) = 0
             AND COALESCE(promotional_incentives, 0) = 0
            THEN 1
            ELSE 0
        END AS is_iva_clean_sample,

        income_without_tips - COALESCE(referral_bonuses, 0)
        AS income_excluding_referrals,

        income_without_tips AS income_including_referrals

    FROM Cleansed_Earnings ce
),

Vehicle_Exclusion AS (
    SELECT
        sb.*,

        CASE
            WHEN vehicle_category = 'A' THEN p.category_a_multiplier
            WHEN vehicle_category = 'B' THEN p.category_b_multiplier
            WHEN vehicle_category = 'C' THEN p.category_c_multiplier
            ELSE 1.00
        END AS vehicle_multiplier,

        p.threshold_2026,
        p.tax_discrepancy_tolerance,
        p.iva_reference_rate,
        p.presumptive_4_smgv_base

    FROM Scenario_Base sb
    CROSS JOIN Parameters p
),

Income_Calculation AS (
    SELECT
        *,

        ROUND(income_excluding_referrals * vehicle_multiplier, 2)
        AS net_income_defense_position,

        ROUND(income_including_referrals * vehicle_multiplier, 2)
        AS net_income_conservative_position,

        ROUND(presumptive_4_smgv_base * vehicle_multiplier, 2)
        AS net_income_presumptive_4_smgv

    FROM Vehicle_Exclusion
),

Classification AS (
    SELECT
        *,

        CASE
            WHEN net_income_defense_position >= threshold_2026
            THEN 'MANDATORY_FULL_IMSS'
            ELSE 'INDEPENDENT_RT_ONLY'
        END AS imss_status_defense_position,

        CASE
            WHEN net_income_conservative_position >= threshold_2026
            THEN 'MANDATORY_FULL_IMSS'
            ELSE 'INDEPENDENT_RT_ONLY'
        END AS imss_status_conservative_position,

        CASE
            WHEN net_income_presumptive_4_smgv >= threshold_2026
            THEN 'MANDATORY_FULL_IMSS'
            ELSE 'INDEPENDENT_RT_ONLY'
        END AS imss_status_presumptive_4_smgv

    FROM Income_Calculation
),

Tax_Reconciliation AS (
    SELECT
        *,

        ROUND(iva_withheld / iva_reference_rate, 2) AS iva_reference_base,

        CASE
            WHEN ABS(
                net_income_defense_position - (iva_withheld / iva_reference_rate)
            ) > net_income_defense_position * tax_discrepancy_tolerance
            THEN 'DISCREPANCY_ALERT'
            ELSE 'CONSISTENT'
        END AS iva_consistency_check,

        CASE
            WHEN imss_status_conservative_position = 'MANDATORY_FULL_IMSS'
             AND imss_status_defense_position = 'INDEPENDENT_RT_ONLY'
            THEN 'REFERRAL_DEFENSE_CASE'
            ELSE 'NO_REFERRAL_IMPACT'
        END AS referral_defense_flag,

        CASE
            WHEN imss_status_presumptive_4_smgv = 'MANDATORY_FULL_IMSS'
             AND imss_status_defense_position = 'INDEPENDENT_RT_ONLY'
            THEN 'PRESUMPTIVE_OVERCLASSIFICATION_RISK'
            ELSE 'NO_PRESUMPTIVE_DELTA'
        END AS presumptive_risk_flag

    FROM Classification
)

SELECT
    earner_uuid,
    period,
    vehicle_category,

    gross_monthly_income,
    service_income,
    tips_amount,
    referral_bonuses,
    promotional_incentives,
    cash_collections_adjustment,

    iva_withheld,
    isr_withheld,
    iva_reference_base,
    iva_consistency_check,

    vehicle_multiplier,

    income_without_tips,
    income_excluding_referrals,
    income_including_referrals,

    net_income_defense_position,
    net_income_conservative_position,
    net_income_presumptive_4_smgv,

    imss_status_defense_position,
    imss_status_conservative_position,
    imss_status_presumptive_4_smgv,

    is_iva_clean_sample,
    referral_defense_flag,
    presumptive_risk_flag,

    CASE
        WHEN is_iva_clean_sample = 1
         AND iva_consistency_check = 'CONSISTENT'
        THEN 'SCENARIO_1_IVA_CLEAN_SAMPLE'

        WHEN referral_defense_flag = 'REFERRAL_DEFENSE_CASE'
        THEN 'SCENARIO_2_REFERRAL_DEFENSE'

        WHEN presumptive_risk_flag = 'PRESUMPTIVE_OVERCLASSIFICATION_RISK'
        THEN 'SCENARIO_3_PRESUMPTIVE_RISK'

        ELSE 'STANDARD_THRESHOLD_CLASSIFICATION'
    END AS primary_strategy_scenario

FROM Tax_Reconciliation;
