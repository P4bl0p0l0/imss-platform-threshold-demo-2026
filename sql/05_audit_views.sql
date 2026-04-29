DROP VIEW IF EXISTS view_iva_clean_sample;
DROP VIEW IF EXISTS view_referral_defense_cases;
DROP VIEW IF EXISTS view_presumptive_risk_cases;
DROP VIEW IF EXISTS view_tax_discrepancy_cases;

CREATE VIEW view_iva_clean_sample AS
SELECT
    earner_uuid,
    period,
    vehicle_category,
    gross_monthly_income,
    service_income,
    tips_amount,
    referral_bonuses,
    promotional_incentives,
    net_income_defense_position,
    iva_reference_base,
    iva_consistency_check,
    imss_status_defense_position
FROM final_results
WHERE primary_strategy_scenario = 'SCENARIO_1_IVA_CLEAN_SAMPLE';

CREATE VIEW view_referral_defense_cases AS
SELECT
    earner_uuid,
    period,
    vehicle_category,
    gross_monthly_income,
    tips_amount,
    referral_bonuses,
    income_including_referrals,
    income_excluding_referrals,
    net_income_conservative_position,
    net_income_defense_position,
    imss_status_conservative_position,
    imss_status_defense_position,
    referral_defense_flag
FROM final_results
WHERE referral_defense_flag = 'REFERRAL_DEFENSE_CASE';

CREATE VIEW view_presumptive_risk_cases AS
SELECT
    earner_uuid,
    period,
    vehicle_category,
    gross_monthly_income,
    net_income_defense_position,
    net_income_presumptive_4_smgv,
    imss_status_defense_position,
    imss_status_presumptive_4_smgv,
    presumptive_risk_flag
FROM final_results
WHERE presumptive_risk_flag = 'PRESUMPTIVE_OVERCLASSIFICATION_RISK';

CREATE VIEW view_tax_discrepancy_cases AS
SELECT
    earner_uuid,
    period,
    vehicle_category,
    net_income_defense_position,
    iva_reference_base,
    iva_consistency_check
FROM final_results
WHERE iva_consistency_check = 'DISCREPANCY_ALERT';
