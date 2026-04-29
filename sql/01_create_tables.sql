DROP TABLE IF EXISTS earnings_core;

CREATE TABLE earnings_core (
    earner_uuid TEXT,
    period TEXT,
    vehicle_category TEXT,

    gross_monthly_income REAL,
    service_income REAL,
    tips_amount REAL,
    referral_bonuses REAL,
    promotional_incentives REAL,
    cash_collections_adjustment REAL,

    iva_withheld REAL,
    isr_withheld REAL,

    payout_date TEXT,
    service_period_start TEXT,
    service_period_end TEXT
);
