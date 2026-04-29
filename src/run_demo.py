import argparse
import sqlite3
from pathlib import Path

import pandas as pd
from tabulate import tabulate


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "imss_threshold_demo.db"
DATA_PATH = ROOT / "data" / "earnings_core_sample.csv"
OUTPUTS_DIR = ROOT / "outputs"

<<<<<<< HEAD
SQL_FILES = [
    ROOT / "sql" / "01_create_tables.sql",
    ROOT / "sql" / "03_threshold_classification.sql",
    ROOT / "sql" / "05_audit_views.sql",
]

=======
>>>>>>> 97234b5 (Load expanded sample data from CSV)

def run_sql_file(connection, file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        sql_script = file.read()
    connection.executescript(sql_script)


def load_csv_to_database(connection):
    df = pd.read_csv(DATA_PATH)

    df.to_sql(
        "earnings_core",
        connection,
        if_exists="append",
        index=False
    )

    connection.commit()


def setup_database():
    if DB_PATH.exists():
        DB_PATH.unlink()

    connection = sqlite3.connect(DB_PATH)

    run_sql_file(connection, ROOT / "sql" / "01_create_tables.sql")
    load_csv_to_database(connection)
    run_sql_file(connection, ROOT / "sql" / "03_threshold_classification.sql")
    run_sql_file(connection, ROOT / "sql" / "05_audit_views.sql")

    connection.commit()
    return connection


def query_df(connection, query):
    return pd.read_sql_query(query, connection)


def print_table(title, df):
    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)

    if df.empty:
        print("No records found.")
    else:
        print(tabulate(df, headers="keys", tablefmt="github", showindex=False))


def export_output(df, filename):
    OUTPUTS_DIR.mkdir(exist_ok=True)
    path = OUTPUTS_DIR / filename
    df.to_csv(path, index=False)
    print(f"\nExported: {path}")


def show_raw_data(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
            vehicle_category,
            gross_monthly_income,
            service_income,
            tips_amount,
            referral_bonuses,
            promotional_incentives,
            cash_collections_adjustment,
            iva_withheld
        FROM earnings_core
        ORDER BY earner_uuid;
        """
    )
    print_table("STEP 1 - RAW EARNINGS DATA", df)


def show_cleansed_income(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
            gross_monthly_income,
            tips_amount,
            cash_collections_adjustment,
            income_without_tips
        FROM final_results
        ORDER BY earner_uuid;
        """
    )
    print_table("STEP 2 - CLEANSED INCOME AFTER TIPS AND CASH ADJUSTMENTS", df)


def show_vehicle_exclusion(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
            vehicle_category,
            income_excluding_referrals,
            vehicle_multiplier,
            net_income_defense_position
        FROM final_results
        ORDER BY earner_uuid;
        """
    )
    print_table("STEP 3 - VEHICLE TOOL EXCLUSION FACTOR", df)


def show_main_classification(connection):
    df = query_df(
        connection,
        """
        SELECT
            imss_status_defense_position,
            COUNT(*) AS total_earners
        FROM final_results
        GROUP BY imss_status_defense_position;
        """
    )
    print_table("STEP 4 - MAIN THRESHOLD CLASSIFICATION", df)


def show_scenario_mapping(connection):
    df = query_df(
        connection,
        """
        SELECT
            primary_strategy_scenario,
            COUNT(*) AS total_earners
        FROM final_results
        GROUP BY primary_strategy_scenario
        ORDER BY total_earners DESC;
        """
    )
    print_table("STEP 5 - STRATEGIC SCENARIO MAPPING", df)


def show_iva_clean_sample(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
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
        FROM view_iva_clean_sample
        ORDER BY vehicle_category, net_income_defense_position DESC;
        """
    )
    print_table("SCENARIO 1 - IVA CLEAN SAMPLE", df)
    export_output(df, "scenario_1_iva_clean_sample.csv")


def show_referral_defense(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
            vehicle_category,
            gross_monthly_income,
            service_income,
            tips_amount,
            referral_bonuses,
            net_income_conservative_position,
            net_income_defense_position,
            imss_status_conservative_position,
            imss_status_defense_position
        FROM view_referral_defense_cases
        ORDER BY vehicle_category, referral_bonuses DESC;
        """
    )
    print_table("SCENARIO 2 - REFERRAL DEFENSE CASES", df)
    export_output(df, "scenario_2_referral_defense.csv")


def show_presumptive_risk(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
            vehicle_category,
            gross_monthly_income,
            net_income_defense_position,
            net_income_presumptive_4_smgv,
            imss_status_defense_position,
            imss_status_presumptive_4_smgv
        FROM view_presumptive_risk_cases
        ORDER BY vehicle_category, net_income_presumptive_4_smgv DESC;
        """
    )
    print_table("SCENARIO 3 - PRESUMPTIVE OVERCLASSIFICATION RISK", df)
    export_output(df, "scenario_3_presumptive_risk.csv")


def show_tax_discrepancies(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
            vehicle_category,
            net_income_defense_position,
            iva_reference_base,
            iva_consistency_check
        FROM view_tax_discrepancy_cases
        ORDER BY earner_uuid;
        """
    )
    print_table("AUDIT VIEW - TAX DISCREPANCY ALERTS", df)
    export_output(df, "tax_discrepancy_alerts.csv")


def show_threshold_edge_cases(connection):
    df = query_df(
        connection,
        """
        SELECT
            earner_uuid,
            vehicle_category,
            net_income_defense_position,
            imss_status_defense_position,
            ROUND(ABS(net_income_defense_position - 9582.47), 2) AS distance_to_threshold
        FROM final_results
        ORDER BY distance_to_threshold ASC
        LIMIT 10;
        """
    )
    print_table("OPTIONAL - CLOSEST CASES TO THRESHOLD", df)
    export_output(df, "threshold_edge_cases.csv")


def show_all(connection):
    show_raw_data(connection)
    show_cleansed_income(connection)
    show_vehicle_exclusion(connection)
    show_main_classification(connection)
    show_scenario_mapping(connection)
    show_iva_clean_sample(connection)
    show_referral_defense(connection)
    show_presumptive_risk(connection)
    show_tax_discrepancies(connection)
    show_threshold_edge_cases(connection)


def main():
    parser = argparse.ArgumentParser(
        description="IMSS Platform Threshold Demo 2026"
    )

    parser.add_argument(
        "--scenario",
        choices=[
            "all",
            "raw",
            "cleansed",
            "vehicle",
            "classification",
            "mapping",
            "iva",
            "referrals",
            "presumptive",
            "tax_alerts",
            "edge_cases",
        ],
        default="all",
        help="Select which part of the demo to run."
    )

    args = parser.parse_args()

    connection = setup_database()

    if args.scenario == "all":
        show_all(connection)
    elif args.scenario == "raw":
        show_raw_data(connection)
    elif args.scenario == "cleansed":
        show_cleansed_income(connection)
    elif args.scenario == "vehicle":
        show_vehicle_exclusion(connection)
    elif args.scenario == "classification":
        show_main_classification(connection)
    elif args.scenario == "mapping":
        show_scenario_mapping(connection)
    elif args.scenario == "iva":
        show_iva_clean_sample(connection)
    elif args.scenario == "referrals":
        show_referral_defense(connection)
    elif args.scenario == "presumptive":
        show_presumptive_risk(connection)
    elif args.scenario == "tax_alerts":
        show_tax_discrepancies(connection)
    elif args.scenario == "edge_cases":
        show_threshold_edge_cases(connection)

    connection.close()


if __name__ == "__main__":
    main()