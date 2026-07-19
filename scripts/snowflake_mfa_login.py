"""Perform one interactive Snowflake TOTP login and cache the MFA token locally."""

from __future__ import annotations

from getpass import getpass

import snowflake.connector

from pipeline.config import get_settings


def main() -> None:
    settings = get_settings()
    if not settings.snowflake_account or not settings.snowflake_user:
        raise SystemExit("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set in .env")
    if not settings.snowflake_password:
        raise SystemExit("SNOWFLAKE_PASSWORD must be set in .env")

    passcode = getpass("Current Snowflake TOTP passcode: ")
    if not passcode.isdigit() or len(passcode) != 6:
        raise SystemExit("The TOTP passcode must contain exactly six digits")

    connection = snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        passcode=passcode,
        authenticator="username_password_mfa",
        client_request_mfa_token=True,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        role=settings.snowflake_role,
    )
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        finally:
            cursor.close()
    finally:
        connection.close()
    print("[snowflake] MFA login succeeded; the temporary MFA token was cached securely")


if __name__ == "__main__":
    main()
