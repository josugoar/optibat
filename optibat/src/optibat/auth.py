"""
Database authentication module.

It provides a simple login check for XXXX_XXXX data warehouse.
It is designed to verify credentials and connectivity, not to manage sessions or
persist connections. This separation allows the rest of the system to remain agnostic
to authentication details and to fail fast if credentials are invalid.

Author: Josu Gomez Arana (XXXX_XXXX)
"""

from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError


def login(user: str, password: str, name: str) -> bool:
    """
    Attempt to authenticate to the XXXX_XXXX database using the provided credentials.

    This function does not return a connection objectbut only checks if the credentials
    are valid and the database is reachable. This design ensures that authentication
    failures are detected early, and avoids holding open connections unnecessarily.

    Args:
        user (str): Database username.
        password (str): Database password.
        name (str): DSN (Data Source Name).

    Returns:
        bool: True if authentication succeeds, False otherwise.
    """
    # Create a SQLAlchemy engine with the given credentials and DSN.
    # USE THICK MODE TO REACH XXXX_XXXX! Without it the connection will fail
    # because it does not know about XXXX_XXXX. Alternative is to put the
    # details in the settings, but XXXX_XXXX does not want that.
    con = create_engine(
        "oracle+oracledb://@",
        connect_args={"user": user, "password": password, "dsn": name},
        pool_pre_ping=True,
        thick_mode=True,
    )
    try:
        # Because there is no access to corporate XXXX_XXXX, use personal database
        # credentials instead of LDAP.
        # CHANGE WHEN XXXX_XXXX GIVES ACCESS!
        with con.connect():
            pass
    # DatabaseError is the only exception that can be raised when authentication
    # fails. Any other exception is a bug, don not catch.
    except DatabaseError:
        return False
    else:
        return True
