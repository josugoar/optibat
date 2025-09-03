from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError


def login(user: str, password: str, name: str) -> bool:
    con = create_engine(
        "oracle+oracledb://@",
        connect_args={"user": user, "password": password, "dsn": name},
        pool_pre_ping=True,
        thick_mode=True,
    )
    try:
        with con.connect():
            pass
    except DatabaseError:
        return False
    else:
        return True
