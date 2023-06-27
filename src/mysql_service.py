import time
import mysql.connector
import uuid

from typing import List
from src.setup import get_settings

def _with_mysql_client() -> mysql.connector.MySQLConnection:
    msqlSettings = get_settings()['db']
    client = mysql.connector.connect(
        host=msqlSettings['host'],
        user=msqlSettings['username'],
        password=msqlSettings['password'],
    )

    return client



def create_mysql_user(groups: List[str]):
    client = _with_mysql_client()
    cursor = client.cursor()

    # Create user
    db_user = str(uuid.uuid4()).replace('-', '')
    db_pass = str(uuid.uuid4()).replace('-', '')
    
    cursor.execute(f"drop user if exists '{db_user}';")
    cursor.execute(f"create user '{db_user}' identified by '{db_pass}';")

    # Create and assign groups
    for group in groups:
        cursor.execute(f"create role if not exists '{group}';")
        cursor.execute(f"grant select on build_test.* to '{group}';")
        cursor.execute(f"grant '{group}' to '{db_user}';")

    cursor.execute(
        f"alter user '{db_user}' default role {', '.join(groups)};")
    
    return db_user, db_pass


def delete_mysql_user(db_user: str):
    client = _with_mysql_client()
    client.execute(f"drop user if exists '{db_user}';")
