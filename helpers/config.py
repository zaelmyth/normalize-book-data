import argparse
import dotenv
import os


def get():
    dotenv.load_dotenv()

    db_host = os.getenv("DB_HOST")
    db_port = int(os.getenv("DB_PORT"))
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", type=str, default=db_host, help="Database host")
    parser.add_argument("--db-port", type=int, default=db_port, help="Database port")
    parser.add_argument("--db-username", type=str, default=db_username, help="Database username")
    parser.add_argument("--db-password", type=str, default=db_password, help="Database password")
    parser.add_argument("--db-name", type=str, default=db_name, help="Database name")

    return parser.parse_args()
