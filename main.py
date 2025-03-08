import argparse
import dotenv
import mysql.connector
import os
import normalizer


def main():
    config = get_config()

    db_read = get_mysql_connection(config)
    cursor_read = db_read.cursor(dictionary=True)

    db_write = get_mysql_connection(config)
    cursor_write = db_write.cursor()

    create_normalized_name_column(cursor_read, cursor_write)
    populate_normalized_name_column(cursor_read, cursor_write)

    cursor_write.close()
    cursor_read.close()
    db_write.close()
    db_read.close()


def get_config():
    dotenv.load_dotenv()

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", type=str, default=db_host, help="Database host")
    parser.add_argument("--db-port", type=str, default=db_port, help="Database port")
    parser.add_argument("--db-username", type=str, default=db_username, help="Database username")
    parser.add_argument("--db-password", type=str, default=db_password, help="Database password")
    parser.add_argument("--db-name", type=str, default=db_name, help="Database name")

    return parser.parse_args()


def get_mysql_connection(config):
    return mysql.connector.connect(
        host=config.db_host,
        port=config.db_port,
        user=config.db_username,
        password=config.db_password,
        database=config.db_name,
        autocommit=True,
    )


def create_normalized_name_column(cursor_read, cursor_write):
    cursor_read.execute("SHOW COLUMNS FROM authors WHERE Field = 'normalized_name'")
    normalized_name_column = cursor_read.fetchone()

    if normalized_name_column is None:
        cursor_write.execute("ALTER TABLE authors ADD normalized_name VARCHAR(500) DEFAULT NULL")


def populate_normalized_name_column(cursor_read, cursor_write):
    cursor_read.execute("SELECT * FROM authors order by id asc")
    while True:
        authors = cursor_read.fetchmany(1000)
        if not authors:
            break

        normalized_authors = []
        for author in authors:
            name = normalizer.Normalizer.normalize(author["name"])
            normalized_authors.append((name, author["id"]))

        cursor_write.executemany(
            "UPDATE authors SET normalized_name = %s WHERE id = %s",
            normalized_authors
        )


main()
