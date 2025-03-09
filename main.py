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
    index_normalized_name_column(cursor_read, cursor_write)
    duplicated_authors = get_duplicated_authors(cursor_read)

    for author in duplicated_authors:
        main_author_id = get_main_author_id(cursor_read, author["normalized_name"])
        merge_authors(cursor_read, cursor_write, author["normalized_name"], main_author_id)

    delete_normalized_name_column(cursor_write)

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


def index_normalized_name_column(cursor_read, cursor_write):
    cursor_read.execute("SHOW INDEXES FROM authors WHERE Column_name = 'normalized_name'")
    normalized_name_index = cursor_read.fetchone()

    if normalized_name_index is None:
        cursor_write.execute("ALTER TABLE authors ADD INDEX normalized_name_index (normalized_name)")


def get_duplicated_authors(cursor_read):
    cursor_read.execute("""
        SELECT normalized_name, COUNT(*) AS count
        FROM authors
        GROUP BY normalized_name
        HAVING count > 1;
    """)

    return cursor_read.fetchall()


def get_main_author_id(cursor_read, normalized_name):
    cursor_read.execute(
        """
            SELECT author_book.author_id, COUNT(*) AS books_count
            FROM authors
            LEFT JOIN author_book ON author_book.author_id = authors.id
            WHERE authors.normalized_name = %s
            GROUP BY author_book.author_id
            ORDER BY books_count DESC, author_book.author_id ASC
            LIMIT 1
        """,
        [normalized_name],
    )

    return cursor_read.fetchone()["author_id"]


def merge_authors(cursor_read, cursor_write, normalized_name, main_author_id):
    cursor_read.execute(
        "SELECT id FROM authors WHERE normalized_name = %s AND id != %s",
        [normalized_name, main_author_id],
    )
    authors = cursor_read.fetchall()

    cursor_write.executemany(
        "UPDATE author_book SET author_id = %s WHERE author_id = %s",
        [(main_author_id, author["id"]) for author in authors]
    )

    cursor_write.execute(
        "DELETE FROM authors WHERE normalized_name = %s AND id != %s",
        [normalized_name, main_author_id],
    )


def delete_normalized_name_column(cursor_write):
    cursor_write.execute("ALTER TABLE authors DROP COLUMN normalized_name")


main()
