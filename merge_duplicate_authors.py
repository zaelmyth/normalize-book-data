import helpers.config
import helpers.db
import helpers.normalizer
import pymysql.cursors


def main():
    config = helpers.config.get()

    db_read = helpers.db.get(config)
    cursor_read = db_read.cursor(pymysql.cursors.DictCursor)

    db_write = helpers.db.get(config)
    cursor_write = db_write.cursor(pymysql.cursors.SSCursor)

    print("Creating normalized_name column...")
    create_normalized_name_column(cursor_read, cursor_write)

    print("Populating normalized_name column...")
    populate_normalized_name_column(cursor_read, cursor_write)

    print("Indexing normalized_name column...")
    index_normalized_name_column(cursor_read, cursor_write)

    print("Merging duplicated authors...")
    merge_duplicated_authors(cursor_write)

    print("Deleting normalized_name column...")
    delete_normalized_name_column(cursor_write)

    cursor_write.close()
    cursor_read.close()
    db_write.close()
    db_read.close()

    print("Done!")


def create_normalized_name_column(cursor_read, cursor_write):
    cursor_read.execute("SHOW COLUMNS FROM authors WHERE Field = 'normalized_name'")
    normalized_name_column = cursor_read.fetchone()

    if normalized_name_column is None:
        cursor_write.execute("ALTER TABLE authors ADD normalized_name VARCHAR(500) DEFAULT NULL")


def populate_normalized_name_column(cursor_read, cursor_write):
    cursor_read.execute("SELECT * FROM authors WHERE normalized_name IS NULL ORDER BY id ASC")
    while True:
        authors = cursor_read.fetchmany(1000)
        if not authors:
            break

        normalized_authors = []
        for author in authors:
            name = helpers.normalizer.Normalizer.normalize(author["name"])
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


def merge_duplicated_authors(cursor_write):
    cursor_write.execute("DROP TABLE IF EXISTS authors_duplicates")
    cursor_write.execute("DROP TABLE IF EXISTS authors_main")

    cursor_write.execute("""
        CREATE TABLE authors_duplicates AS
        SELECT normalized_name, GROUP_CONCAT(name SEPARATOR '|||') AS names
        FROM authors
        GROUP BY normalized_name
        HAVING COUNT(*) > 1
    """)

    cursor_write.execute("""
        CREATE TABLE authors_main AS
        SELECT
            ad.normalized_name,
            (
                SELECT a2.id
                FROM authors a2
                LEFT JOIN author_book ab2 ON a2.id = ab2.author_id
                WHERE a2.normalized_name = ad.normalized_name
                GROUP BY a2.id
                ORDER BY COUNT(ab2.author_id) DESC, a2.id ASC
                LIMIT 1
            ) AS main_author_id
        FROM authors_duplicates ad
    """)

    cursor_write.execute("""
        UPDATE author_book ab
        JOIN authors a ON ab.author_id = a.id
        JOIN authors_main am ON a.normalized_name = am.normalized_name
        SET ab.author_id = am.main_author_id
        WHERE ab.author_id != am.main_author_id
    """)

    cursor_write.execute("""
        DELETE a
        FROM authors a
        JOIN authors_main am ON a.normalized_name = am.normalized_name
        WHERE a.id != am.main_author_id
    """)


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


if __name__ == '__main__':
    main()
