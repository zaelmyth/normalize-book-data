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

    print("Indexing normalized_name column in authors table...")
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
            normalized_authors,
        )


def index_normalized_name_column(cursor_read, cursor_write):
    cursor_read.execute("SHOW INDEXES FROM authors WHERE Column_name = 'normalized_name'")
    normalized_name_index = cursor_read.fetchone()

    if normalized_name_index is None:
        cursor_write.execute("ALTER TABLE authors ADD INDEX normalized_name_index (normalized_name)")


def merge_duplicated_authors(cursor_write):
    print("Dropping old tables...")
    cursor_write.execute("DROP TABLE IF EXISTS authors_duplicates")
    cursor_write.execute("DROP TABLE IF EXISTS authors_main")

    print("Creating authors_duplicates table...")
    cursor_write.execute(
        "SET SESSION group_concat_max_len = 1000000;")  # needed to avoid length error when there are many authors
    cursor_write.execute("""
        CREATE TABLE authors_duplicates AS
        SELECT normalized_name, GROUP_CONCAT(name SEPARATOR '|||') AS names
        FROM authors
        GROUP BY normalized_name
        HAVING COUNT(*) > 1
    """)
    cursor_write.execute("CREATE INDEX idx_authors_duplicates_normalized_name ON authors_duplicates(normalized_name)")

    print("Creating tmp_author_counts table...")
    cursor_write.execute("""
        CREATE TEMPORARY TABLE tmp_author_counts AS
        SELECT a.id, a.normalized_name, COUNT(ab.author_id) AS book_count
        FROM authors a
        LEFT JOIN author_book ab ON a.id = ab.author_id
        GROUP BY a.id, a.normalized_name
    """)
    cursor_write.execute("CREATE INDEX idx_tmp_author_counts_normalized_name ON tmp_author_counts(normalized_name)")

    print("Creating tmp_ranked_authors table...")
    cursor_write.execute("""
        CREATE TEMPORARY TABLE tmp_ranked_authors AS
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY normalized_name
            ORDER BY book_count DESC, id ASC
        ) AS rn
        FROM tmp_author_counts;
    """)

    print("Creating authors_main table...")
    cursor_write.execute("""
        CREATE TABLE authors_main AS
        SELECT ad.normalized_name, ra.id AS main_author_id
        FROM authors_duplicates ad
        INNER JOIN tmp_ranked_authors ra ON ad.normalized_name = ra.normalized_name
        WHERE ra.rn = 1
    """)

    print("Indexing normalized_name column in authors_main table...")
    cursor_write.execute("ALTER TABLE authors_main ADD INDEX normalized_name_main_index (normalized_name)")

    print("Updating author_book table...")
    cursor_write.execute("""
        UPDATE author_book ab
        JOIN (
            SELECT a.id AS duplicate_id, am.main_author_id
            FROM authors a
            JOIN authors_main am ON a.normalized_name = am.normalized_name
            WHERE a.id != am.main_author_id
        ) AS authors_map ON ab.author_id = authors_map.duplicate_id
        SET ab.author_id = authors_map.main_author_id;
    """)

    print("Deleting duplicated authors...")
    cursor_write.execute("""
        DELETE a
        FROM authors a
        JOIN authors_main am ON a.normalized_name = am.normalized_name
        WHERE a.id != am.main_author_id
    """)


def delete_normalized_name_column(cursor_write):
    cursor_write.execute("ALTER TABLE authors DROP COLUMN normalized_name")


if __name__ == '__main__':
    main()
