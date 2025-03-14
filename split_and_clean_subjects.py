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

    for separator in ["--", "->", " / "]:
        print(f"Getting subjects to split with separator '{separator}'...")
        subjects = get_subjects_to_split(cursor_read, separator)

        print(f"Splitting {len(subjects)} subjects with separator '{separator}'...")
        for i, subject in enumerate(subjects):
            print(f"Splitting subject '{subject['name']}' {i+1}/{len(subjects)}...")
            split_subject(cursor_read, cursor_write, subject, separator)

    # todo: for the following functions we should check if the resulting subject already exists instead of ignoring the
    #  duplicate name error

    print("Capitalizing first letters...")
    capitalize_first_letters(cursor_write)

    print("Removing ending periods...")
    remove_ending_periods(cursor_write)

    print("Trimming spaces...")
    trim_spaces(cursor_write)

    cursor_write.close()
    cursor_read.close()
    db_write.close()
    db_read.close()

    print("Done!")


def get_subjects_to_split(cursor_read, separator):
    cursor_read.execute("SELECT * FROM subjects WHERE name LIKE %s", [f"%{separator}%"])

    return cursor_read.fetchall()


def split_subject(cursor_read, cursor_write, subject, separator):
    new_subjects = [sub.strip() for sub in subject["name"].split(separator)]

    placeholders = ", ".join(["%s"] * len(new_subjects))
    cursor_read.execute(f"SELECT id, name FROM subjects WHERE name IN ({placeholders})", new_subjects)
    subject_ids = {row["name"]: row["id"] for row in cursor_read.fetchall()}

    subjects_to_insert = [sub for sub in new_subjects if sub not in subject_ids]
    if subjects_to_insert:
        cursor_write.executemany("INSERT INTO subjects (name) VALUES (%s)", [(sub,) for sub in subjects_to_insert])

        insert_placeholders = ", ".join(["%s"] * len(subjects_to_insert))
        cursor_read.execute(f"SELECT id, name FROM subjects WHERE name IN ({insert_placeholders})", subjects_to_insert)
        for row in cursor_read.fetchall():
            subject_ids[row["name"]] = row["id"]

    cursor_read.execute("SELECT book_id FROM book_subject WHERE subject_id = %s", [subject["id"]])
    affected_books = [row["book_id"] for row in cursor_read.fetchall()]

    if affected_books:
        new_pairs = []
        for book_id in affected_books:
            for sub_id in subject_ids.values():
                new_pairs.append((book_id, sub_id))

        if new_pairs:
            cursor_write.executemany("INSERT INTO book_subject (book_id, subject_id) VALUES (%s, %s)", new_pairs)

        books_placeholder = ", ".join(["%s"] * len(affected_books))
        cursor_write.execute(
            f"DELETE FROM book_subject WHERE subject_id = %s AND book_id IN ({books_placeholder})",
            [subject["id"]] + affected_books
        )

    cursor_write.execute("DELETE FROM subjects WHERE id = %s", [subject["id"]])


def capitalize_first_letters(cursor_write):
    cursor_write.execute("""
        UPDATE IGNORE subjects
        SET name = CONCAT(UPPER(LEFT(name, 1)), SUBSTRING(name, 2))
        WHERE BINARY LEFT(name, 1) BETWEEN 'a' AND 'z'
    """)


def remove_ending_periods(cursor_write):
    cursor_write.execute("""
        UPDATE IGNORE subjects
        SET name = LEFT(name, CHAR_LENGTH(name) - 1)
        WHERE name LIKE '%.'
    """)


def trim_spaces(cursor_write):
    cursor_write.execute("""
        UPDATE IGNORE subjects
        SET name = TRIM(name)
        WHERE name LIKE ' %' OR name LIKE '% '
    """)


if __name__ == '__main__':
    main()
