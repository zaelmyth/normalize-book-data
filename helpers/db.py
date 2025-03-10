import pymysql


def get(config):
    return pymysql.connect(
        host=config.db_host,
        port=config.db_port,
        user=config.db_username,
        password=config.db_password,
        database=config.db_name,
        autocommit=True,
    )
