from mysql.connector import connect, Error
from mysqlx import errorcode

import yaml

TABLES = {}

TABLES["users"] = """
CREATE TABLE users(
    id VARCHAR(256) PRIMARY KEY, 
    name VARCHAR(256),
    url VARCHAR(256)
);
"""

TABLES["friends"] = """
CREATE TABLE friends( 
    id VARCHAR(256) PRIMARY KEY, 
    name VARCHAR(256),
    url VARCHAR(256)
);
"""

TABLES["user_friend"] = """
CREATE TABLE user_friend (
    user_id VARCHAR(256),
    friend_id VARCHAR(256),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(friend_id) REFERENCES friends(id),
    PRIMARY KEY(user_id, friend_id)
)
"""

DB_NAME = "twitter_users"

INSERT_USER_QUERY = """
INSERT IGNORE INTO users
(id, name, url)
VALUES ( %s, %s, %s )
"""

INSERT_FRIEND_QUERY = """
INSERT IGNORE INTO friends
(id, name, url)
VALUES ( %s, %s, %s )
"""

INSERT_USER_FRIEND_QUERY = """
INSERT IGNORE INTO user_friend
(user_id, friend_id)
VALUES ( %s, %s )
"""

DELETE_USER_FRIEND_QUERY = """
DELETE FROM user_friend
"""

DELETE_USER_QUERY = """
DELETE FROM users
"""

DELETE_FRIEND_QUERY = """
DELETE FROM friends
"""

SELECT_USER_FRIENDS_COUNT = """
SELECT COUNT(*) as num
FROM user_friend
WHERE user_id=%s
"""

SELECT_USER_FRIENDS = """
SELECT uf.friend_id, f.name
FROM user_friend as uf
INNER JOIN friends as f
    ON uf.friend_id = f.id
WHERE uf.user_id=%s
"""

SELECT_USER_COUNT = """
SELECT COUNT(*) as num
FROM users
WHERE id=%s
"""

with open('db.yaml') as f:
    db_config = yaml.safe_load(f)

connection = None


def connect_to_db():
    global connection
    if connection is not None:
        return connection
    try:
        connect(**db_config)
        connection = connect(**db_config)
        create_db()
        create_tables()
        return connection
    except Error as e:
        print(e)
        exit(1)


def disconnect_from_db():
    global connection
    cursor = connection.cursor()
    cursor.close()
    connection.close()
    connection = None


def create_db():
    cursor = connection.cursor()
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except Error as err:
        print("Database {} cannot be used.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB_NAME))
            connection.database = DB_NAME
        else:
            connection.close()
            print(err)
            exit(1)


def create_tables():
    cursor = connection.cursor()
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
            print("OK")
        except Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
                connection.close()
                exit(1)


def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


def insert_users(users):
    with connection.cursor() as cursor:
        cursor.executemany(INSERT_USER_QUERY, users)
        connection.commit()


def insert_friends(friends):
    with connection.cursor() as cursor:
        cursor.executemany(INSERT_FRIEND_QUERY, friends)
        connection.commit()


def insert_user_friends(user_friend):
    with connection.cursor() as cursor:
        cursor.executemany(INSERT_USER_FRIEND_QUERY, user_friend)
        connection.commit()


def clean_up_users():
    with connection.cursor() as cursor:
        cursor.execute(DELETE_USER_QUERY)
        connection.commit()


def clean_up_friends():
    with connection.cursor() as cursor:
        cursor.execute(DELETE_FRIEND_QUERY)
        connection.commit()


def clean_up_user_friends():
    with connection.cursor() as cursor:
        cursor.execute(DELETE_USER_FRIEND_QUERY)
        connection.commit()


def fill_db(data):
    users = []
    friends = []
    user_friend = []
    for user in data:
        users.append((user["id"], user["name"], user["url"]))
        for friend in user["friends"]:
            friends.append((friend["id"], friend["name"], user["url"]))
            user_friend.append((user["id"], friend["id"]))
    insert_users(users)
    insert_friends(friends)
    insert_user_friends(user_friend)


def clean_up_db():
    clean_up_user_friends()
    clean_up_users()
    clean_up_friends()


def get_friends_count_from_db(user_id):
    with connection.cursor() as cursor:
        cursor.execute(SELECT_USER_FRIENDS_COUNT, (user_id,))
        return cursor.fetchone()


def get_friends_from_db(user_id):
    with connection.cursor() as cursor:
        cursor.execute(SELECT_USER_FRIENDS, (user_id,))
        return cursor.fetchall()


def write_new_friends_to_db(user_id, new_friends):
    friends = []
    user_friend = []
    for friend in new_friends:
        friends.append((friend["id"], friend["name"], friend["url"]))
        user_friend.append((user_id, friend["id"]))
    insert_friends(friends)
    insert_user_friends(user_friend)


def write_new_users_to_db(new_users):
    users = []
    for user in new_users:
        users.append((user["id"], user["name"], user["url"]))
    insert_users(users)


def user_not_in_db(user_id):
    with connection.cursor() as cursor:
        cursor.execute(SELECT_USER_COUNT, (user_id,))
        return cursor.fetchone()[0] < 1
