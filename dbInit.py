from pymongo import MongoClient
import psycopg2

def createMongo():
    # OPEN CONNECTION TO DESIRED DATABASE
    client = MongoClient('mongodb://localhost:27017/')
    db = client['a4_mongo']
    collection = db['games']

    # DUMMY DATA FOR GAMES COLLECTION
    games = [
        {"game": "Sly 2: Band of Thieves", "year": "2004"},
        {"game": "Halo 3", "year": "2007"},
        {"game": "Tetris", "year": "1984"},
        {"game": "Pong", "year": "1972"},
        {"game": "Stardew Valley", "year": "2016"},
    ]

    # INSERT DATA AND CLOSE CONNECTION
    collection.insert_many(games)
    client.close()

def createPostgreSQL():
    # OPEN CONNECTION TO DESIRED DATABASE
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="admin"
    )
    connection.autocommit = True
    cursor = connection.cursor()
    

    # CREATE DB
    cursor.execute("CREATE DATABASE a4_postgres")

    # CLOSE CONNECTION
    cursor.close()
    connection.close()

    # OPEN CONNECTION TO DESIRED DATABASE
    connection = psycopg2.connect(
        host="localhost",
        database="a4_postgres",
        user="postgres",
        password="admin"
    )
    connection.autocommit = True
    cursor = connection.cursor()

    # CREATE TABLE
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id SERIAL PRIMARY KEY,
            game VARCHAR(255),
            sales_price DECIMAL(10, 2),
            in_stock INTEGER
        )""")

    # INSERT DATA INTO TABLE
    cursor.execute("""
        INSERT INTO games (game, sales_price, in_stock)
        VALUES
            ('Sly 2: Band of Thieves', 19.99, 8),
            ('Halo 3', 29.99, 33),
            ('Tetris', 9.99, 27),
            ('Pong', 4.99, 6),
            ('Stardew Valley', 14.99, 0)
    """)

    # CLOSE CONNECTION
    cursor.close()
    connection.close()


def dbInit():
    createMongo()
    createPostgreSQL()
    print("Databases created successfully")

dbInit()