from pymongo import MongoClient
import psycopg2

def setup_databases():
    # CONNECT TO POSTGRESQL
    postgreSQL_connection = psycopg2.connect(
        host="localhost",
        database="a4_postgres",
        user="postgres",
        password="admin"
    )

    # CONNECT TO MONGODB
    mongo_client = MongoClient('mongodb://localhost:27017/')

    return postgreSQL_connection, mongo_client

def menu():
    print("1. Read Data")
    print("2. Write Data")
    print("3. Update Data")
    print("4. Delete Data")
    print("0. Exit")
    try:
        choice = int(input("Your choice: "))
        return choice
    except Exception:
        return -1

def main():
    postgreSQL_connection, mongo_client = setup_databases()
    # POSTGRESQL
    postgreSQL_connection.autocommit = True
    postgreSQL_cursor = postgreSQL_connection.cursor()

    # MONGODB
    db = mongo_client['a4_mongo']
    collection = db['games']

    print("Welcome to the database system!")
    print("What do you want to do?")
    while True:
        choice = menu()
        match choice:
            # READ DATA
            case 1:
                print("\nChoose where you want to read data from:")
                print("1. PostgreSQL")
                print("2. MongoDB")
                print("3: Both")
                try:
                    read_choice = int(input("Your choice: "))
                except Exception:
                    print("Invalid choice\n")
                    continue
                match read_choice:
                    case 1:
                        # GET ALL POSTGRESQL DATA AND READ ONE ROW AT A TIME
                        postgreSQL_cursor.execute("SELECT * FROM games")
                        for row in postgreSQL_cursor.fetchall():
                            print(row)
                    case 2:
                        # GET ALL MONGODB DATA AND READ ONE ROW AT A TIME
                        for data in collection.find():
                            print(data)
                    case 3:
                        # GET ALL POSTGRESQL DATA AND CHANGE THEM INTO LISTS
                        postgreSQL_entries = []
                        postgreSQL_cursor.execute("SELECT * FROM games")
                        for row in postgreSQL_cursor.fetchall():
                            postgreSQL_entries.append(list(row))

                        # GET ALL MONGODB DATA
                        mongo_entries = []
                        for data in list(collection.find()):
                            mongo_entries.append(data)

                        # JOIN THE YEAR FROM MONGODB INTO POSTGRESQL DATA
                        for i in postgreSQL_entries:
                            for j in mongo_entries:
                                if i[1] == j["game"]:
                                    i.append(j["year"])
                        
                        # PRINT THE JOINED DATA
                        for entry in postgreSQL_entries:
                            print(entry)

                    case _:
                        print("Invalid choice\n")
            # WRITE NEW DATA
            case 2:
                print("\nChoose where you want to write to:")
                print("1. PostgreSQL")
                print("2. MongoDB")
                try:
                    read_choice = int(input("Your choice: "))
                except Exception:
                    print("Invalid choice\n")
                    continue
                match read_choice:
                    case 1:
                        # WRITE DATA TO POSTGRESQL
                        game = input("\nWhat is the game called: ")
                        price = input("How much does the game cost: ")
                        stock = input("How many games are in stock: ")
                        try:
                            postgreSQL_cursor.execute("INSERT INTO games (game, sales_price, in_stock) VALUES (%s, %s, %s)", (game, price, stock))
                            print("Data inserted into PostgreSQL\n")             
                        except Exception:
                            print("Insertion failed\n")
                    case 2:
                        # WRITE DATA TO MONGODB
                        game = input("\nWhat is the game called: ")
                        try:
                            # CHECK IF YEAR VALUE IS INTEGER, RAISES EXCEPTION IF NOT
                            year = int(input("What year was the game released: "))
                            collection.insert_one({"game": game, "year": year})
                            print("Data inserted into MongoDB")
                        except ValueError:
                            print("Year must be an interger value\n")
                        except Exception:
                            print("Insertion failed\n")
                    case _:
                        print("Invalid choice\n")
            # UPDATE EXISTING DATA
            case 3:
                print("\nChoose where you want to update data in:")
                print("1. PostgreSQL")
                print("2. MongoDB")
                try:
                    read_choice = int(input("Your choice: "))
                except Exception:
                    print("Invalid choice\n")
                    continue
                match read_choice:
                    case 1:
                        game = input("\nWhat game do you want to update: ")
                        price = input("What is the new price: ")
                        stock = input("How many games are in stock: ")
                        try:
                            postgreSQL_cursor.execute("UPDATE games SET sales_price = %s, in_stock = %s WHERE game = %s", (price, stock, game))
                            print("Data updated in PostgreSQL\n")
                        except Exception:
                            print("Update failed\n")
                    case 2:
                        game = input("\nWhat game do you want to update: ")
                        try:
                            # CHECK IF YEAR VALUE IS INTEGER, RAISES EXCEPTION IF NOT
                            year = int(input("What is the new release date: "))
                            collection.update_one({"game": game}, {"$set": {"year": year}})
                            print("Data updated in MongoDB\n")
                        except ValueError:
                            print("Year must be an integer value\n")
                        except Exception:
                            print("Update failed\n")
                    case _:
                        print("Invalid choice\n")
            # DELETE DATA
            case 4:
                print("\nChoose where you want to delete data from:")
                print("1. PostgreSQL")
                print("2. MongoDB")
                try:
                    read_choice = int(input("Your choice: "))
                except Exception:
                    print("Invalid choice\n")
                    continue
                match read_choice:
                    case 1:
                        game = input("\nWhat game would you like to delete: ")
                        try:
                            postgreSQL_cursor.execute("DELETE FROM games WHERE game = %s", (game,))
                            print("Data deleted from PostgreSQL\n")
                        except Exception:
                            print("Deletion failed\n")
                    case 2:
                        game = input("\nWhat game would you like to delete: ")
                        try:
                            collection.delete_one({"game": game})
                            print("Data deleted from MongoDB\n")
                        except Exception:
                            print("Deletion failed\n")
                    case _:
                        print("Invalid choice\n")
            # EXIT
            case 0:
                print("Exiting...")
                break
            # DEFAULT CASE
            case _:
                print("Invalid choice\n")
                continue

    # CLOSE CONNECTIONS
    postgreSQL_cursor.close()
    postgreSQL_connection.close()
    mongo_client.close()

if __name__ == "__main__":
    main()