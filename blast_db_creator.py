import sys
import mysql.connector as connector
from mysql.connector import errorcode

config = {
  "user": "marie",
  "password": "***", # Type in your password here
  # "host": "127.0.0.1"
}

db_name = "blast_results_fornicata" 


def create_connection():
    conn = None
    try:
        conn = connector.connect(**config)
        cur = conn.cursor()
        cur.execute("USE {}".format(db_name))
        return conn
    except connector.Error as e:
        if err.errno == errorcode.ER_BAD_HOST_ERROR:
            print("Chybná adresa DB serveru", err)
        elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Zřejmě uživatelské jméno nebo heslo je chybně")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Databáze neexistuje" , db_name)
            create_database(conn)
            print("Databáze vytvořena", db_name)
            return conn
        print("Chyba připojení: ", err)
        exit(1)


def create_database(conn):
    try:
        cur = conn.cursor()
        cur.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db_name))
        cur.execute("USE {}".format(db_name))
    except connector.Error as err:
        print("Chyba databáze: ", err)
        exit(1)


def main():
    conn = create_connection()
    print("Verze DB serveru: ", conn.get_server_info())


if __name__ == '__main__':
    main()





##### SUGGESTION #####

conn = mysql.connector.connect(
    host="localhost",  # Replace with your MySQL server host
    user="username",   # Replace with your MySQL username
    password="password" # Replace with your MySQL password
)

# Create a cursor to execute SQL commands
cursor = conn.cursor()

# Create the database
database_name = "my_database"
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
conn.database = database_name

# Create table 1: organisms
cursor.execute("""
    CREATE TABLE IF NOT EXISTS organisms (
        id INT AUTO_INCREMENT PRIMARY KEY,
        species_identifier VARCHAR(255)
    )
""")

# Create table 2: genes
cursor.execute("""
    CREATE TABLE IF NOT EXISTS genes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        organism_id INT,
        gene_identifier VARCHAR(255),
        orthogroup VARCHAR(255),
        FOREIGN KEY (organism_id) REFERENCES organisms(id)
    )
""")

# Create table 3: hits
cursor.execute("""
    CREATE TABLE IF NOT EXISTS hits (
        id INT AUTO_INCREMENT PRIMARY KEY,
        gene_id INT,
        sseqid VARCHAR(255),
        pident FLOAT,
        length INT,
        matches INT,
        gaps INT,
        qstart INT,
        qend INT,
        sstart INT,
        send INT,
        evalue FLOAT,
        bitscore FLOAT,
        FOREIGN KEY (gene_id) REFERENCES genes(id)
    )
""")

# Create table 4: taxonomy
cursor.execute("""
    CREATE TABLE IF NOT EXISTS taxonomy (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sseqid VARCHAR(255),
        taxonomy TEXT,
        FOREIGN KEY (sseqid) REFERENCES hits(sseqid)
    )
""")

# Commit changes and close the connection
conn.commit()
conn.close()

print("Database and tables created successfully!")


