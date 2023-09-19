#!/usr/bin/env python3
import sys
import argparse
import mysql.connector as connector
# from mysql.connector import errorcode

"""def parse_arguments():
    usage = "./blast_db_creator.py"
    description = "A small bit of something that calls MySQL and prepares a database with four connected tables to store Blast results in an efficient way"
    parser = argparse.ArgumentParser(usage, description=description)
    parser.add_argument("-p", "--password", required=True, help="Provide password for your MySQL account")
    return parser.parse_args()

password = parse_arguments()
print(password)"""

password = input("Please type in your MySQL password here: ")

config = {
  "user": "marie",
  "password": password
}

db_name = "blast_results_fornicata" 

conn = connector.connect(**config)
cursor = conn.cursor()
cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
cursor.execute(f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET 'utf8'")
cursor.execute(f"USE {db_name}")
print("DB server version: ", conn.get_server_info())


# Create table 1: organisms
cursor.execute("""
    CREATE TABLE IF NOT EXISTS organisms (
        id INT AUTO_INCREMENT PRIMARY KEY,
        species_identifier VARCHAR(66) NOT NULL
    )
""")

# Create table 2: genes
cursor.execute("""
    CREATE TABLE IF NOT EXISTS genes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        organism_id INT NOT NULL,
        gene_identifier VARCHAR(36) NOT NULL,
        FOREIGN KEY (organism_id) REFERENCES organisms(id)
    )
""")

# Create table 3: hits
# TODO: if we are strict, the sseqid VARCHAR lenght can be reduced to 10
cursor.execute("""
    CREATE TABLE IF NOT EXISTS hits (
        id INT AUTO_INCREMENT PRIMARY KEY,
        gene_id INT NOT NULL,
        sseqid VARCHAR(12) NOT NULL,
        pident FLOAT NOT NULL,
        length INT NOT NULL,
        matches INT NOT NULL,
        gaps INT NOT NULL,
        qstart INT NOT NULL,
        qend INT NOT NULL,
        sstart INT NOT NULL,
        send INT NOT NULL,
        evalue FLOAT NOT NULL,
        bitscore FLOAT NOT NULL,
        FOREIGN KEY (gene_id) REFERENCES genes(id),
        INDEX (sseqid)
    )
""")

# Create table 4: taxonomy
cursor.execute("""
    CREATE TABLE IF NOT EXISTS taxonomy (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sseqid VARCHAR(12) NOT NULL,
        taxonomy TEXT NOT NULL,
        FOREIGN KEY (sseqid) REFERENCES hits(sseqid)
    )
""")

# Commit changes and close the connection
conn.commit()
conn.close()

print("Database and tables created successfully!")


