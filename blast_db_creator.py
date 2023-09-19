#!/usr/bin/env python3
import sys
import mysql.connector as connector
# from mysql.connector import errorcode

config = {
  "user": "marie",
  "password": "", # Type in your password here
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
        species_identifier VARCHAR(66)
    )
""")

# Create table 2: genes
cursor.execute("""
    CREATE TABLE IF NOT EXISTS genes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        organism_id INT,
        gene_identifier VARCHAR(36),
        FOREIGN KEY (organism_id) REFERENCES organisms(id)
    )
""")

# Create table 3: hits
# TODO: if we are strict, the sseqid VARCHAR lenght can be reduced to 10
cursor.execute("""
    CREATE TABLE IF NOT EXISTS hits (
        id INT AUTO_INCREMENT PRIMARY KEY,
        gene_id INT,
        sseqid VARCHAR(12),
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
        FOREIGN KEY (gene_id) REFERENCES genes(id),
        INDEX (sseqid)
    )
""")

# Create table 4: taxonomy
cursor.execute("""
    CREATE TABLE IF NOT EXISTS taxonomy (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sseqid VARCHAR(12),
        taxonomy TEXT,
        FOREIGN KEY (sseqid) REFERENCES hits(sseqid)
    )
""")

# Commit changes and close the connection
conn.commit()
conn.close()

print("Database and tables created successfully!")


