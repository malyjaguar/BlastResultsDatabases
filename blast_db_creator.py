#!/usr/bin/env python3
#
#
### BLAST DATABASE CREATOR ###
#
# This little thingy prepares a MySQL database with four connected tables 
# which we will use to store laaaaaarge amount of data from blasting whole transcriptomes of dozens of species
# The Martin Kolisko lab
# Marie Pazoutova, https://github.com/malyjaguar

 
import mysql.connector as connector


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
        species_identifier VARCHAR(64) NOT NULL
    )
""")

# Create table 2: genes
cursor.execute("""
    CREATE TABLE IF NOT EXISTS genes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        organism_id INT NOT NULL,
        gene_identifier VARCHAR(128) NOT NULL,
        FOREIGN KEY (organism_id) REFERENCES organisms(id)
    )
""")

# Create table 3: hits
# TODO: if we are strict, the sseqid VARCHAR lenght can be reduced to 10
cursor.execute("""
    CREATE TABLE IF NOT EXISTS hits (
        id INT AUTO_INCREMENT PRIMARY KEY,
        gene_id INT NOT NULL,
        sseqid VARCHAR(16) NOT NULL,
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
        sseqid VARCHAR(16) NOT NULL,
        taxonomy TEXT NOT NULL,
        FOREIGN KEY (sseqid) REFERENCES hits(sseqid)
    )
""")

# Commit changes and close the connection
conn.commit()
conn.close()

print("Database and tables created successfully!")


