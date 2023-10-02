#!/usr/bin/env python3
#
#
import sys
import mysql.connector as connector


password = input("Please type in your MySQL password here: ")

config = {
  "user": "marie",
  "password": password
}

db_name = "blast_results_fornicata" 
organisms = ['acromantula', 'bundimun', 'chizpurfle', 'cupacabra', 'kelpie', 'kneazle', 'manticore'] 
import ipdb; ipdb.set_trace()


conn = connector.connect(**config)
cursor = conn.cursor()
cursor.execute(f"USE {db_name}")
 

# Commit changes and close the connection
conn.commit()
conn.close()
