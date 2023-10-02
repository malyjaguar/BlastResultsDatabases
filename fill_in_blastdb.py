#!/usr/bin/env python3
#
#


import argparse
from pathlib import Path
import mysql.connector as connector

def parse_arguments():
    usage = "./fill_in_blastdb.py"
    description = "Adding data for one transcriptome and its blast results into our MySQL database"
    parser = argparse.ArgumentParser(usage, description=description)
    parser.add_argument("-n", "--name", required=True, help="Type the species / strain name of the transcriptome")
    parser.add_argument("-t", "--transcriptome", required=True, help="Path to file with transcriptome data, fasta expected")
    parser.add_argument("-b", "--blast-results", required=True, help="Path to blast results file, tsv format expected")
    return parser.parse_args()


password = input("Please type in your MySQL password here: ")

config = {
  "user": "marie",
  "password": password
}

db_name = "blast_results_fornicata" 

if __name__ == "__main__":
    args = parse_arguments()

    organism_name = args.name
    #print(organism_name)

    conn = connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(f"USE {db_name}")
    cursor.execute(f"INSERT INTO organisms VALUES ('1', '{organism_name}')")

    #Commit changes and close the connection
    conn.commit()
    conn.close()

# import ipdb; ipdb.set_trace()




