#!/usr/bin/env python3
#
#


import argparse
from pathlib import Path
import mysql.connector as connector
 

# first of all: define the necessary arguments 

def parse_arguments():
    usage = "./fill_in_blastdb.py"
    description = "Adding data for one transcriptome and its blast results into our MySQL database"
    parser = argparse.ArgumentParser(usage, description=description)
    parser.add_argument("-n", "--name", required=True, help="Type the species / strain name of the transcriptome")
    parser.add_argument("-t", "--transcriptome", required=True, help="Path to file with transcriptome data, fasta expected")
    parser.add_argument("-b", "--blast_results", required=True, help="Path to blast results file, tsv format expected")
    return parser.parse_args()


# defining some global variables

password = input("Please type in your MySQL password here: ")

config = {
  "auth_plugin":'mysql_native_password',
  "user": "marie",
  "password": password,
  "database": "blast_results_fornicata" 
}

args = parse_arguments()


# defining functions for file handling

def parse_fasta():
  def validate_input_path(path_to_file):
    if Path(path_to_file).is_file():
        print(f"Transcriptome file taken from {path_to_file}")
    else:
        # TODO: change exception to more specific 
        raise Exception (f"Transcriptome file {path_to_file} not valid.")

  validate_input_path(args.transcriptome)


def parse_blast_table():
  def validate_input_path(path_to_file):
    if Path(path_to_file).is_file():
        print(f"Blast results tsv file taken from {path_to_file}")
    else:
        # TODO: change exception to more specific 
        raise Exception (f"Blast results tsv file {path_to_file} not valid.")
    
  validate_input_path(args.blast_results)


### ...aaand TADAAAA, here we go!

if __name__ == "__main__":
  organism_name = args.name
  conn = connector.connect(**config)
  cursor = conn.cursor()

  parse_fasta()
  parse_blast_table()


  ### TABLE 1
  cursor.execute(f"INSERT INTO `organisms` (`species_identifier`) VALUES ('{organism_name}')")
  # that's it - just this :-)

  
  ### TABLE 2 
  # FIRST, we have to transfer an organism_id from Table 1 that matches the transcriptome's species
  # SECOND, we open the transcriptome file and parse it. 
  # We need to read every header line starting with > and use (insert) the string behind it
  

  ### TABLE 3
  # FIRST, we need to transfer the gene_ID from Table 2, matching it with qseqid from 
  # the input file given in -b argument
  # SECOND, we take the whole table from -b file and toss it in


  ### TABLE 4 - Taxonomy  
  # I don't know yet

  #Commit changes and close the connection
  conn.commit()
  conn.close()



  # import ipdb; ipdb.set_trace()


