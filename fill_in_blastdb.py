#!/usr/bin/env python3
#
#


import argparse
from pathlib import Path
import mysql.connector as connector
import pandas as pd 


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

def validate_input_fasta(path_to_file):
  if Path(path_to_file).is_file():
      print(f"Transcriptome file taken from {path_to_file}")
  else:
      raise Exception (f"Transcriptome file {path_to_file} not valid.")

def validate_input_blast(path_to_file):
  if Path(path_to_file).is_file():
      print(f"Blast results tsv file taken from {path_to_file}")
  else:
      raise Exception (f"Blast results tsv file {path_to_file} not valid.")


def parse_fasta():
  gene_headers = []
  with open(args.transcriptome, "r", encoding="utf-8") as f:
    # line = f.readline()
    # print(line)
    for line in f:
      if line.startswith('>') and line[1:].strip():
        genename = line[1:].strip()
        gene_headers.append(genename)
    return gene_headers


def parse_blast_table(path_to_file):
    names = ['qseqid', 'sseqid', 'taxonomy', 'pident', 'length', 'matches', 'gaps', 'qstart', 'qend', 'sstart', 'send', 'evalue', 'bitscore']
    data = pd.read_csv(path_to_file, sep = "\t", names = names)     
    blast_results = data.values.tolist()
    return blast_results

### ...aaand TADAAAA, here we go!

if __name__ == "__main__":
  organism_name = args.name
  conn = connector.connect(**config)
  cursor = conn.cursor()

  validate_input_fasta(args.transcriptome)
  validate_input_blast(args.blast_results)


  ### TABLE 1
  cursor.execute(f"INSERT INTO `organisms` (`species_identifier`) VALUES ('{organism_name}')")
  # that's it - just this :-)

  ##### NOW we need to ask the db to give us ID number for this new organism_name
  
  
  ### TABLE 2 
  # FIRST, we have to transfer an organism_id from Table 1 that matches the transcriptome's species
  cursor.execute("select id from organisms where species_identifier = 'acromantula' ")
  organism_id = cursor.fetchone()[0] #the cursor.fetchone put the number we want in a tuple, that's why the [0] at the end
  print(organism_id)
  # SECOND, we open the transcriptome file and parse it. 
  # We need to read every header line starting with > and use (insert) the string behind it
  # Yes, we could have written this code so that we only work with one input file
  # and take qseqid's from the blast results table
  # Yet, parsing the transcriptome headers will yeald to complete list of genes
  # in case we need to check those who had no hit at all in blast or whatever
  genes = parse_fasta()
  #print(genes[:6])
  """
  try:
    cursor.executemany("INSERT INTO `genes` (`gene_identifier`) VALUES (%s)", genes)
  except connector.Error as error:
    print(f"Failed to insert record into MySQL table: {error}")
  """

  ### TABLE 3
  # FIRST, we need to transfer the gene_ID from Table 2, matching it with qseqid from 
  # the input file given in -b argument
  # SECOND, we take the whole table from -b file and toss it in
  blast_table = parse_blast_table(args.blast_results)
  #for line in blast_table:
  #   print(line)

  ### TABLE 4 - Taxonomy  
  # I don't know yet

  #Commit changes and close the connection
  conn.commit()
  conn.close()


  # import ipdb; ipdb.set_trace()


