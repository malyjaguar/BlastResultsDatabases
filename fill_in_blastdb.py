#!/usr/bin/env python3
#
#
# all you need is in the README ;)
# The Martin Kolisko lab
# https://github.com/malyjaguar with help of the great https://github.com/Seraff


import argparse
from pathlib import Path
import mysql.connector as connector
# from pprint import pprint


# first of all: the necessary arguments 


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
# password = "<Itypedmypasswordhere>"
database = input("Database name: ")

config = {
  "auth_plugin":'mysql_native_password',
  "user": "marie",
  "password": password,
  "database": database 
}

INSERT_BATCH_SIZE = 2048


# defining functions for file handling etc.


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


def parse_fasta(path_to_file):
  gene_headers = []
  with open(path_to_file, "r", encoding="utf-8") as f:
    for line in f:
      if line.startswith('>') and line[1:].strip():
        genename = line[1:].strip()
        gene_headers.append(genename)
    return gene_headers
    

def retrieve_gene_IDs(cursor):
  gene_ID_dict = {}
  cursor.execute("select id, gene_identifier from genes")
  table_2 = cursor.fetchall()
  for iterablestuff in table_2:
    gene_id = iterablestuff[0]
    gene_name = iterablestuff[1] 
    gene_ID_dict[gene_name] = gene_id
  return gene_ID_dict


### ...aaand TADAAAA, here we go!


if __name__ == "__main__":
  args = parse_arguments()
  organism_name = args.name
  conn = connector.connect(**config)
  cursor = conn.cursor()

  validate_input_fasta(args.transcriptome)
  validate_input_blast(args.blast_results)


  ### TABLE 1


  cursor.execute(f"INSERT INTO `organisms` (`species_identifier`) VALUES ('{organism_name}')")
  # that's it - just this :-)
  # TODO: do we want some print status to be sure whe inserted just fine?
  
  
  ### TABLE 2 
  # before parsing the data, we have first to fetch an organism_id from Table 1
  # TODO: do we want to add some error checks here? 


  cursor.execute(f"select id from organisms where species_identifier = '{organism_name}' ")
  organism_id = cursor.fetchone()[0] #the cursor.fetchone() puts the number we want in a tuple, that's why the [0] at the end
  print(f"Organism ID for species {organism_name} is {organism_id}")


  genes = parse_fasta(args.transcriptome)

  insert_cnt = 0

  # to optimalize memory usage, we load data into the table in batches
  for gene in genes: 
    sql = "INSERT INTO `genes` (`organism_id`, `gene_identifier`) VALUES (%s, %s)"
    cursor.execute(sql, (organism_id, gene))
    insert_cnt += 1
    if insert_cnt >= INSERT_BATCH_SIZE:
      conn.commit()
      insert_cnt = 0 
    
  conn.commit()

  ### TABLE 3
  # again, we first need to transfer the gene_IDs from Table 2 
  # TODO: add a try - catch loop to save incomplete lines in a log file or something


  gene_ID_dict = retrieve_gene_IDs(cursor)
      

  ### TOHLE TED POTREBUJE DODELAT!
  
  with open(args.blast_results) as f:
    for line in f:
      print(line)
      exit
    # this is how our data look like: qseqid sseqid stitle pident length mismatch gapopen qstart qend sstart send stitle qcovhsp scovhsp evalue bitscore   
      columns = ['gene_id', 'sseqid', 'pident', 'length', 'matches', 'gaps', 'qstart', 'qend', 'sstart', 'send', 'qcovhsp', 'scovhsp', 'evalue', 'bitscore']
      column_string = ','.join([f'`{name}`' for name in columns]) # "`bla`, `blo`, `blu`"
      values_string = ','.join(["%s " for _ in columns])  
      sql = f"INSERT INTO `hits` ({column_string}) VALUES ({values_string})"

      blast_row = line.strip().split('\t')


      # retrieving the gene IDs from dictionary that we prepared earlier
      qseqid = blast_row[0].strip()
      gene_id = gene_ID_dict[qseqid]
      
      # and a charming example of duplicated code is here!  
      values = [gene_id] 
      values.append(blast_row[1])
      values.append(float(blast_row[3]))
      values.append(int(blast_row[4]))
      values.append(int(blast_row[5]))
      values.append(int(blast_row[6]))
      values.append(int(blast_row[7]))
      values.append(int(blast_row[8]))
      values.append(int(blast_row[9]))
      values.append(int(blast_row[10]))
      values.append(float(blast_row[12]))
      values.append(float(blast_row[13]))
      values.append(float(blast_row[14]))
      values.append(float(blast_row[15]))
    

      cursor.execute(sql, values)
      insert_cnt += 1
      if insert_cnt >= INSERT_BATCH_SIZE:
        conn.commit()
        print("2048 hits inserted and committed")
        insert_cnt = 0  

  conn.commit()

  ### TABLE 4 - Taxonomy  
  # TODO 

  #Commit changes and close the connection
 
  conn.close()




