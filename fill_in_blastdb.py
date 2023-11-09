#!/usr/bin/env python3
#
#
# all you need is in the README ;)
# The Martin Kolisko lab
# https://github.com/malyjaguar with help of the great https://github.com/Seraff


import argparse
from pathlib import Path
import mysql.connector as connector
from pprint import pprint


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


# password = input("Please type in your MySQL password here: ")
password = "marie"

config = {
  "auth_plugin":'mysql_native_password',
  "user": "marie",
  "password": password,
  "database": "blast_results_fornicata" 
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


def parse_blast_table(path_to_file):
    names = ['qseqid', 'sseqid', 'stitle', 'pident', 'length', 'matches', 'gaps', 'qstart', 'qend', 'sstart', 'send', 'stitle', 'qcovhsp', 'scovhsp', 'evalue', 'bitscore']

    with open(path_to_file) as f:
       data = []
       for line in f:
          splitted = [l.strip() for l in line.split("\t")]
          # TODO: the following line could be rewritten with 'in range' so that it appends only selected columns, right?
          data.append(dict([[names[i], splitted[i]] for i, _ in enumerate(names)]))
    return data


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


  gene_ID_dict = retrieve_gene_IDs(cursor)
      
  blast_table = parse_blast_table(args.blast_results)    
  pprint(blast_table[:2])

  for datarow in blast_table:
    columns = ['gene_id', 'sseqid', 'pident', 'length', 'matches', 'gaps', 'qstart', 'qend', 'sstart', 'send', 'qcovhsp', 'scovhsp', 'evalue', 'bitscore']
    column_string = ','.join([f'`{name}`' for name in columns]) # "`bla`, `blo`, `blu`"
    values_string = ','.join(["%s " for _ in columns])
    sql = f"INSERT INTO `hits` ({column_string}) VALUES ({values_string})"
    
    print()
    # ['qseqid', 'sseqid', 'taxonomy', 'pident', 'length', 'matches', 'gaps', 'qstart', 'qend', 'sstart', 'send', 'evalue', 'bitscore']
    # cursor.execute(f"select id from genes where gene_identifier = '{qseqid}' ")
    # gene_id = cursor.fetchone()[0] #the cursor.fetchone() puts the number we want in a tuple, that's why the [0] at the end
    # print(f"Gene ID for gene {qseqid} is {gene_id}")

    qseqid = datarow["qseqid"].strip()
    gene_id = gene_ID_dict[qseqid]

    # and a charming example of duplicated code is here!  
    values = [gene_id] 
    values.append(datarow["sseqid"])
    values.append(float(datarow["pident"]))
    values.append(int(datarow["length"]))
    values.append(int(datarow["matches"]))
    values.append(int(datarow["gaps"]))
    values.append(int(datarow["qstart"]))
    values.append(int(datarow["qend"]))
    values.append(int(datarow["sstart"]))
    values.append(int(datarow["send"]))
    values.append(float(datarow["qcovhsp"]))
    values.append(float(datarow["scovhsp"]))
    values.append(float(datarow["evalue"]))
    values.append(float(datarow["bitscore"]))
  


    cursor.execute(sql, values)
    if insert_cnt >= INSERT_BATCH_SIZE:
      conn.commit()
      insert_cnt = 0  

  conn.commit()

  ### TABLE 4 - Taxonomy  


  #Commit changes and close the connection
  
  conn.close()




