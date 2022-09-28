#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Project ：base_project
@File    ：update_pubchem_compounds.py
@Author  ：Madeline_Han
@Date    ：2022/9/26 4:41 PM
'''
import pandas as pd
from tqdm import tqdm
tqdm.pandas(desc='apply')
from rdkit import Chem
import os
os.chdir("/home/hanmy/Madeline/pubmed_data/pubchem")
import pymysql
import subprocess

def runcmd(cmd, verbose = False, *args, **kwargs)->None:
    """Using terminal in Python"""
    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip())
    pass

def rdkit_standard_smiles(smiles:str)->str:
    """standardize the smiles"""
    try:
        rdkit_standard_smiles = str(Chem.MolToSmiles(Chem.MolFromSmiles(smiles)))
    except:
        rdkit_standard_smiles = None
    return rdkit_standard_smiles

def get_rdkit_smiles(cid_name:str,dir:str) -> None:
    """Generate a new row named rdkit_smiles which is from RDKit"""
    cid_name = cid_name.split(".")[0]
    reader = pd.read_csv(os.path.join(dir,cid_name),header=None,sep="\t",chunksize=100)
    for chunks in reader:
        print("chunks",type(chunks))
        # added a new column using column[1]
        chunks[2] = chunks.progress_apply(lambda row: rdkit_standard_smiles(row[1]), axis=1)
        chunks.to_csv(os.path.join(dir,"pubchem_rdkit_smiles.tsv"),mode="a",index=False,sep="\t")
    print("The pubchem_rdkit_smiles.tsv file has generated!")
def download_pubmed(synonym_name:str,cid_name:str,dir:str)->None:
    """Download the chemicals and Synonyn,store in dir"""
    if not os.path.exists(dir+"/"+synonym_name.split(".")[0]):
        synonym_url = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/%s" % synonym_name
        synonym_cmd = "wget " + synonym_url + ";gzip " + synonym_name + " -d %s" % dir
        print("Downloading %s" % synonym_cmd)
        runcmd(synonym_cmd, verbose=True)
        print("Download %s finished!"% synonym_cmd)
    else:
        print("This file %s already exists" % synonym_name)

    if not os.path.exists(dir+"/"+cid_name.split(".")[0]):
        cid_url = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/%s" % cid_name
        cid_cmd = "wget " + cid_url + ";gzip " + cid_name + " -d %s" % dir
        print("Downloading %s" % cid_name)
        runcmd(cid_cmd, verbose=True)
        print("Download %s finished!" % cid_name)
    else:
        print("This file %s already exists" % cid_name)

def link_database():
    """link the database"""
    db = pymysql.connect(host='x.x.x.x',
                         user='user',
                         password='passwords',
                         database='database_name',
                         local_infile=1)
    return db
def delete_table(table_name:str):
    """Delete the old table,retain the structure of the table.
    Here we don't include the comparison of the old data with new data."""
    db = link_database()
    cursor = db.cursor()
    sql = "truncate table %s;" % table_name
    try:
        cursor.execute(sql)
        db.commit()
        print("Delete %s successfully!" % table_name)
    except:
        db.rollback()
    db.close()

def import_data(file_name:str,dir:str)->None:
    """Import the two table"""

    file_dir = os.path.join(dir,file_name)

    db = link_database()
    cursor = db.cursor()

    sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE pubmed_cid_smiles FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';" \
          % file_dir
    # print("sql",sql)
    try:
        cursor.execute(sql)
        db.commit()
        print("Import %s successful" % file_name)
    except:
        db.rollback()
    db.close()

def backup(backup_dir:str) ->None:
    if not os.path.exists(dir+"/"+"pubchem_data.sql"):
        print("start import")
        cmd = "mysqldump -uroot -p'passwords' --databases database_name --tables pubmed_cid_synonym_filtered pubmed_cid_smiles > %s/pubchem_data.sql" % backup_dir
        runcmd(cmd, verbose=True)
        print("Import finished")
    else:
        print("Backup file already exists！")
def delete_download_data(dir:str) ->None:
    delete_cmd = "rm -rf " + dir + "/*"
    runcmd(delete_cmd, verbose=True)

def main():
    synonym_name = 'CID-Synonym-filtered.gz'
    cid_name = 'CID-SMILES.gz'
    dir = '/home/hanmy/Madeline/pubmed_data/pubchem'
    backup_dir = '/home/hanmy/Madeline/pubmed_data/backup'
    # 1.get the data from pubchem
    download_pubmed(synonym_name,cid_name,dir)
    if os.path.exists(synonym_name.split(".")[0]) and os.path.exists(cid_name.split(".")[0]):
        # 2.deal with the data
        get_rdkit_smiles(cid_name,dir)
        # 3.backup the old data
        # backup(backup_dir)
        # 4.delete the old table
        delete_table("pubmed_cid_smiles")
        delete_table("pubmed_cid_synonym_filtered")
        # 5.import new table
        import_data("pubchem_rdkit_smiles.tsv",dir)
        import_data(synonym_name.split(".")[0],dir)
        # 6.delete the data generated during this process
        print("All is done!")

if __name__ == '__main__':
    main()