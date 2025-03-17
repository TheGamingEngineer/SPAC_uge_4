# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 09:09:41 2025

@author: spac-30
"""

import mysql.connector
import pandas
import sys

class MySQL:
    def __init__(self,host,user,password,db):
        self.host=host
        self.user=user
        self.password=password
        self.db=db
        self.connection=None
    
    def connect(self):
        try: ## try to make connection
            self.connection=mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db)
            print("Connection Successful")
        except:
            try: 
                self.connection=mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password)
                self.connection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.db}")
                self.connection.commit()
                self.connection.cursor().close()
                print("Connection Successful")
            except mysql.connector.Error as e:
                print(f"Error:{e}")
                self.connection=None
     
    def close(self):
        if self.connection:
            self.connection.close()
            print("Session Closed")
        else:
            print("No Active Session")
     
    def create_db(self):
       if self.connection:
           
           db_check=self.connection.cursor()
           db_check.execute("SHOW DATABASES")
           self.connection.commit()
           db_check.close()
           if self.db in db_check:
               print(f"Database {self.db} Already Exists.")
           else:
               self.connection.cursor().execute(f"CREATE DATABASE {self.db}")
               self.connection.commit()
               self.connection.cursor().close()
       else:
           print("No Connection Established. Start A Session First.")

    def insert_table(self,name,tabel):
        if not type(tabel)==pandas.core.frame.DataFrame and ".csv" in tabel:
            try:
                data=pandas.read_csv(tabel,sep=",")
            except FileNotFoundError as e:
                print(f"File not found: {e}. Either the filename has been given erroneously, path to file has not been provided when needed, or file simply does not exist!")
            except PermissionError as e:
                print(f"Permission denied: {e}. Please change permissions of the file and try again.")
            except IsADirectoryError as e:
                print(f"Expected a file but found a directory: {e}. This script only handles text-files.")
            except OSError as e:
                print(f"OS error: {e}.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}.")
        elif type(tabel)==pandas.core.frame.DataFrame:
            data=tabel
        else:
            print("ERROR: For This either use a .csv file or a pandas dataframe. ")
            sys.exit()
        
        columns=", ".join([f" {col} VARCHAR(255)" for col in list(data.columns)])
        cursor= self.connection.cursor()    
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {name}  ({columns})")
        cursor.commit()
        
        cols=", ".join(list(data.columns))
        null_content=", ".join(["%s" for x in list(data.columns)])
        
        command=f"INSERT INTO {name} ({cols}) VALUES ({null_content})"
        content_tuples=list(data.itertuples(index=False,name=None))
        cursor.executemany(command,content_tuples)
        cursor.commit()
        
