# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 11:18:50 2025

@author: spac-30
"""
import connect_MySQL

mydb=connect_MySQ(host="localhost",
                  user="root",
                  password=,
                  db=)


class MySQL:
    def __init__():
        self=0
    
    def create_db(self, name):
       if self.connection:
           
           db_check=self.connection.cursor().execute("SHOW DATABASES")
           if name in db_check:
               print(f"Database {name} Already Exists.")
           else:
               self.connection.cursor().execute(f"CREATE DATABASE {name}")
       else:
           print("No Connection Established. Start A Session First.")
       
       