# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 09:09:41 2025

@author: spac-30
"""

import mysql.connector

class connect_MySQL:
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
        except mysql.connector.Error as e:
            print(f"Error:{e}")
            self.connection=None
     
    def close(self):
        if self.connection:
            self.connection.close()
            print("Session Closed")
        else:
            print("No Active Session")
     