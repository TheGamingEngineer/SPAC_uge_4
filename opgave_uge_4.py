# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 11:18:50 2025

@author: spac-30
"""


from MySQL import MySQL
import pandas as pd



mydb=MySQL(host="localhost",
                  user="root",
                  password="Velkommen25",
                  db="combined_orders")
mydb.connect()


mydb.insert_table("table1", "Data\Data\orders_combined.csv")
    