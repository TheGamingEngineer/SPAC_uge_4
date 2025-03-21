# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 11:18:50 2025

@author: spac-30
"""


from MySQL import MySQL




mydb=MySQL(host="localhost",
                  user="root",
                  password="Velkommen25")
mydb.connect()

mydb.use_db("combined_orders")

mydb.import_table("table1", "Data\Data\orders_combined.csv")

mydb.show_table("table1")

mydb.show_db()

mydb.rename_db("combined_orders","Orders")

mydb.disconnect()    