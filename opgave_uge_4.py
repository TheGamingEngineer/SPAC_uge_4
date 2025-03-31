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

print("Make and Use the database 'assignment'")
mydb.use_db("assignment")

print("import tables")
mydb.import_table("Orders", "Data\Data\orders.csv")
mydb.import_table("customers", "Data\Data\customers.csv")
mydb.import_table("products", "Data\Data\products.csv")
mydb.import_table("combined_orders", "Data\Data\orders_combined.csv")

print("show product tabel")
mydb.show_table("products")

print("show summary of database")
mydb.show_db()

print("Rename tabel 'Orders' to 'orders'")
mydb.rename_db("Orders","orders")

print("Rename database 'assignment' to 'business'")
mydb.rename_db("assignment","business")

print("Rename value 'webcam' in variabel 'name' in table 'products' to 'web-cam' and back")
mydb.update_table("products", "name", "Web-cam", "name", "Webcam")
mydb.show_table("products")
mydb.update_table("products", "name", "Webcam", "name", "Web-cam")
mydb.show_table("products")

print("show column 'name' from tabel 'products'")
mydb.show_col("products","name")

print("filter columns 'name' and 'price' from tabel 'products' to tabel 'test'")
mydb.filter_table("products","test",["name","price"])

print("add column 'temp' to tabel 'test' and remove it again.")
mydb.add_col("test","temp","INT")
mydb.show_table("test")
mydb.delete_col("test","temp")

print("grouped summary of tabel 'test' grouped by column 'name' and 'price'")
mydb.group_by("test", grouping=["name","price"])

print("add product 'Jack Sparrow' with price to tabel 'test' and remove it again")
mydb.add_row("test", ["name","price"],["Jack Sparrow","72"])
mydb.show_table("test")
mydb.delete_row("test", "name","Jack Sparrow")
mydb.show_table("test")

print("remove tabel 'test'")
mydb.delete_table("test")

print("Join tabels 'orders','customers', and 'products' through id")
mydb.delete_col("orders","id")
mydb.rename_col("orders", "customer", "id")
mydb.join_tables("orders","customers","purchase_history","id","LEFT")
mydb.rename_col("purchase_history","id","id_main")

mydb.rename_col("products","id","product")
mydb.join_tables("purchase_history","products","all_orders","product","LEFT")
mydb.delete_col("all_orders","product")

print("exporting all_orders table as a csv file")
mydb.export_tables("./tests/","all_orders")

#print("exporting database")
#mydb.backup_db("business")

#mydb.disconnect()    
