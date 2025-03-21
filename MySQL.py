# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 09:09:41 2025

@author: spac-30
"""

import mysql.connector
import pandas
import sys
from tabulate import tabulate
import os


## Jeg vælger at kombinere alle MySQL-funktionerne i én kommando for at gøre det mere overskueligt og lettere at håndtere forbindelsen
class MySQL:
    # definere initielt klassen med værtscomputernavn, brugernavn, adgangskode, den brugerdefinerede database og forbindelsen til MySQL
    def __init__(self,host,user,password):
        self.host=host ## værtscomputernavn
        self.user=user ## brugernavn
        self.password=password ## adgangskode til computeren
        self.db=None ## gemmer det anvendte database navn
        self.connection=None ## forbindelse til database
    
    # funktion til at forbinde til MySQL-serveren
    def connect(self):
        try: ## prøver at oprette forbindelse til databasen med de initielle instillinger
            self.connection=mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password)
            print("Connection Successful")
        except mysql.connector.Error as e: ## Hvis det hverken er muligt at oprette forbindelse til databasen eller at oprette databasen, rejses der en generel fejl
                print(f"Error:{e}")
                self.connection=None
    
    # funktion til at lukke forbindelsen til MySQL serveren, som også giver besked, hvis der ikke er nogen aktiv session
    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Session Closed")
        else:
            print("No Active Session")
    
    # funktion til at vælge/oprette den database, som man gerne vil bruge
    def use_db(self,db):
        if self.connection:
            ## vi tjekker om databasen findes i forvejen for at der ikke skal være fejl ved oprettelse af den. 
            ## Vi vil derfor hente en liste over eksisterende databaser på serveren. 
            db_interface=self.connection.cursor()
            db_interface.execute("SHOW DATABASES")
            db_check=[x[0] for x in db_interface.fetchall()]
            db_interface.close()
            
            ## hvis databasen findes, vil den database bare vælges.
            if db in db_check:
                print(f"Database {db} Already Exists. {db} Selected As Current Database")
            ## hvis databasen ikke findes, vil den blive oprettet før den bliver valgt. 
            else:
                self.connection.cursor().execute(f"CREATE DATABASE {db}")
                self.connection.commit()
                self.connection.cursor().close()
                print(f"Currently Using Database {db}")
            self.connection.database=db
            self.db=db
        else:
            print("No Connection Established. Start A Session First.")
    
    # funktion til at vise indhold fra tabel 'table' med mulighed for at vise alt i tabellen eller bare en kolonne via 'col'. 
    ## man kan vælge om man vil se hele tabellen eller kun første række med 'fetch'
    ## hvis man vil søge på baggrund af et filter, kan man vælge at definere core_var ("corelating variable") og core_var ("corelating value")
    def show_table(self,table,col="*",fetch="all"):
        if self.connection:
            interface=self.connection.cursor()
            
            ## hvis brugeren giver en liste af kolonnenavne, vil det blive omformet til en string, som kan anvendes i kommandoen. 
            if type(col)==list:
                col=", ".join(col)
                
            ## definer kommandoen
            command=f"SELECT {col} FROM {table}"
            ## hvis brugeren kun vil have top X række, kan brugeren sætte fetch til et heltal. 
            if isinstance(fetch,int):
                command+=f" LIMIT {fetch}"
            elif fetch.lower()=="first" or fetch.lower()=="one" or fetch.lower()=="top":
                command+=" LIMIT 1"
            ## eksekver kommando
            interface.execute(command)
            
            ## definer og vis tabel
            column_names=[title[0] for title in interface.description]
            table=interface.fecthall()
            print(tabulate(table, headers=column_names, tablefmt="fancy_grid"))
            
        else:
            print("No Connection Established. Start A Session First.")
            
            
    # funktion til at slette en tabel fra den nuværende database. 
    def delete_table(self,table):
        if self.connection:    
            self.connection.cursor().execute(f"DROP TABLE IF EXISTS {table}")
        else:
            print("No Connection Established. Start A Session First.")
    
    # funktion til at slette en database fra serveren
    def delete_db(self,db):
        if self.connection:    
            ## Vi skal lige tjekke om den givne database rent faktisk findes på serveren. 
            db_interface=self.connection.cursor()
            db_interface.execute("SHOW DATABASES")
            db_check=[x[0] for x in db_interface.fetchall()]
            db_interface.close()
            if db in db_check:
                self.connection.cursor().execute(f"DROP DATABASE IF EXISTS {db}")
            else:
                print(f"ERROR: Database {db} Not Found On Server. ")
        else:
            print("No Connection Established. Start A Session First.")
    
    # funktion til at ændre værdier i en variabel i tabellen 'table' på basis af en given værdi i en variabel
    ## 'var1' og 'var2' er variabler, samt 'value1' og 'value2' er værdier til tabellen
    def update_table(self, table, var1, value1, var2, value2):
        if self.connection:  
            command=f"UPDATE {table} SET {var1} = '{value1}' WHERE {var2} = '{value2}'"
            self.connection.cursor().execute(command)
            self.connection.commit()
        else:
            print("No Connection Established. Start A Session First.")
    
    # funktion til at indsætte en tabel i databasen fra en .csv fil eller en pandas.dataframe. 
    ## her menes det, at: 
    ### 'name' er navnet på tabellen i databasen
    ### 'tabel' er den tabel, som man gerne vil indsætte i databasen
    def import_table(self,name,tabel):
        ## hvis 'tabel' indhentes direkte fra en .csv fil, vil tabellen hentes og åbnes som en pandas.dataframe
        if not type(tabel)==pandas.core.frame.DataFrame and ".csv" in tabel:
            try:
                data=pandas.read_csv(tabel,sep=",")
            except FileNotFoundError as e:
                print(f"File not found: {e}. Either the filename has been given erroneously, path to file has not been provided when needed, or file simply does not exist!")
            except PermissionError as e:
                print(f"Permission denied: {e}. Please change permissions of the file and try again.")
            except OSError as e:
                print(f"OS error: {e}.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}.")
        ## hvis 'tabel' allerede er en pandas.dataframe, vil den blot blive omdefineret for at kunne anvende det samme uden kontekstændringer. 
        elif type(tabel)==pandas.core.frame.DataFrame:
            data=tabel
        ## der kan tages højde for andre data-input (såsom fra .xlsx filer), men for denne opgave, vil der kun blive taget højde for .csv filer og pandas.dataframe's
        else:
            print("ERROR: For This either use a .csv file or a pandas dataframe. ")
            sys.exit()
        
        # opretter en tom tabel med antallet af kolonner svarende til kolonnenavnene fra 'tabel' (kodeinspiration fra www.w3schools.com) 
        columns=", ".join([f" {col} VARCHAR(255)" for col in list(data.columns)])
        self.connection.cursor().execute(f"CREATE TABLE IF NOT EXISTS {name}  ({columns})")
        self.connection.commit()
        
        # opretter de tomme celler i tabellen
        cols=", ".join(list(data.columns))
        null_content=", ".join(["%s" for x in list(data.columns)])
        
        # indsætter data i den tomme tabel 
        command=f"INSERT INTO {name} ({cols}) VALUES ({null_content})"
        content_tuples=list(data.itertuples(index=False,name=None))
        self.connection.cursor().executemany(command,content_tuples)
        self.connection.commit()

        
    # funktion, som viser indholdet af den database, som man bruger.
    ## Den viser kun tabel navn og dimensioner. Hvis man vil se indholdet i en tabel, kan man anvende show_table() funktionen istedet. 
    def show_db(self):
        if self.connection:  
            interface=self.connection.cursor()
            interface.execute("SHOW TABLES")
            tables=interface.fetchall()
            for x in tables:
                # udtrækker tabelnavn
                name=x[0]
                
                # indhenter tabellen som en list of tuples      
                interface.execute(f"SELECT * FROM {name}")
                table=interface.fetchall()
                
                # tæller kolonner ved at udregne længden af første række (aka. første "tuple" i listen)
                first_row=list(table[0])
                col_count=len(first_row)
                
                # tæller rækker ved at tælle længden af listen med "tuple"'s
                row_count=len(table)
                
                # printer række med tabelnawn: N Columns * M Rows
                print(f"{name}: {col_count} Columns * {row_count} Rows")
        else:
            print("No Connection Established. Start A Session First.")
            
    # Funktion til at flytte en database til en ny database.
    def rename_db(self,old_name,new_name):
        if self.connection:  
            interface=self.connection.cursor()
            interface.execute(f"CREATE DATABASE {new_name}")
            interface.execute(f"SHOW TABLES FROM {old_name}")
            tables=interface.fetchall()
            ## flytter hver tabel fra database 'old_name' til database 'new_name'
            for x in tables:
                name=x[0]
                interface.execute(f"RENAME TABLE {old_name}.{name} TO {new_name}.{name}")
            
            ## hvis vi flytter indhold fra en database, som vi allerede arbejder på, flytter vi os lige over til den nye database.  
            if self.db==old_name:
                self.connection.database=new_name
                self.db=new_name
            interface.execute(f"DROP DATABASE {old_name}")
            
        else:
            print("No Connection Established. Start A Session First.")
    
    # funktion til at vise et kolonne med mulige filtre.
    ## 'filters' skal være en liste over kriterier for en selektion skrevet som man vil det i SQL, f.eks "by = 'Odense'"
    ## 'andor' skal være en liste med et antal "AND"/"OR" svarende til antal kriterier -1 
    def show_col(self,table,col="*",filters=[],andor=[]):
        if self.connection:  
            interface=self.connection.cursor()
            command=f"SELECT {col} FROM {table}"
            
            ## såfremt at 'filters' og 'andor' er ikke-tomme lister, vil det, som skrives i dem blive skrevet som kommando
            if type(filters)==type(andor)==list and len(filters)!=0 and len(andor)!=0:
                ## er der filtre, skal det initieres med WHERE
                command+=" WHERE "
                ## Det vil ikke være utænkeligt at andor bliver kortere end filters, så det gøres lige lidt længere, så alle filtre kan komme med. 
                if len(andor)<len(filters):
                    andor+=[""]
                for x in zip(filters,andor):
                    ## inde i et filter kan der godt være "between","and", "in" og/eller "or", som f.eks. pris BETWEEN 0 AND 1000
                    ## men det skal sikres at være med stort, så det sikres her. 
                    if " between " in x[0] or " and " in x[0] or " or " in x[0] or " in " in x[0]:
                        filter_list=x[0].split(" ")
                        filter_list=[y.upper() for y in filter_list if y in ["between","and","or","in"]]
                        x[0]=" ".join(filter_list)
                    ## sikre os også at andor er med stor 
                    x[1]=x[1].upper()
                    ## opbygger kommandoen.
                    command+=f" {x[0]} {x[1]}"
            
            interface.execute(command)
            table=interface.fetchall()
            column_names = [column[0] for column in interface.description]
            
            print(tabulate(table, headers=column_names, tablefmt="fancy_grid"))
        else:
            print("No Connection Established. Start A Session First.")
    
    # Funktion, som filtere en tabel til en ny tabel
    def fitler_table(self,table,new_table,col="*",filters=[],andor=[]):
        if self.connection:  
            interface=self.connection.cursor()
            ## hvis brugeren vælger en liste af variabler, skal det lige bearbejdes, så det kan læses som en SQL kommando
            if type(col)==list:
                col=", ".join(col)
            ## hvis col er angivet som en streng, skal vi sikre os at den er opsat, så den er læselig for SQL.
            elif type(col)==str and ", " not in col:
                filter_list=col.split(",")
                filter_list=", ".join(filter_list)
            command=f"SELECT {col} INTO {new_table} FROM {table}"
            ## såfremt at 'filters' og 'andor' er ikke-tomme lister, vil det, som skrives i dem blive skrevet som kommando
            if type(filters)==type(andor)==list and len(filters)!=0 and len(andor)!=0:
                ## er der filtre, skal det initieres med WHERE
                command+=" WHERE "
                ## Det vil ikke være utænkeligt at andor bliver kortere end filters, så det gøres lige lidt længere, så alle filtre kan komme med. 
                if len(andor)<len(filters):
                    andor+=[""]
                for x in zip(filters,andor):
                    ## inde i et filter kan der godt være "between","and", "in" og/eller "or", som f.eks. pris BETWEEN 0 AND 1000
                    ## men det skal sikres at være med stort, så det sikres her. 
                    if " between " in x[0] or " and " in x[0] or " or " in x[0] or " in " in x[0]:
                        filter_list=x[0].split(" ")
                        filter_list=[y.upper() for y in filter_list if y in ["between","and","or","in"]]
                        x[0]=" ".join(filter_list)
                    ## sikre os også at andor er med stor 
                    x[1]=x[1].upper()
                    ## opbygger kommandoen.
                    command+=f" {x[0]} {x[1]}"
            
            interface.execute(command)
        else:
            print("No Connection Established. Start A Session First.")
    
    # Funktion til at slette kolonner fra en tabel i databasen.
    def delete_col(self,table,col):
        if self.connection:  
            interface=self.connection.cursor()
            if type(col)==list:
                col=", ".join(col)
            elif type(col)==str and ", " not in col:
                filter_list=col.split(",")
                col=", ".join(filter_list)
            command=f"ALTER TABLE {table} DROP COLUMN {col}"
            interface.execute(command)
        else:
            print("No Connection Established. Start A Session First.")
    
    # Funktion til at tilføje en kolonne til en tabel i databasen.
    def add_col(self,table,col,coltype):
        if self.connection:  
            interface=self.connection.cursor()
            if type(col)==list:
                col=", ".join(col)
            elif type(col)==str and ", " not in col:
                filter_list=col.split(",")
                col=", ".join(filter_list)
            command=f"ALTER TABLE {table} ADD COLUMN {col} {coltype}"
            interface.execute(command)
        else:
            print("No Connection Established. Start A Session First.")
    
    # funktion til at vise en grupperet tabel over data i en tabel i en database
    def group_by(self,table,col="*",grouping=[]):
        if self.connection: 
            interface=self.connection.cursor()
            ## strukturer kolonnerne, så de passer til kommandoen.
            if type(col)==list:
                col=", ".join(col)
            elif type(col)==str and ", " not in col:
                col=col.split(",")
                col=", ".join(col)
                ## strukturer grupperingskriterier, så de passer til kommandoen    
            if type(grouping)==list:
                col=", ".join(col)
            elif type(grouping)==str and ", " not in grouping:
                grouping=grouping.split(",")
                grouping=", ".join(grouping)
            ## opstil og eksekver kommando
            command=f"SELECT {col} FROM {table} GROUP BY {grouping}"
            interface.execute(command)
            table = interface.fetchall()
            column_names=[column[0] for column in interface.description]
            ## udskriv grupperet tabel
            print(tabulate(table, headers=column_names, tablefmt="fancy_grid"))
        else:
            print("No Connection Established. Start A Session First.")
    
    # funktion til at tilføje rækker til en tabel i databasen. 
    ## 'cols' og 'vals' skal være lister med henholdsvist kolonnenavne i tabellen og de værdier, som man vil indsætte.
    def add_row(self,table,cols=[],vals=[]):
        if self.connection: 
            interface=self.connection.cursor()
            ## typisk vil antal kolonner og celler være nogle stykker, så det vil være lettest at bearbejde dem i lister
            if type(cols)!=list or type(vals)!=list:
                raise TypeError("Variables 'cols' and 'vals' must be lists!")
                
            ## for insert kommandoen SKAL antal kolonner og celler stemme overens med tabellen
            interface.execute(f"SHOW COLUMNS FROM {table}")
            table_cols=len(interface.fetchall())
            if len(cols)!=len(vals)!=table_cols:
                ValueError(f"Variables 'cols' and 'vals' must be of equal lengths to the number of columns in table {table}")
        
            ## variablerne skal tilpasses kommandoen
            columns="("+", ".join(cols)+")"
            empty_cells="("+", ".join(['%s']*len(vals))+")"
            
            ## så har vi nok til kommandoen
            command=f"INSERT INTO {table} {columns} VALUES {empty_cells}"
            interface.execute(command,vals)
        else:
            print("No Connection Established. Start A Session First.")
            
    # funktion til at fjerne rækker i en tabel i databasen
    ## 'col_name' skal være en streng svarende til et kolonnenavn i tabellen
    ## 'col_val' skal bare være en enkelt værdi, som selekteres for i kolonnen.
    def delete_row(self,table,col_name,col_val):
        if self.connection: 
            interface=self.connection.cursor()
            command=f"DELETE FROM {table} WHERE {col_name} = %s"
            interface.execute(command,col_val)
        else:
            print("No Connection Established. Start A Session First.")
            
    # funktion til at vise en table for en join-query imellem to tabeller
    ## 'table' variablerne referere til tabelnavnene og skal være en streng
    ## 'col' variablberne referere til kolonnenavnet og skal være en streng med et enkelt kolonnenavn
    ## 'common_key' referere til den fællesnøgle, som anvendes til join-funktionen og skal være en streng med et enkelt kolonnenavn
    ## 'common_key' kan godt have forskellige navne i hver sin tabel, så det er derfor at der skelnes imellem højre og venstre.
    ## 'direction' definere retningen for join-query.
    ### Den kan også tage højde for full-join-query ved at skrive 'full'
    def join_tables(self,left_table,left_col,right_table,right_col,left_common_key,right_common_key,direction="inner"):
        if self.connection: 
            interface=self.connection.cursor()
            ## opsætter de primære kommandoer for join-query
            base_command=f"SELECT {left_table}.{left_col}, {right_table}.{right_col} FROM {left_table} "
            
            ## her tages der højde for at brugeren kan skrive 'direction' med småt
            direction=direction.upper()
            
            ## basis retningskommandoen defineres her. 
            direction_command=f" JOIN {right_table} ON {left_table}.{left_common_key} = {right_table}.{right_common_key}"
            
            ## her skelnes der imellem almindelige join-queries og full join-query
            if direction!="FULL":
                command=base_command + direction + direction_command
            else:
                command= base_command + "LEFT" + direction_command + " UNION " + base_command + "RIGHT" + direction_command 
                
            interface.execute(command)
            results=interface.fetchall()
            
            column_names=[title[0] for title in interface.description]
            
            print(tabulate(results, headers=column_names, tablefmt="fancy_grid"))
        else:
            print("No Connection Established. Start A Session First.")
    
    
    # Funktion, som eksportere tabeller fra databasen til computeren. 
    ## 'file_path' definere stien til mappen, som filerne skal downloades til
    ## 'table_name' definere tabellen/tabellerne, som skal hentes. 
    ## hvis 'table_name' defineres som "*" (som er standard), hentes alle 
    def export_tables(self,file_path,table_name="*"):
        if self.connection: 
            interface=self.connection.cursor()
            
            if table_name=="*":
                interface.execute("SHOW TABLES")
                tables=interface.fetchall()
            else:
                tables=[(table_name,0)]
                
            for table in tables:
                table_name=table[0]
                
                interface.execute(f"SELECT * FROM {table_name}")
                table=interface.fetchall()
                
                column_names=[col[0] for col in interface]
                
                df = pandas.DataFrame(table, columns=column_names)
                final_path = os.path.join(file_path,f"{table_name}.csv")
                df.to_csv(final_path,index=False)
                
        else:
            print("No Connection Established. Start A Session First.")