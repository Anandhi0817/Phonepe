#Package
import os
import json
import pandas as pd
import psycopg2

#aggregated transaction 
path_1 = "C:/Users/SHALINI/Desktop/phonepe/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list= os.listdir(path_1)

columns_1={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[] }

for state in agg_tran_list:
    current_state =path_1 + state + "/"
    agg_year_list = os.listdir(current_state)
    
    for year in agg_year_list:
        current_year = current_state + year + "/"
        agg_file_list = os.listdir(current_year)

        for file in agg_file_list:
            current_file = current_year + file
            data = open(current_file ,"r")
            
        A = json.load(data)

        for i in A["data"]["transactionData"]:
            name = i["name"]
            count = i["paymentInstruments"][0]["count"]
            amount = i["paymentInstruments"][0]["amount"]
            columns_1["Transaction_type"].append(name)
            columns_1["Transaction_count"].append(count)
            columns_1["Transaction_amount"].append(amount)
            columns_1["States"].append(state)
            columns_1["Years"].append(year)
            columns_1["Quarter"].append(int(file.strip(".json")))

        aggregated_transaction = pd.DataFrame(columns_1)

aggregated_transaction["States"] = aggregated_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggregated_transaction["States"] = aggregated_transaction["States"].str.replace("-"," ")
aggregated_transaction["States"] = aggregated_transaction["States"].str.title()
aggregated_transaction['States'] = aggregated_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli")

#aggregated user
path_2 = "C:/Users/SHALINI/Desktop/phonepe/pulse/data/aggregated/user/country/india/state/"
agg_user_list= os.listdir(path_2)

columns_2 = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}

for state in agg_user_list:
    current_state = path_2 + state + "/"
    agg_year_list = os.listdir(current_state)
    
    for year in agg_year_list:
        current_year = current_state + year + "/"
        agg_file_list = os.listdir(current_year)
        
        for file in agg_file_list:
            current_file = current_year + file
            data = open(current_file,"r")
            
            B = json.load(data)

            try:
                for i in B["data"]["usersByDevice"]:
                                    brand = i["brand"]
                                    count = i["count"]
                                    percentage = i["percentage"]
                                    columns_2["Brands"].append(brand)
                                    columns_2["Transaction_count"].append(count)
                                    columns_2["Percentage"].append(percentage)
                                    columns_2["States"].append(state)
                                    columns_2["Years"].append(year)
                                    columns_2["Quarter"].append(int(file.strip(".json")))
            except:
                    pass

aggregated_user= pd.DataFrame(columns_2)           

aggregated_user["States"] = aggregated_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggregated_user["States"] = aggregated_user["States"].str.replace("-"," ")
aggregated_user["States"] = aggregated_user["States"].str.title()
aggregated_user['States'] = aggregated_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#map transaction
path_3 = "C:/Users/SHALINI/Desktop/phonepe/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list= os.listdir(path_3)

columns_3 = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}

for state in map_tran_list:
    current_state = path_3+state+"/"
    map_year_list = os.listdir(current_state)
    
    for year in map_year_list:
        current_year = current_state+year+"/"
        map_file_list = os.listdir(current_year)
        
        for file in map_file_list:
            current_file = current_year+file
            data = open(current_file,"r")
            
            C = json.load(data)

            for i in C['data']["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns_3["District"].append(name)
                columns_3["Transaction_count"].append(count)
                columns_3["Transaction_amount"].append(amount)
                columns_3["States"].append(state)
                columns_3["Years"].append(year)
                columns_3["Quarter"].append(int(file.strip(".json")))

map_transaction = pd.DataFrame(columns_3)

map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction['States'] = map_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman")

# map user
path_4= "C:/Users/SHALINI/Desktop/phonepe/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path_4)

columns_4= {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

for state in map_user_list:
    current_state = path_4+state+"/"
    map_year_list = os.listdir(current_state)
    
    for year in map_year_list:
        current_year= current_state+year+"/"
        murrent_yearap_file_list = os.listdir(current_year)
        
        for file in map_file_list:
            current_file = current_year+file
            data = open(current_file,"r")
            
            D = json.load(data)

            for i in D["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns_4["Districts"].append(district)
                columns_4["RegisteredUser"].append(registereduser)
                columns_4["AppOpens"].append(appopens)
                columns_4["States"].append(state)
                columns_4["Years"].append(year)
                columns_4["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(columns_4)

map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")   

#top transaction
path_5= "C:/Users/SHALINI/Desktop/phonepe/pulse/data/top/transaction/country/india/state/"
top_tran_list = os.listdir(path_5)

columns_5= {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_tran_list:
    current_state = path_5+state+"/"
    top_year_list = os.listdir(current_state)
    
    for year in top_year_list:
        current_year = current_state+year+"/"
        top_file_list = os.listdir(current_year)
        
        for file in top_file_list:
            current_file = current_year+file
            data = open(current_file,"r")
            E = json.load(data)

            for i in E["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns_5["Pincodes"].append(entityName)
                columns_5["Transaction_count"].append(count)
                columns_5["Transaction_amount"].append(amount)
                columns_5["States"].append(state)
                columns_5["Years"].append(year)
                columns_5["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(columns_5)

top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#top user
path_6= "C:/Users/SHALINI/Desktop/phonepe/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path_6)

columns_6= {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in top_user_list:
    current_state = path_6+state+"/"
    top_year_list = os.listdir(current_state)

    for year in top_year_list:
        current_year = current_state+year+"/"
        top_file_list = os.listdir(current_year)

        for file in top_file_list:
            current_file = current_year+file
            data = open(current_file,"r")
            F = json.load(data)

            for i in F["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns_6["Pincodes"].append(name)
                columns_6["RegisteredUser"].append(registereduser)
                columns_6["States"].append(state)
                columns_6["Years"].append(year)
                columns_6["Quarter"].append(int(file.strip(".json")))

top_user = pd.DataFrame(columns_6)

top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()
top_user['States'] = top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#Table Creation
#pgsql connection
mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='mysql@11',
                        database='phonepe',
                        port='5432')
cursor=mydb.cursor()

#aggregated transaction table
create_query1 = '''CREATE TABLE if not exists aggregated_transaction (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Transaction_type varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount bigint
                                                                      )'''
cursor.execute(create_query1)
mydb.commit()

for index,row in aggregated_transaction.iterrows():
    insert_query1 = '''INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Transaction_type"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_query1,values)
    mydb.commit()

#aggregated user table
create_query2 = '''CREATE TABLE if not exists aggregated_user (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Brands varchar(50),
                                                                Transaction_count bigint,
                                                                Percentage float)'''
cursor.execute(create_query2)
mydb.commit()

for index,row in aggregated_user.iterrows():
    insert_query2 = '''INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Brands"],
              row["Transaction_count"],
              row["Percentage"])
    cursor.execute(insert_query2,values)
    mydb.commit()

#map_transaction_table
create_query3 = '''CREATE TABLE if not exists map_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                District varchar(50),
                                                                Transaction_count bigint,
                                                                Transaction_amount float)'''
cursor.execute(create_query3)
mydb.commit()

for index,row in map_transaction.iterrows():
            insert_query3 = '''
                INSERT INTO map_Transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['States'],
                row['Years'],
                row['Quarter'],
                row['District'],
                row['Transaction_count'],
                row['Transaction_amount']
            )
            cursor.execute(insert_query3,values)
            mydb.commit() 

#map_user_table
create_query4 = '''CREATE TABLE if not exists map_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(50),
                                                        RegisteredUser bigint,
                                                        AppOpens bigint)'''
cursor.execute(create_query4)
mydb.commit()

for index,row in map_user.iterrows():
    insert_query4 = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["RegisteredUser"],
              row["AppOpens"])
    cursor.execute(insert_query4,values)
    mydb.commit()

#top_transaction_table
create_query5 = '''CREATE TABLE if not exists top_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                pincodes int,
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''
cursor.execute(create_query5)
mydb.commit()

for index,row in top_transaction.iterrows():
    insert_query5 = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["Transaction_count"],
              row["Transaction_amount"])
    cursor.execute(insert_query5,values)
    mydb.commit()

#top_user_table
create_query6 = '''CREATE TABLE if not exists top_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Pincodes int,
                                                        RegisteredUser bigint
                                                        )'''
cursor.execute(create_query6)
mydb.commit()

for index,row in top_user.iterrows():
    insert_query6 = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                                            values(%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Pincodes"],
              row["RegisteredUser"])
    cursor.execute(insert_query6,values)
    mydb.commit()