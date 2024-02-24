import os
import json
import time
import pymysql
import pandas as pd
import streamlit as st
import plotly.express as px

#aggregated transaction 
path_1 = r"C:\Phonepe\pulse\data\aggregated\transaction\country\india\state"
aggregated_transaction= os.listdir(path_1)

D1 = {"State":[], "Year":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[]}

for state in aggregated_transaction:
    current_state = os.path.join(path_1, state)
    agg_year_list = os.listdir(current_state)
    
    for year in agg_year_list:
        current_year = os.path.join(current_state, year)
        agg_file_list = os.listdir(current_year)
        
        for file in agg_file_list:
            current_file = os.path.join(current_year, file)
            with open(current_file, "r") as data_file:
                    A = json.load(data_file)
                    print(A["data"])
                    
                    for i in A["data"]["transactionData"]:
                        name = i["name"]
                        count = i["paymentInstruments"][0]["count"]
                        amount = i["paymentInstruments"][0]["amount"]
                        D1['Transaction_type'].append(name)
                        D1['Transaction_count'].append(count)
                        D1['Transaction_amount'].append(amount)
                        D1['State'].append(state)
                        D1['Year'].append(year)
                        D1["Quarter"].append(int(file.strip(".json")))

aggregated_transaction = pd.DataFrame(D1)

#aggregated user
path_2 = r"C:\Phonepe\pulse\data\aggregated\user\country\india\state"
aggregated_user = os.listdir(path_2)

D2 = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}

for state in aggregated_user:
    current_state = os.path.join(path_2, state)
    agg_year_list = os.listdir(current_state)
    
    for year in agg_year_list:
        current_year = os.path.join(current_state, year)
        agg_file_list = os.listdir(current_year)
        
        for file in agg_file_list:
            current_file = os.path.join(current_year, file)
            with open(current_file, "r") as data_file:
                    B = json.load(data_file)
                    print(B["data"])

                    users = B["data"]["aggregated"]["registeredUsers"]

                    if B["data"]["usersByDevice"] is not None:
                        for i in B["data"]["usersByDevice"]:
                            brand = i["brand"]
                            count = i["count"]
                            percentage = i["percentage"]
                            D2["Brands"].append(brand)
                            D2["Transaction_count"].append(count)
                            D2["Percentage"].append(percentage)
                            D2["States"].append(state)
                            D2["Years"].append(year)
                            D2["Quarter"].append(int(file.strip(".json")))
                    else:
                        print("No data available for users by device !")

aggregated_user = pd.DataFrame(D2)

#map transaction
path_3 = r"C:\Phonepe\pulse\data\map\transaction\hover\country\india\state"
map_transaction = os.listdir(path_3)

D3 = {"State": [], "Year": [], "Quarter": [], "District": [], "Transaction_count": [], "Transaction_amount": []}

for state in map_transaction:
    current_state = os.path.join(path_3, state)
    map_year_list = os.listdir(current_state)

    for year in map_year_list:
        current_year = os.path.join(current_state, year)
        map_file_list = os.listdir(current_year)

        for file in map_file_list:
            current_file = os.path.join(current_year, file)
            with open(current_file, "r") as data_file:
                    C = json.load(data_file)
                    print(C["data"])
                
                    for i in C['data']["hoverDataList"]:
                        name = i["name"]
                        count = i["metric"][0]["count"]
                        amount = i["metric"][0]["amount"]
                        D3["District"].append(name)
                        D3["Transaction_count"].append(count)
                        D3["Transaction_amount"].append(amount)
                        D3["State"].append(state)
                        D3["Year"].append(year)
                        D3["Quarter"].append(int(file.strip(".json")))

map_transaction = pd.DataFrame(D3)

# map user
path_4=r"C:\Phonepe\pulse\data\map\user\hover\country\india\state"
map_user = os.listdir(path_4)

D4= {"State":[], "Year":[], "Quarter":[], "District":[], "RegisteredUser":[], "AppOpens":[]}

for state in map_user:
    current_state = os.path.join(path_4, state)
    map_year_list = os.listdir(current_state)

    for year in map_year_list:
        current_year= os.path.join(current_state, year)
        map_file_list = os.listdir(current_year)

        for file in map_file_list:
            current_file = os.path.join(current_year, file)
            with open(current_file, "r") as data_file:
                    D = json.load(data_file)
                    print(D["data"])

                    for i in D["data"]["hoverData"].items():
                            district = i[0]
                            registeredUsers = i[1]["registeredUsers"]
                            appopens = i[1]["appOpens"]
                            D4["District"].append(district)
                            D4["RegisteredUser"].append(registeredUsers)
                            D4["AppOpens"].append(appopens)
                            D4["State"].append(state)
                            D4["Year"].append(year)
                            D4["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(D4)

#top_transaction
path_5 = r"C:\Phonepe\pulse\data\top\transaction\country\india\state"
top_transaction = os.listdir(path_5)

D5 = {"State": [], "Year": [], "Quarter": [], "Pincodes": [], "Transaction_count": [], "Transaction_amount": []}

for state in top_transaction:
    current_state = os.path.join(path_5, state)
    top_year_list = os.listdir(current_state)

    for year in top_year_list:
        current_year = os.path.join(current_state, year)
        top_file_list = os.listdir(current_year)

        for file in top_file_list:
            current_file = os.path.join(current_year, file)
            with open(current_file, "r") as data_file:
                E = json.load(data_file)
                print(E["data"])

                for i in E["data"]["pincodes"]:
                    entityName = i["entityName"]
                    count = i["metric"]["count"]
                    amount = i["metric"]["amount"]
                    D5["Pincodes"].append(entityName)
                    D5["Transaction_count"].append(count)
                    D5["Transaction_amount"].append(amount)
                    D5["State"].append(state)
                    D5["Year"].append(year)
                    D5["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(D5)

#top user
path_6= r"C:\Phonepe\pulse\data\top\user\country\india\state"
top_user = os.listdir(path_6)

D6 = {"State":[], "Year":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in top_user:
    current_state = os.path.join(path_6, state)
    top_year_list = os.listdir(current_state)

    for year in top_year_list:
        current_year = os.path.join(current_state, year)
        top_file_list = os.listdir(current_year)

        for file in top_file_list:
            current_file = os.path.join(current_year, file)
            with open(current_file, "r") as data_file:
                    F = json.load(data_file)
                    print(F["data"])

                    for i in F["data"]["pincodes"]:
                        name = i["name"]
                        registeredUsers = i["registeredUsers"]
                        D6["Pincodes"].append(name)
                        D6["RegisteredUser"].append(registeredUsers)
                        D6["State"].append(state)
                        D6["Year"].append(year)
                        D6["Quarter"].append(int(file.strip(".json")))

top_user = pd.DataFrame(D6)

#SQL Connection
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Shalini@11')
cursor = myconnection.cursor()

# Establish Connection with current databse
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Shalini@11',database = "Phonepe")
cursor = myconnection.cursor()

# SQL query to create the table for aggregated_transaction
create_query = '''CREATE TABLE IF NOT EXISTS aggregated_transaction (
                    State VARCHAR(50),
                    Year INT,
                    Quarter INT,
                    Transaction_type VARCHAR(50),
                    Transaction_count BIGINT,
                    Transaction_amount BIGINT
                  )'''
cursor.execute(create_query)
cursor = myconnection.cursor()

for _, row in aggregated_transaction.iterrows():
    insert_query = '''INSERT INTO aggregated_transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''

    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["Transaction_type"],
              row["Transaction_count"],
              row["Transaction_amount"]
             )

    cursor.execute(insert_query, values)
    myconnection.commit()

# SQL query to create the table for aggregated_user 
create_query = '''CREATE TABLE if not exists aggregated_user (States varchar(50),
                                                              Years int,
                                                              Quarter int,
                                                              Brands varchar(50),
                                                              Transaction_count bigint,
                                                              Percentage float)'''
cursor.execute(create_query)
cursor = myconnection.cursor()

for _, row in aggregated_user.iterrows():
    insert_query = '''INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                      VALUES (%s, %s, %s, %s, %s, %s)'''

    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Brands"],
              row["Transaction_count"],
              row["Percentage"]
             )

    cursor.execute(insert_query, values)
    myconnection.commit()

if aggregated_user.empty:
    print("No data available for users by device !")
else:
    cursor.execute(create_query)

# SQL query to create the table for map_transaction 
create_query = '''CREATE TABLE if not exists map_transaction (State varchar(50),
                                                              Year int,
                                                              Quarter int,
                                                              District varchar(50),
                                                              Transaction_count bigint,
                                                              Transaction_amount float)'''
cursor.execute(create_query)
cursor = myconnection.cursor()

for _, row in map_transaction.iterrows():
    insert_query = '''INSERT INTO map_transaction (State, Year, Quarter, District, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''

    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["District"],
              row["Transaction_count"],
              row["Transaction_amount"]
             )

    cursor.execute(insert_query, values)
    myconnection.commit()

# SQL query to create the table for map_user 
create_query = '''CREATE TABLE IF NOT EXISTS map_user (State varchar(50),
                                                        Year int,
                                                        Quarter int,
                                                        District varchar(50),
                                                        RegisteredUser bigint,
                                                        Appopens BIGINT)'''                                                                    
cursor.execute(create_query)
cursor = myconnection.cursor()

for _, row in map_user.iterrows():
    insert_query = '''INSERT INTO map_user (State, Year, Quarter, District, RegisteredUser,AppOpens)
                      VALUES (%s, %s, %s, %s, %s, %s)'''

    values = (row["State"], 
              row["Year"], 
              row["Quarter"], 
              row["District"],
              row["AppOpens"],
              row["RegisteredUser"])

    cursor.execute(insert_query, values)
    myconnection.commit()

# SQL query to create the table for top_transaction
create_query = '''CREATE TABLE IF NOT EXISTS top_transaction (
                    State VARCHAR(50),
                    Year INT,
                    Quarter INT,
                    Pincodes INT,
                    Transaction_count BIGINT,
                    Transaction_amount BIGINT
                  )'''
cursor.execute(create_query)
cursor = myconnection.cursor()

for _, row in top_transaction.iterrows():
    insert_query = '''INSERT INTO top_transaction (State, Year, Quarter, Pincodes, Transaction_count, Transaction_amount)
                      VALUES (%s, %s, %s, %s, %s, %s)'''

    values = (row["State"], 
              row["Year"], 
              row["Quarter"], 
              row["Pincodes"],
              row["Transaction_count"], 
              row["Transaction_amount"])

    cursor.execute(insert_query, values)
    myconnection.commit()

# SQL query to create the table for top_user
create_query = '''CREATE TABLE IF NOT EXISTS top_user (
                    State VARCHAR(50),
                    Year INT,
                    Quarter INT,
                    Pincodes INT,
                    RegisteredUser BIGINT
                )'''
cursor.execute(create_query)
cursor = myconnection.cursor()

for _, row in top_user.iterrows():
    insert_query = '''INSERT INTO top_user (State, Year, Quarter, Pincodes, RegisteredUser)
                      VALUES (%s, %s, %s, %s, %s)'''

    values = (row["State"], 
              row["Year"], 
              row["Quarter"], 
              row["Pincodes"],
              row["RegisteredUser"])

    cursor.execute(insert_query, values)
    myconnection.commit()

#Tables
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print(tables)

#SQL Connection
# Establish Connection with current databse
myconnection = pymysql.connect(host = 'localhost',user='root',password='Shalini@11',database = "Phonepe")
cursor = myconnection.cursor()

#Streamlit part
st.markdown("<div style='text-align: center;'><h1 style='color: Purple; font-size: 25px; font-family: Arial, sans-serif;'>PHONEPE DATA VISUALIZATION</h1></div>", unsafe_allow_html=True)

with st.sidebar:
    selected = st.selectbox("Select", ["Home", "Geo-Data Visualization", "Top Charts Data Analysis"])

#tab1, tab2, tab3 = st.tabs(["Home Page","Geo-Data Visualization","Top Charts Data Analysis"])

if selected == "Home":
    st.subheader('Welcome to our PhonePe Data Analytics Dashboard!')
    st.write(
    "Explore, analyze, and visualize data like never before. Uncover insights and trends that empower smarter decision-making.\n"
    "Geo_Data Visualization! - Explore dynamic geodata visualization that brings your data to life on maps. Discover geographic trends and patterns with interactive maps that provide valuable insights into your data.\n"
    "Interactive Charts! - Dive into information with interactive charts that transform numbers into visual stories. From bar graphs to pie charts, explore data from different angles.\n"
    "Top Questions Answered! - Get instant answers to your top questions. Our dashboard provides key insights, addressing queries on demand, so you can make informed decisions swiftly.")
    st.markdown('<div class="icon-container"><img src="C:/Phonepe/Phonepe.png" width="60"></div>', unsafe_allow_html=True)
    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("© 2024 Phonepe Data Visualization Team")

if selected == "Geo-Data Visualization":
    year = [2018, 2019, 2020, 2021, 2022, 2023]
    quarter = [1, 2, 3, 4]
    type  = ["Transaction", "Total_Users"]

    select_bar1,select_bar2,select_bar3 = st.columns(3)
    with select_bar1:
        Type = st.selectbox("Select the type",options = type)
    with select_bar2:
        Year = st.selectbox("Select a year",options = year)
    with select_bar3:
        Quarter = st.selectbox("Select a Quarter",options = quarter)
    
    st.subheader("Live Geo-Data Visualization Section")
    st.write("Add our visualization or data exploration components here for live geo-data visualization.")
    
    if Type == "Transaction":
        query_1 = f"SELECT State, SUM(Transaction_count) AS No_of_Transactions, SUM(Transaction_amount) AS Total_amount \
              FROM map_transaction WHERE year = {Year} AND quarter = {Quarter} \
              GROUP BY State, Year, Quarter \
              ORDER BY State, Year, Quarter"
        cursor.execute(query_1)
        tables = cursor.fetchall()
        df_tables = pd.DataFrame(tables, columns=['State', 'No_of_Transactions', 'Total_amount'])
        df_tables.No_of_Transactions = df_tables.No_of_Transactions.astype(float)
        df_tables['State'] = df_tables['State'].map({"andaman-&-nicobar-islands":"Andaman & Nicobar", "andhra-pradesh":"Andhra Pradesh",
                                           "arunachal-pradesh":"Arunachal Pradesh","assam":"Assam","bihar":"Bihar","chandigarh":"Chandigarh",
                                           "chhattisgarh":"Chhattisgarh","dadra-&-nagar-haveli-&-daman-&-diu":"Dadra and Nagar Haveli and Daman and Diu",
                                           "delhi":"Delhi", "goa":"Goa","gujarat":"Gujarat","haryana":"Haryana","himachal-pradesh":"Himachal Pradesh",
                                           "jammu-&-kashmir":"Jammu & Kashmir","jharkhand":"Jharkhand", "karnataka":"Karnataka", "lakshadweep":"Lakshadweep",
                                           "madhya-pradesh":"Madhya Pradesh","maharashtra":"Maharashtra","manipur":"Manipur","meghalaya":"Meghalaya",
                                           "mizoram":"Mizoram","nagaland":"Nagaland","odisha":"Odisha","puducherry":"Puducherry","punjab":"Punjab",
                                           "rajasthan":"Rajasthan","sikkim":"Sikkim","tamil-nadu":"Tamil Nadu","telangana":"Telangana","tripura":"Tripura",
                                           "uttar-pradesh":"Uttar Pradesh","uttarakhand":"Uttarakhand","west-bengal":"West Bengal"}) 
    
        # Load India GeoJSON file
        india_geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    
        fig = px.choropleth(
            df_tables,
            geojson=india_geojson_url,
            featureidkey='properties.ST_NM',
            locations='State',  
            color='No_of_Transactions',
            hover_name='State',  
            color_continuous_scale='earth',
            title='Aggregated Transaction Data by State'
        )
    
        fig.update_geos(fitbounds="locations", visible=False)
        with st.spinner("Please wait! Generating map..."):
            time.sleep(1)
            st.plotly_chart(fig, use_container_width=True)
    
    if Type == "Total_Users":
        query_2 = f"SELECT State, SUM(RegisteredUser) AS Total_Users, SUM(AppOpens) AS Total_AppOpens \
              FROM map_user WHERE year = {Year} AND quarter = {Quarter} \
              GROUP BY State ORDER BY State"
        cursor.execute(query_2)
        tables = cursor.fetchall()
        df_tables = pd.DataFrame(tables, columns=['State', 'Total_Users', 'Total_AppOpens'])
        df_tables.Total_Users = df_tables.Total_AppOpens.astype(float)
        df_tables['State'] = df_tables['State'].map({"andaman-&-nicobar-islands":"Andaman & Nicobar", "andhra-pradesh":"Andhra Pradesh",
                                           "arunachal-pradesh":"Arunachal Pradesh","assam":"Assam","bihar":"Bihar","chandigarh":"Chandigarh",
                                           "chhattisgarh":"Chhattisgarh","dadra-&-nagar-haveli-&-daman-&-diu":"Dadra and Nagar Haveli and Daman and Diu",
                                           "delhi":"Delhi", "goa":"Goa","gujarat":"Gujarat","haryana":"Haryana","himachal-pradesh":"Himachal Pradesh",
                                           "jammu-&-kashmir":"Jammu & Kashmir","jharkhand":"Jharkhand", "karnataka":"Karnataka", "lakshadweep":"Lakshadweep",
                                           "madhya-pradesh":"Madhya Pradesh","maharashtra":"Maharashtra","manipur":"Manipur","meghalaya":"Meghalaya",
                                           "mizoram":"Mizoram","nagaland":"Nagaland","odisha":"Odisha","puducherry":"Puducherry","punjab":"Punjab",
                                           "rajasthan":"Rajasthan","sikkim":"Sikkim","tamil-nadu":"Tamil Nadu","telangana":"Telangana","tripura":"Tripura",
                                           "uttar-pradesh":"Uttar Pradesh","uttarakhand":"Uttarakhand","west-bengal":"West Bengal"})
        india_geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        
        fig = px.choropleth(df_tables, geojson=india_geojson_url,
                featureidkey='properties.ST_NM',
                locations='State', 
                color='Total_Users',
                hover_name='State', 
                color_continuous_scale='Reds',
                title='User Distribution by State')
        
        fig.update_geos(fitbounds="locations", visible=False)  
        with st.spinner("Please wait! Generating map..."):
            time.sleep(1)
        st.plotly_chart(fig, use_container_width=True)

if selected == "Top Charts Data Analysis":
    st.subheader("This Analysis is Based on the Overall Data")
    questions = [
        '1. Top 10 States by Total Transaction Amount',
        '2. Top 10 Brands by Total Transaction Count',
        '3. Top 10 States by Lowest Total Transaction Amount',
        '4. Total Transaction Count by District',
        '5. Average Transaction Amount by State']

    selected_question = st.selectbox("Select a question", questions)

    if selected_question.startswith('1'):
        query_1 = '''
            SELECT State, SUM(Transaction_amount) AS Total_Transaction_Amount
            FROM aggregated_transaction
            GROUP BY State
            ORDER BY Total_Transaction_Amount DESC
            LIMIT 10
        '''
        cursor.execute(query_1)
        tables = cursor.fetchall()
        df_tables = pd.DataFrame(tables, columns=['State', 'Total_Transaction_Amount'])
        fig = px.bar(df_tables, x='State', y='Total_Transaction_Amount',
                     labels={'Total_Transaction_Amount': 'Total Transaction Amount'}, 
                     title='Top 10 States by Total Transaction Amount')
        col1, col2=st.columns(2)
        col1.plotly_chart(fig,use_container_width=True)
        col2.write(df_tables)

    elif selected_question.startswith('2'):
        query_2 = '''
            SELECT Brands, SUM(Transaction_count) AS Total_Transaction_Count
            FROM aggregated_user
            GROUP BY Brands
            ORDER BY Total_Transaction_Count DESC
            LIMIT 10
        '''
        cursor.execute(query_2)
        tables = cursor.fetchall()
        df_tables = pd.DataFrame(tables, columns=['Brands', 'Total_Transaction_Count'])
        fig = px.bar(df_tables, x='Brands', y='Total_Transaction_Count',
                     labels={'Total_Transaction_Count': 'Total Transaction Count'}, 
                     title='Top 10 Brands by Total Transaction Count')
        col1, col2=st.columns(2)
        col1.plotly_chart(fig,use_container_width=True)
        col2.write(df_tables)

    elif selected_question.startswith('3'):
        query_3 = '''
            SELECT State, SUM(Transaction_amount) AS Total_Transaction_Amount
            FROM aggregated_transaction
            GROUP BY State
            ORDER BY Total_Transaction_Amount ASC
            LIMIT 10
        '''
        cursor.execute(query_3)
        tables = cursor.fetchall()
        df_tables = pd.DataFrame(tables, columns=['State', 'Total_Transaction_Amount'])
        fig = px.bar(df_tables, x='State', y='Total_Transaction_Amount',
                     labels={'Total_Transaction_Amount': 'Total Transaction Amount'}, 
                     title='Top 10 States by Lowest Total Transaction Amount')
        col1, col2=st.columns(2)
        col1.plotly_chart(fig,use_container_width=True)
        col2.write(df_tables)

    elif selected_question.startswith('4'):
        query_4 = '''
            SELECT District, COUNT(Transaction_count) AS Total_Transaction_Count
            FROM map_transaction
            GROUP BY District
            ORDER BY Total_Transaction_Count DESC
            LIMIT 10
        '''
        cursor.execute(query_4)
        tables = cursor.fetchall()
        df_tables = pd.DataFrame(tables, columns=['District', 'Total_Transaction_Count'])
        fig = px.bar(df_tables, x='District', y='Total_Transaction_Count',
                     labels={'Total_Transaction_Count': 'Total Transaction Count'}, 
                     title='Total Transaction Count by District')
        col1, col2=st.columns(2)
        col1.plotly_chart(fig,use_container_width=True)
        col2.write(df_tables)

    elif selected_question.startswith('5'):
        query_5 = '''
            SELECT State, AVG(Transaction_amount) AS Average_Transaction_Amount
            FROM aggregated_transaction
            GROUP BY State
            LIMIT 10
        '''
        cursor.execute(query_5)
        tables = cursor.fetchall()
        df_tables = pd.DataFrame(tables, columns=['State', 'Average_Transaction_Amount'])
        fig = px.bar(df_tables, x='State', y='Average_Transaction_Amount',
                     labels={'Average_Transaction_Amount': 'Average Transaction Amount'}, 
                     title='Average Transaction Amount by State')
        col1, col2=st.columns(2)
        col1.plotly_chart(fig,use_container_width=True)
        col2.write(df_tables)
