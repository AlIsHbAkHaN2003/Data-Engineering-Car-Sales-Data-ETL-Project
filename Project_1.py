import pandas as pd
import pymysql

class Database:
    #create database connection
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
    
    def __enter__(self):
        self.connection = pymysql.Connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.connection.cursor()
        return self.cursor
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

class sales_data:
    def __init__(self,host,user,password,database):
        self.database = Database(host,user,password,database)
    def create_tables(self):
        with self.database as cursor:
            try:       
                #table1 creation query
                table1 = "CREATE TABLE IF NOT EXISTS sales_person(salesperson_id BIGINT AUTO_INCREMENT PRIMARY KEY, salesperson_name char(50) ,commission_rate FLOAT DEFAULT NULL, commission_earned FLOAT DEFAULT NULL)"
            
                #table2 creation query
                table2 = "CREATE TABLE IF NOT EXISTS customer(customer_id BIGINT AUTO_INCREMENT PRIMARY KEY,salesperson_id BIGINT NOT NULL,customer_name CHAR(50) NOT NULL ,buydate DATE NOT NULL ,FOREIGN KEY (salesperson_id ) REFERENCES sales_person(salesperson_id) ON DELETE CASCADE ON UPDATE CASCADE)"
        
                #table3 creation query
                table3 = "CREATE TABLE IF NOT EXISTS cars(car_id BIGINT AUTO_INCREMENT PRIMARY KEY,customer_id BIGINT NOT NULL,car_manufacturer char(20) NOT NULL,model_year INT DEFAULT NULL,car_model VARCHAR(20) DEFAULT NULL,sale_price INT NOT NULL,FOREIGN KEY (customer_id) REFERENCES customer(customer_id) ON DELETE CASCADE ON UPDATE CASCADE)"
                cursor.execute(table1) 
                cursor.execute(table2)
                cursor.execute(table3) 
            except Exception as e:
                print ("Error 3: there is this error in this code : ",e) 
                  
    def get_insertion_querry(self):
        try:   
            #data insertion query1
            insertion_in_table1 ="INSERT INTO sales_person(salesperson_name,commission_rate ,commission_earned) VALUES (%s,%s,%s)"
        
            #data insertion query2
            insertion_in_table2 = "INSERT INTO customer(salesperson_id,customer_name,buydate) VALUES(%s,%s,%s)"
       
            #data insertion query3
            insertion_in_table3 = "INSERT INTO cars(customer_id,car_manufacturer,model_year,car_model ,sale_price) VALUES(%s,%s,%s,%s,%s)"
            return insertion_in_table1,insertion_in_table2,insertion_in_table3  
        except Exception as e:
            print ("Error : there is this error in this code : ",e)
            

    def access_dataset(self):  
        #Access sales_person data
        try:
            sales_person = pd.read_csv('cars_sales_data.csv',encoding='UTF-8',skip_blank_lines = 'True',usecols= ['Salesperson','Commission Rate','Commission Earned'])[['Salesperson','Commission Rate','Commission Earned']]  
        #Access customer data  
            customer = pd.read_csv('cars_sales_data.csv',encoding='UTF-8',skip_blank_lines = 'True',usecols=['IndexColumn','Customer Name','Date'])[['IndexColumn','Customer Name','Date']]  
        #Access cars data     
            cars = pd.read_csv('cars_sales_data.csv',encoding='UTF-8',skip_blank_lines = 'True' ,usecols=['IndexColumn','Car Make','Car Year','Car Model','Sale Price'])[['IndexColumn','Car Make','Car Year','Car Model','Sale Price']]
            return sales_person,customer,cars
        except Exception as e:
                print ("Error : there is this error in this code : ",e)   
   
        
    def load_data_in_rdbms(self,sales_person,customer,cars,insertion_in_table1,insertion_in_table2,insertion_in_table3):     
        with self.database as cursor:
            
            try:
                #Load data from dataset into table
                for index,row in sales_person.iterrows():
                    cursor.execute(insertion_in_table1,list(row))

                #Load data from dataset into table
                for index,row in customer.iterrows():
                    cursor.execute(insertion_in_table2,list(row))
            
                #Load data from dataset into table
                for index,row in cars.iterrows():
                    cursor.execute(insertion_in_table3,list(row))
            except Exception as e:
                print ("Error : there is this error in this code : ",e)
            
if __name__=='__main__':
    sales_database = sales_data('localhost','root','zxcv.123','sales_data')
    create_table = sales_database.create_tables()
    insertion_in_table1,insertion_in_table2,insertion_in_table3 = sales_database.get_insertion_querry()    
    sales_person,customer,cars = sales_database.access_dataset()       
    sales_database.load_data_in_rdbms(sales_person,customer,cars,insertion_in_table1,insertion_in_table2,insertion_in_table3)  
    
    
        