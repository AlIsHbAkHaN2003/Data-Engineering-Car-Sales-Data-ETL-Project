# dataengineering-car_sales_data-project
Data engineering cars_sales_data project by Alishba khan  

A brief description of this project:

-------------------------------------------------About dataset-----------------------------------------------------------

This is a beginner-friendly project; a CSV file containing data on car sales is used. 
dataset-link = https://www.kaggle.com/datasets/suraj520/car-sales-data

---------------------------------------Extraction and Transformation Part-------------------------------------------------

The Pandas library of Python is used to access the dataset file and select the columns for the respected tables, and the Pymysql library of Python is used to create database connectivity. 

--------------------------------------------------Loading Part-------------------------------------------------------------

To store the data in tables, I first created tables using MySQL queries and stored the data of a particular table in a variable, then loaded the data into tables using the iterating tool to iterate over the file.
