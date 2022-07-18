# **Tables**

There are two tables - Rental_data and Location.<br />
The schema of the two tables are as follows:

### **Table : Rental_data**

```id : int
property_type : String
room_type : String
bathrooms: float
bedrooms : float
minimum_nights : int
location_id : int
price : float
```

### **Table : Location**
``` id : int
city : String 
state : String
country : String
pincode : int
```



# **Tasks Performed**

Imported the MySQL table data into the HDFS using Sqoop (Using bash script)

Created two external tables in the hive and loaded the data from HDFS to Hive (Using bash script)

Performed some transformation like joins and obtained the results for: (Using bash script)

1. Find minimum, maximum and average price of the rentals. (aggregate functions)
2. Finding the number of rentals for each property type (group by clause)
3. Displaying either high price/low price for each row depending on the price and average price (case-when-then-else)
4.  Finding the number of rentals for each location_id and displayings its city, state, pincode (Joins)
5. Displaying minimum, maximum and average price, city of rentals in each location. (joins)
