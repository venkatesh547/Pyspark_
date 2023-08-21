# Table_name:: 
  Employees

#Column details:
	id
 	name
 	country
 	Salary
 	experience
 	join_date


#Sample Queries:
 select count(*) from employee WHERE joining_date < 2023
 SELECT count(*) FROM Employee WHERE Name = 'prem'
 SELECT * FROM Employee WHERE Name ='Dennis' AND Sal=25000 AND Experience=5
 SELECT * FROM Employee WHERE Sal>18000
 SELECT * FROM Employee WHERE Experience>5




 We can use Indexing and Z-order(data-skipping algorithms) to dramatically reduce the amount of data that needs to be read.

 Data shuffle won't happen in z-order, it only co-locate the data stats range and logs it in the 'json' file under the Delta log folder.


 code::

 # repartitionByRange will group the data in different partitions.
 # if we are filtering mostly by a single column('id'), then repartitionByRange works well. But, in our case Z-order helps since we are going to use multiple filter columns

  df.repartitionByRange(10,'id')

 # OPTIMIZE ZORDER works best with partitioned tables. 
 
 # We are partitioning the data by country column to split on write operation. Since, assuming table has millions of records.

  df.write.format("delta").mode("overwrite").partitionBy('country').saveAsTable("Employees") 

  OPTIMIZE Employees ZORDER BY ('id','name','Salary','experience','join_date')

  # Note Z-order can have stats log up to 32 columns by default, which is configurable. But, more columns in Z-order may reduce performance.
