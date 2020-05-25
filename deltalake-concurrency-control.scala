// Databricks notebook source
// DBTITLE 1,Prepare test data & table
dbutils.fs.mkdirs("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-base-table")

//upload test data to filestore and move to our test dbfs folder.
dbutils.fs.cp("dbfs:/FileStore/tables/employee_test_data-f4581.csv", "dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-base-table/") 

// COMMAND ----------

// MAGIC %sql
// MAGIC --create hive table on testdata.
// MAGIC CREATE TABLE IF NOT EXISTS tbl_employee_base(
// MAGIC   first_name	string	,
// MAGIC   last_name	string	,
// MAGIC   company_name	string	,
// MAGIC   address	string	,
// MAGIC   city	string	,
// MAGIC   county	string	,
// MAGIC   state	string	,
// MAGIC   zip	string	,
// MAGIC   phone1	string	,
// MAGIC   phone2	string	,
// MAGIC   email	string	,
// MAGIC   web	string)
// MAGIC   USING com.databricks.spark.csv
// MAGIC   LOCATION "dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-base-table"

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tbl_employee_base;

// COMMAND ----------

// MAGIC %sql
// MAGIC select state , count(*) from tbl_employee_base group by state

// COMMAND ----------

// DBTITLE 1,Start concurrency control test - 1st write to delta table 
//write few data to from tbl_employee_base to delta table and partitioned by state.
val df_write1 =spark.sql("select * from tbl_employee_base where state in ('AZ','LA')")
df_write1.write.format("delta").partitionBy("state")
                .mode("append")
                .save("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table")

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

// DBTITLE 1,#let's see delta lake files.
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table

// COMMAND ----------

// DBTITLE 1,#Commit log file - On 1st Write
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/

// COMMAND ----------

// DBTITLE 1,#Data file
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/state=AZ/

// COMMAND ----------

// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/state=LA/

// COMMAND ----------

// MAGIC %fs 
// MAGIC head dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/00000000000000000000.crc

// COMMAND ----------

// DBTITLE 1,#Commit log
// MAGIC %fs 
// MAGIC head dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/00000000000000000000.json

// COMMAND ----------

// DBTITLE 1,Test 1 - concurrent write in two different partition

var state = List[String]("MN","OR")

for(a <- state.par)
{ 
  //write few data to from tbl_employee_base to delta table and partitioned by state.
  val df_write2 =spark.sql(s"select * from tbl_employee_base where state ='$a'")
  df_write2.write.format("delta").partitionBy("state")
                  .mode("append")
                  .save("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table")
}

// COMMAND ----------

// DBTITLE 1,Test 1 Result
// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

// DBTITLE 1,#Commit log file - on concurrent write in two partition
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/

// COMMAND ----------

// DBTITLE 1,Transaction 1 {"state":"OR"} - commit log 
// MAGIC %fs 
// MAGIC head dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/00000000000000000001.json

// COMMAND ----------

// DBTITLE 1,Transaction 2 {"state":"MN"} - commit log 
// MAGIC %fs 
// MAGIC head dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/00000000000000000002.json

// COMMAND ----------

// MAGIC %md
// MAGIC Test 1 Conclusion - No conflict, both transaction completed successfully 

// COMMAND ----------

// DBTITLE 1, Test 2 - concurrent write in existing and same partition 
var state = List[String]("MN","MN")

for(a <- state.par)
{ 
  //write few data to from tbl_employee_base to delta table and partitioned by state.
  val df_write2 =spark.sql(s"select * from tbl_employee_base where state ='$a'")
  df_write2.write.format("delta").partitionBy("state")
                  .mode("append")
                  .save("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table")
}

// COMMAND ----------

// DBTITLE 1,Test 2 Result
// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/

// COMMAND ----------

// DBTITLE 1,Transaction 1 {"state":"MN"} - commit log 
// MAGIC %fs 
// MAGIC head dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/00000000000000000003.json

// COMMAND ----------

// DBTITLE 1,Transaction 2 {"state":"MN"} - commit log 
// MAGIC %fs 
// MAGIC head dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/00000000000000000004.json

// COMMAND ----------

// MAGIC %md
// MAGIC Test 2 Conclusion - No conflict, both transaction completed successfully

// COMMAND ----------

// DBTITLE 1,Test 3 - concurrent read & update in same partition
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/state=OR

// COMMAND ----------

var transactions = List[String]("tran1","tran2")

for(tran <- transactions.par)
{ 
  if(tran=="tran1")
  {
    val df =spark.sql(s"""select * from delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table` where state = 'OR'""")
    //Thread.sleep(1000)
    df.write.format("delta").partitionBy("state")
                .mode("append")
                .save("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee_read_update")
  }else{
  spark.sql(s"""
    UPDATE delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`
    SET last_name = 'Ashish'
    WHERE state = 'OR'
    """)
  }
}

// COMMAND ----------

// DBTITLE 1,Test 3 Result - tran2- Update (existing table employee-delta-table)
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/state=OR

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table` WHERE state = 'OR'

// COMMAND ----------

// DBTITLE 1,Test 3 Result - tran1- Insert (New Table employee_read_update )
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee_read_update/state=OR

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee_read_update`

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee_read_update` WHERE state = 'OR'

// COMMAND ----------

// MAGIC %md
// MAGIC Test 3 Conclusion - No conflict, both transaction Read and Update completed successfully

// COMMAND ----------

// DBTITLE 1,Test 4 - concurrent Insert & update employee-delta-table in same partition
// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/state=OR

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

spark.sql("CREATE TABLE employee_delta_table USING DELTA LOCATION 'dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table'")

// COMMAND ----------

var transactions = List[String]("tran1","tran2")

for(tran <- transactions.par)
{ 
  if(tran=="tran1")
  {
    spark.sql(s"""
    insert into employee_delta_table
    select * from employee_delta_table where state = 'OR'
    """)
    //or below query - both are same
    /* val df =spark.sql(s"""select * from delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table` where state = 'OR'""")
    df.write.format("delta").partitionBy("state")
                .mode("append")
                .save("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table")*/
  }else{
  spark.sql(s"""
    UPDATE employee_delta_table
    SET first_name = 'Ashish', last_name = 'kumar'
    WHERE state = 'OR'
    """)
  }
}

// COMMAND ----------

// DBTITLE 1,Test 4 Result - tran2- Update - Success
// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

//if you see here there is two file added for our insert(tran1) & Update(tran12) statement, it has added  ****7e.c000.snappy.parquet & ***56.c000.snappy.parquet two new file.

// COMMAND ----------

// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/state=OR

// COMMAND ----------

// MAGIC %fs
// MAGIC ls dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log

// COMMAND ----------

// MAGIC %fs
// MAGIC head dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/_delta_log/00000000000000000006.json

// COMMAND ----------

// DBTITLE 1,Test 4 Result - tran1- Insert- Failed/Rollback - Error - ConcurrentAppendException
// MAGIC %sql
// MAGIC --if you see here there is no new write operation. that show tran1 didn't commit.
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

/* if you see in above command(insert and update) it created 2 new file (****7e.c000.snappy.parquet & ***56.c000.snappy.parquet) but you will find commit log entry only for (***56.c000.snappy.parquet), which is generated by update statement but as insert is failed, there is no commit log for ***7e.c000.snappy.parquet file. 
now you must be thinking if it is failed then why there is data file. well actually it write data first and then check whether there is conflict or not if there is conflict it don't generate commit log for that transaction.
I know what would be your next question, is this data will come in my select query & how long this uncommitted data will be here ??
No, this data file will not be read while select oprations and using vaccum command databricks automatically remove uncommitted files older than a retention threshold. The default threshold is 7 days.*/
display(dbutils.fs.ls("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table/state=OR"))

// COMMAND ----------

// DBTITLE 1,Test 5 - concurrent update & update employee-delta-table in same partition
var transactions = List[String]("tran1","tran2")

for(tran <- transactions.par)
{ 
  if(tran=="tran1")
  {
   spark.sql(s"""
    UPDATE employee_delta_table
    SET first_name = 'Ashish', last_name = 'kumar'
    WHERE state = 'OR' and county='Washington'
    """)
  }else{
  spark.sql(s"""
    UPDATE employee_delta_table
    SET first_name = 'Rohit', last_name = 'sharma'
    WHERE state = 'OR' and county='Multnomah' 
    """)
  }
}

// COMMAND ----------

// DBTITLE 1,Test 5 Result - tran2- Update - Failed/Rollback - Error - ConcurrentAppendException
// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta.`dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table`

// COMMAND ----------

// DBTITLE 1,Test 6 - concurrent Delete & Read employee-delta-table in same partition
var transactions = List[String]("tran1","tran2")

for(tran <- transactions.par)
{ 
  if(tran=="tran1")
  {
   val df_read = spark.sql(s"""select * from employee_delta_table WHERE state = 'OR'""")
   df_read.write.format("delta").partitionBy("state")
                .mode("append")
                .save("dbfs:/work_area/ashish@riveriqconnectgmail.onmicrosoft.com/deltalake-concurrency-control/employee-delta-table")
  }else{
  spark.sql(s"""
    delete from employee_delta_table
    WHERE state = 'OR'""")
  }
}

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY employee_delta_table LIMIT 1

// COMMAND ----------

// DBTITLE 1,Test 7 - concurrent Delete & Delete employee-delta-table in same partition
var transactions = List[String]("tran1","tran2")

for(tran <- transactions.par)
{ 
  if(tran=="tran1")
  {
   spark.sql(s"""
    delete from employee_delta_table
    WHERE state = 'OR'""")
  }else{
  spark.sql(s"""
    delete from employee_delta_table
    WHERE state = 'OR'""")
  }
}

// COMMAND ----------

// DBTITLE 1,Test 8 - concurrent Delete & Alter employee-delta-table in same partition
var transactions = List[String]("tran1","tran2")

for(tran <- transactions.par)
{ 
  if(tran=="tran1")
  {
   spark.sql(s"""ALTER TABLE employee_delta_table ADD COLUMNS (phone3 int)""")
  }else{
   spark.sql(s"""insert into employee_delta_table 
    select * from tbl_employee_base where state = 'OR'""")
  }
}

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY employee_delta_table
