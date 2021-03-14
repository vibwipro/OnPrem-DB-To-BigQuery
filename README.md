# Loading data from OnPrem-DB to a BigQuery table
![Description](https://miro.medium.com/max/1244/1*kYGn3nWMfIjK2hPe7VBqHg.png)

---
> This repository represent a solution how to load on-prem oracle data to GCP-BigQuery table. Learn how to ingest on-premises RDBMS (Oracle, SqlServer, Sybase etc. ) data with SQLcl, gsutil & BQ-Script 

A common challenge while designing the Analytics warehouse on BigQuery is to transfer all the historical data stored in traditional databases, and this data size can vary from few Gb’s to many Tb’s. Google Cloud recommends two approaches to solve this problem.

1. Export the tables into .csv file, and then use BigQuery Jobs or Dataflow Pipeline to load data into Bigquery.
2. Use a Dataflow Pipeline (Only Java SDK , Apache Beam doesn’t support native JDBC support for Python as of now) to connect directly to on-prem database and load data in Google BigQuery.

Now, the problem with the **first approach** is that the solution can be designed only for the incremental batch process.

And the limitation with **second approach** is that built-in I/O connectors for RDBMS are only available in Java, with the growing popularity and adaption of python for Data Processing and Data science that seems a little unfair but we hope beam provides this support soon !

**Components Overview **
1. OnPrem Database (Source)
2. SQLcl (data exporting tool)
3. BQ Load script (data pipeline)
4. BigQuery table (target)


---

  ## 1. Master Script 

> This is master script which is responsible for Pulling data from On-Prem DB and load it into an intermediate file. Then .CSV file is considred for loading through a BQ-Script. Post data loading through gsutil using BQ-script archival of intermediate .CSV files is done. A referal table schema is used to pull and load data from different or-prem tables!

We can find Master shell script Code [csv_upload.sh](https://github.com/vibwipro/OnPrem-DB-To-BigQuery/blob/main/csv_upload.sh) and Table schema [Schema](https://github.com/vibwipro/OnPrem-DB-To-BigQuery/blob/main/schemas/T1.json)

---

Thanks!!
