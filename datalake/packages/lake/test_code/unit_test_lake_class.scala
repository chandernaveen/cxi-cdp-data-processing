// Databricks notebook source
// MAGIC %run "/datalake/packages/regulation/hashing_properties"

// COMMAND ----------

// MAGIC %run "../lookup_table_lake_class"

// COMMAND ----------

// MAGIC %run "/datalake/packages/utils/privacy_functions"

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import com.cxi.lake._

def writeLookup(lookupDf: DataFrame): Unit = {
  authorize()
  LookupTableLake.upsert(lookupDf)
  unauthorize("fs.azure.account.key.dls2deveastus2cxi.blob.core.windows.net")
}

// COMMAND ----------

import scala.util.Random
import com.cxi.regulation.Hash

def randomInt(x: Int): Int = scala.util.Random.nextInt(x)

val newDF = sc.parallelize(
  Seq.fill(5){("test", "USA", "123", "USA-123-" + randomInt(5), Hash.sha256Hash("USA-123-" + randomInt(5)))}
).toDF("process_name", "country", "cxi_partner_id", "cxi_customer_id", "hashof_cxi_customer_id")

// COMMAND ----------

display(newDF)

// COMMAND ----------

writeLookup(newDF)

// COMMAND ----------

//The following is to confirm, first I authorize myself manually then I check the table 

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select * from privacy.lookup_table --This should fail (not authorize)

// COMMAND ----------

authorize()

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from privacy.lookup_table --This should work now, plus should have randomly generated data

// COMMAND ----------

unauthorize("fs.azure.account.key.dls2deveastus2cxi.blob.core.windows.net")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from privacy.lookup_table --Back to failing