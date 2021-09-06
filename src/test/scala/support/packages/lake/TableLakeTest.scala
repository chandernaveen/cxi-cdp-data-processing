package com.cxi.cdp.data_processing
package support.packages.lake

import support.packages.regulation.Hash
import support.packages.utils.PrivacyFunctions.{authorize, unauthorize}

import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: change to regular test
object TableLakeTest {

    def run(): Unit = {
        // Databricks notebook source
        // MAGIC %run "/datalake/packages/regulation/hashing_properties"

        // COMMAND ----------

        // MAGIC %run "../lookup_table_lake_class"

        // COMMAND ----------

        // MAGIC %run "/datalake/packages/utils/privacy_functions"

        // COMMAND ----------

        val spark = SparkSession.builder().getOrCreate()


        def writeLookup(lookupDf: DataFrame): Unit = {
            authorize(spark)
            LookupTableLake.upsert(lookupDf)
            unauthorize(spark, "fs.azure.account.key.dls2deveastus2cxi.blob.core.windows.net")
        }

        // COMMAND ----------

        def randomInt(x: Int): Int = scala.util.Random.nextInt(x)
        import spark.implicits._

        val newDF = spark.sparkContext.parallelize(
            Seq.fill(5){("test", "USA", "123", "USA-123-" + randomInt(5), Hash.sha256Hash("USA-123-" + randomInt(5)))}
        ).toDF("process_name", "country", "cxi_partner_id", "cxi_customer_id", "hashof_cxi_customer_id")

        // COMMAND ----------

        newDF.show(false)

        // COMMAND ----------

        writeLookup(newDF)

        // COMMAND ----------

        //The following is to confirm, first I authorize myself manually then I check the table

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC
        // MAGIC select * from privacy.lookup_table --This should fail (not authorize)

        // COMMAND ----------

        authorize(spark)

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC
        // MAGIC select * from privacy.lookup_table --This should work now, plus should have randomly generated data

        // COMMAND ----------

        unauthorize(spark, "fs.azure.account.key.dls2deveastus2cxi.blob.core.windows.net")

        // COMMAND ----------

        // MAGIC %sql
        // MAGIC
        // MAGIC select * from privacy.lookup_table --Back to failing
    }
}
