package com.cxi.cdp.data_processing
package support.packages.utils

import com.databricks.service.DBUtils
import org.apache.spark.sql.SparkSession

object PrivacyFunctions {
    // TODO: Should this go into a different folder structure?
    def authorize(spark: SparkSession) {
        val dbutils = DBUtils
        import com.fasterxml.jackson.databind.ObjectMapper;

        val globalConfigDetails=spark.sparkContext.wholeTextFiles("dbfs:/databricks/config/workspace_details.json").collect.take(1)(0)._2
        val mapper = new ObjectMapper();
        val glConfig :java.util.Map[String, String] = mapper.readValue(globalConfigDetails, classOf[java.util.Map[String, String]]);

        val envType=glConfig.get("envType")
        val region=glConfig.get("region")
        val productName = glConfig.get("productName")
        val scope=envType + "-"+region+"-keyvault-scope"
        val storageAccount="dls2"+envType+region+productName
        val location = "data-privacy@"+storageAccount
        val url= "https://dls2"+envType+region+productName+".blob.core.windows.net/data-privacy?"
        val fileLocation = "wasbs://"+location+".blob.core.windows.net/privacy_zone/cxi/crypto/pii/"
        val secretToken: String = dbutils.secrets.get(scope, "sp-crypto-rw-secret")

        spark.conf.set(
            "fs.azure.account.key." + storageAccount + ".blob.core.windows.net",
            secretToken)
    }

    // COMMAND ----------

    //"fs.azure.account.key.dls2deveastus2cxi.blob.core.windows.net"
    def unauthorize(spark: SparkSession, key: String): Boolean = {
        try {
            spark.conf.unset(key)
            true
        } catch   {
            case e: Throwable =>
                throw new RuntimeException(e)
        }
    }

    // COMMAND ----------

    //TODO: Should this go in global config?
    def createObjects (spark: SparkSession) {
        try
        {
            spark.sql(s"CREATE DATABASE privacy")
            println(s"Created Database privacy")
        }
        catch
        {
            case e: org.apache.spark.sql.AnalysisException =>
                println(s"Database privacy Already Exists")
        }

        try
        {
            spark.sql(s"""
    CREATE TABLE privacy.lookup_table (
     process_name string ,
     country string ,
     cxi_partner_id string ,
     cxi_customer_id string ,
     hashof_cxi_customer_id string,
     feed_date timestamp,
     id string
     )
     USING DELTA
     PARTITIONED BY (process_name)
     LOCATION "wasbs://data-privacy@dls2deveastus2cxi.blob.core.windows.net/privacy_zone/cxi/crypto/pii/lookup_table"
     """)
        }
        catch
        {
            case e: org.apache.spark.sql.AnalysisException =>
                println("Table privacy.lookup_table Already Exists")
        }
    }

}
