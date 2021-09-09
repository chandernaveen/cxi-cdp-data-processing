//TODO: Private Functions needs a home, still debating where this goes.

package com.cxi.cdp.data_processing
package support.packages.lake

class LookupTableLake() extends TableLake("privacy", "lookup_table") with Serializable {

  import org.apache.spark.sql.{DataFrame, SaveMode}
  import org.apache.spark.sql.functions._

  override val requiredFields = Seq("process_name", "country", "cxi_partner_id", "cxi_customer_id", "hashof_cxi_customer_id")

  //TODO: Fix manually entering saveAsTable
  override protected def merge(df: DataFrame): Unit = {
    transformDf(df)
      .write
      .mode(SaveMode.Append)
      .format("delta")
      .partitionBy("process_name")
      .saveAsTable("privacy.lookup_table")
  }

  def transformDf(df: DataFrame): DataFrame = {
    df.select(col("*")
      , current_timestamp().as("feed_date"))
      .withColumn("id", generateUUID())
  }
}

object LookupTableLake {
  import org.apache.spark.sql.DataFrame
  def upsert(df: DataFrame): Unit = {
    new LookupTableLake().upsert(df)
  }
}
