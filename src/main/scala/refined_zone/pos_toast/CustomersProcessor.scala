package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import com.cxi.cdp.data_processing.raw_zone.pos_toast.model.{Checks, Customer}
import refined_zone.pos_toast.config.ProcessorConfig
import refined_zone.pos_toast.RawRefinedToastPartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object CustomersProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val customersTable = config.contract.prop[String](getSchemaRefinedPath("customer_table"))

        val customers = readCustomers(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val transformedCustomers = transformCustomers(customers, config.cxiPartnerId)

        writeCustomers(transformedCustomers, config.cxiPartnerId, s"$destDbName.$customersTable")
    }

    def readCustomers(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawChecksAsStrings = spark
            .table(table)
            .filter($"record_type" === "orders" && $"feed_date" === date)
            .select("record_value")
            .as[String]

        val df = rawChecksAsStrings
            .withColumn("checks", get_json_object($"record_value", "$.checks"))
            .withColumn(
                "checks_array",
                from_json($"checks", DataTypes.createArrayType(Encoders.product[Checks].schema))
            )
            .select(explode($"checks_array").as("check_exploded"))
            .select($"check_exploded.customer" as "customer")
            .select("customer.*")

        Seq("guid", "email", "phone", "firstName", "lastName")
            .foldLeft(df) { case (acc, col_name) =>
                if (df.columns.contains(col_name)) {
                    acc.withColumn(col_name, col(col_name))
                } else {
                    acc.withColumn(col_name, lit(null))
                }
            }
            .select(
                $"guid" as "customer_id",
                $"email" as "email_address",
                $"phone" as "phone_number",
                $"firstName" as "first_name",
                $"lastName" as "last_name"
            )
    }

    def transformCustomers(customers: DataFrame, cxiPartnerId: String): DataFrame = {
        val transformedCustomers = customers
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("created_at", lit(null))
            .withColumn("version", lit(null))
            .dropDuplicates("cxi_partner_id", "customer_id")
        transformedCustomers
    }

    def writeCustomers(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newCustomers"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.customer_id = $srcTable.customer_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
