package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, Tender}
import refined_zone.pos_square.RawRefinedSquarePartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}
import support.crypto_shredding.CryptoShredding
import support.packages.utils.ContractUtils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}

object CxiCustomersProcessor {
    def process(spark: SparkSession,
                contract: ContractUtils,
                date: String,
                cxiPartnerId: String,
                srcDbName: String,
                srcTable: String,
                destDbName: String,
                payments: DataFrame): DataFrame = {

        val refinedDbTable = contract.prop[String](getSchemaRefinedPath("db_name"))
        val customersDimTable = contract.prop[String](getSchemaRefinedPath("customer_table"))
        val customersTable = contract.prop[String](getSchemaRefinedHubPath("customer_table"))

        val customers = broadcast(readAllCustomersDim(spark, refinedDbTable, customersDimTable))

        val orderCustomersData = readOrderCustomersData(spark, date, srcDbName, srcTable)
        val transformedOrderCustomersData = transformOrderCustomersData(orderCustomersData, cxiPartnerId)

        val orderCustomersPickupData = readOrderCustomersPickupData(spark, date, srcDbName, srcTable)
        val transformedOrderCustomersPickupData = transformOrderCustomersPickupData(orderCustomersPickupData, cxiPartnerId)
            .cache()

        val fullCustomerData = transformCustomers(transformedOrderCustomersData, customers, payments)
            .cache()

        val strongIds = computeWeight3CxiCustomerId(fullCustomerData, transformedOrderCustomersPickupData)
        val hashedCombinations = computeWeight2CxiCustomerId(spark,
            cxiPartnerId,
            contract.prop[String]("partner.country"),
            contract.prop[String]("schema.crypto.db_name"),
            contract.prop[String]("schema.crypto.lookup_table"),
            contract.prop[String]("databricks_workspace_config"),
            fullCustomerData)

        val allCustomerIds = strongIds.unionAll(hashedCombinations)
            .cache()

        val cxiCustomers = allCustomerIds
            .select("cxi_customer_id", "customer_type", "customer_weight")
            .withColumn("customer_metadata", lit(null))
            .dropDuplicates("cxi_customer_id")

        writeCxiCustomers(cxiCustomers, s"$destDbName.$customersTable")

        val cxiCustomerIdsByOrder = allCustomerIds.select("ord_id", "cxi_customer_id")
            .groupBy("ord_id")
            .agg(collect_list("cxi_customer_id") as "cxi_customer_id_array")
        cxiCustomerIdsByOrder
    }

    def readOrderCustomersData(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as ord_id,
               |get_json_object(record_value, "$$.closed_at") as ord_timestamp,
               |get_json_object(record_value, "$$.location_id") as ord_location_id,
               |get_json_object(record_value, "$$.state") as ord_state,
               |get_json_object(record_value, "$$.customer_id") as ord_customer_id_1,
               |get_json_object(record_value, "$$.tenders") as tender_array
               |FROM $dbName.$table
               |WHERE record_type = "orders" AND get_json_object(record_value, "$$.state") = "COMPLETED" AND feed_date="$date"
               |""".stripMargin)
    }

    def transformOrderCustomersData(orderCustomersData: DataFrame, cxiPartnerId: String): DataFrame = {
        orderCustomersData
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("tender_array", from_json(col("tender_array"), DataTypes.createArrayType(Encoders.product[Tender].schema)))
            .withColumn("tender", explode(col("tender_array")))
            .withColumn("tender_type", col("tender.type"))
            .withColumn("ord_payment_id", col("tender.id"))
            .withColumn("ord_customer_id_2", col("tender.customer_id"))
            .withColumn("ord_customer_id", when(col("ord_customer_id_1").isNull or col("ord_customer_id_1") === "",
                col("ord_customer_id_2")).otherwise(col("ord_customer_id_1")))
            .drop("tender_array", "tender", "cxi_customer_id_array", "cxi_customer_id_1", "cxi_customer_id_2")
    }

    def readOrderCustomersPickupData(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as ord_id,
               |get_json_object(record_value, "$$.closed_at") as ord_timestamp,
               |get_json_object(record_value, "$$.location_id") as ord_location_id,
               |get_json_object(record_value, "$$.state") as ord_state,
               |get_json_object(record_value, "$$.fulfillments") as fulfillments
               |FROM $dbName.$table
               |WHERE record_type = "orders" AND get_json_object(record_value, "$$.state") = "COMPLETED" AND feed_date="$date"
               |  AND get_json_object(record_value, "$$.fulfillments") IS NOT NULL
               |""".stripMargin)
    }

    def transformOrderCustomersPickupData(orderCustomersPickupData: DataFrame, cxiPartnerId: String): DataFrame = {
        orderCustomersPickupData
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("fulfillments", from_json(col("fulfillments"), DataTypes.createArrayType(Encoders.product[Fulfillment].schema)))
            .withColumn("fulfillment", explode(col("fulfillments")))
            .withColumn("email_address", col("fulfillment.pickup_details.recipient.email_address"))
            .withColumn("phone_number", col("fulfillment.pickup_details.recipient.phone_number"))
            .drop("fulfillment")
    }

    def readAllCustomersDim(spark: SparkSession, dbName: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |customer_id,
               |email_address,
               |phone_number,
               |first_name,
               |last_name,
               |created_at,
               |version
               |FROM $dbName.$table
               |""".stripMargin)
    }

    def transformCustomers(transformedOrderCustomersData: DataFrame,
                           customers: DataFrame,
                           transformedPayments: DataFrame): DataFrame = {

        val transformedCustomers = customers
            .dropDuplicates("customer_id")

        val fullCustomerData = transformedOrderCustomersData
            .join(transformedCustomers, transformedOrderCustomersData("ord_customer_id") === transformedCustomers("customer_id"), "left")
            .join(transformedPayments, transformedOrderCustomersData("ord_payment_id") === transformedPayments("payment_id"), "left")
        fullCustomerData
    }

    def computeWeight3CxiCustomerId(fullCustomerData: DataFrame, transformedOrderCustomersPickupData: DataFrame): DataFrame = {
        val emailsSource = fullCustomerData
            .filter(col("email_address").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("email_address").as("source_id"),
                lit("email").as("source_type"),
                lit(3).as("source_weight"))

        val emailsPickup = transformedOrderCustomersPickupData
            .filter(col("email_address").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("email_address").as("source_id"),
                lit("email").as("source_type"),
                lit(3).as("source_weight"))

        val allEmails = emailsSource.unionAll(emailsPickup)

        val phoneSource = fullCustomerData
            .filter(col("phone_number").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("phone_number").as("source_id"),
                lit("phone").as("source_type"),
                lit(3).as("source_weight"))

        val phonesPickup = transformedOrderCustomersPickupData
            .filter(col("phone_number").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("phone_number").as("source_id"),
                lit("phone").as("source_type"),
                lit(3).as("source_weight"))


        val allPhones = phoneSource.unionAll(phonesPickup)

        allEmails.unionAll(allPhones)
            .withColumnRenamed("source_id", "cxi_customer_id") // Not hashing cause it is supposed to be hash already (weight 3 was in crypto for Landing/Raw)
            .withColumnRenamed("source_type", "customer_type")
            .withColumnRenamed("source_weight", "customer_weight")
    }

    def computeWeight2CxiCustomerId(spark: SparkSession,
                                    cxiPartnerId: String,
                                    country: String,
                                    lookupDestDbName: String,
                                    lookupDestTableName: String,
                                    workspaceConfigPath: String,
                                    fullCustomerData: DataFrame): DataFrame = {
        val commonCombinationFilters: Column =
                col("exp_month").isNotNull and
                col("exp_year").isNotNull and
                col("card_brand").isNotNull and
                col("pan").isNotNull

        val nameExpTypePanCombination = fullCustomerData
            .filter(commonCombinationFilters)
            .withColumn("name_transformed",
                when(col("name").isNotNull and (col("name") notEqual "") and !(col("name") contains "CARDHOLDER"),
                    trim(lower(col("name")))).otherwise(lit(null)))
            .withColumn("first_and_last_name",
                when(col("name_transformed").isNull and
                    col("first_name").isNotNull and (col("first_name") notEqual "") and
                    col("last_name").isNotNull and (col("last_name") notEqual ""),
                    concat(trim(lower(col("first_name"))), lit("/"), trim(lower(col("last_name"))))).otherwise(lit(null)))
            .withColumn("final_name", when(col("name_transformed").isNotNull, col("name_transformed")).otherwise(col("first_and_last_name")))
            .drop("name_transformed", "first_and_last_name")
            .filter(col("final_name").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                concat(
                    col("final_name"), lit("-"),
                    lower(col("exp_month")), lit("-"),
                    lower(col("exp_year")), lit("-"),
                    lower(col("card_brand")), lit("-"),
                    lower(col("pan"))
                ).as("source_id"))
            .withColumn("source_type", lit("combination-card"))
            .withColumn("source_weight", lit(2))


        val binExpTypePanCombination = fullCustomerData
            .filter(col("bin").isNotNull and commonCombinationFilters)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                concat(
                    lower(col("bin")), lit("-"),
                    lower(col("exp_month")), lit("-"),
                    lower(col("exp_year")), lit("-"),
                    lower(col("card_brand")), lit("-"),
                    lower(col("pan"))).as("source_id"),
                lit("combination-bin").as("source_type"),
                lit(2).as("source_weight"))

        // for weight 2 source has already created combination, simply hash it and treat it as weight 3
        val combinationsIds = nameExpTypePanCombination.unionAll(binExpTypePanCombination)
            .withColumnRenamed("source_id", "cxi_customer_id")
            .withColumnRenamed("source_type", "customer_type")
            .withColumnRenamed("source_weight", "customer_weight")

        // consider combinations ids as PII
        val hashedCombinations = new CryptoShredding(spark, country, cxiPartnerId, lookupDestDbName, lookupDestTableName, workspaceConfigPath)
            .applyHashCryptoShredding("common", Map("dataColName" -> "cxi_customer_id"), combinationsIds)
        hashedCombinations
    }

    def writeCxiCustomers(df: DataFrame, destTable: String): Unit = {
        val srcTable = "newCustomers"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_customer_id = $srcTable.cxi_customer_id
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
