package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, Tender}
import refined_zone.hub.model.CxiIdentity._
import refined_zone.pos_square.RawRefinedSquarePartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}
import refined_zone.pos_square.config.ProcessorConfig
import refined_zone.service.MetadataService.extractMetadata
import support.WorkspaceConfigReader
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.{CryptoShredding, PrivacyFunctions}
import support.utils.ContractUtils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}

object CxiIdentityProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String, payments: DataFrame): DataFrame = {

        val refinedDbTable = config.contract.prop[String](getSchemaRefinedPath("db_name"))
        val customersDimTable = config.contract.prop[String](getSchemaRefinedPath("customer_table"))
        val identityTable = config.contract.prop[String](getSchemaRefinedHubPath("identity_table"))

        val customers = broadcast(readAllCustomersDim(spark, refinedDbTable, customersDimTable))

        val orderCustomersData = readOrderCustomersData(spark, config.date, config.srcDbName, config.srcTable)
        val transformedOrderCustomersData = transformOrderCustomersData(orderCustomersData, config.cxiPartnerId)

        val orderCustomersPickupData = readOrderCustomersPickupData(spark, config.date, config.srcDbName, config.srcTable)
        val transformedOrderCustomersPickupData = transformOrderCustomersPickupData(orderCustomersPickupData, config.cxiPartnerId)
            .cache()

        val fullCustomerData = transformCustomers(transformedOrderCustomersData, customers, payments)
            .cache()

        val strongIds = computeWeight3CxiCustomerId(fullCustomerData, transformedOrderCustomersPickupData)
        val hashedCombinations = computeWeight2CxiCustomerId(spark,
            config.cxiPartnerId,
            config.contract.prop[String]("partner.country"),
            config.contract.prop[String]("schema.crypto.db_name"),
            config.contract.prop[String]("schema.crypto.lookup_table"),
            config.contract.prop[String]("databricks_workspace_config"),
            fullCustomerData)

        val allIdentitiesIds = strongIds.unionAll(hashedCombinations)
            .cache()

        writeCxiIdentities(spark, allIdentitiesIds, s"$destDbName.$identityTable", config.contract)

        val cxiIdentitiesIdsByOrder = allIdentitiesIds
            .groupBy("ord_id")
            .agg(collect_list(struct(col(Type) as "identity_type", col(CxiIdentityId))) as CxiIdentityIds)
        cxiIdentitiesIdsByOrder
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
            .drop("tender_array", "tender", "ord_customer_id_1", "ord_customer_id_2")
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
            .withColumnRenamed("source_id", CxiIdentityId) // Not hashing cause it is supposed to be hash already (weight 3 was in crypto for Landing/Raw)
            .withColumnRenamed("source_type", Type)
            .withColumnRenamed("source_weight", Weight)
    }

    def computeWeight2CxiCustomerId(spark: SparkSession,
                                    cxiPartnerId: String,
                                    country: String,
                                    lookupDestDbName: String,
                                    lookupDestTableName: String,
                                    workspaceConfigPath: String,
                                    fullCustomerData: DataFrame): DataFrame = {

        val nameExpTypePanCombination = getNameExpTypePanCombination(fullCustomerData)

        val binExpTypePanCombination = getBinExpTypePanCombination(fullCustomerData)

        // for weight 2 source has already created combination, simply hash it and treat it as weight 3
        val combinationsIds = nameExpTypePanCombination.unionAll(binExpTypePanCombination)
            .withColumnRenamed("source_id", CxiIdentityId)
            .withColumnRenamed("source_type", Type)
            .withColumnRenamed("source_weight", Weight)

        // consider combinations ids as PII
        val cryptoShreddingConfig = CryptoShreddingConfig(
            country = country,
            cxiPartnerId = cxiPartnerId,
            lookupDestDbName = lookupDestDbName,
            lookupDestTableName = lookupDestTableName,
            workspaceConfigPath = workspaceConfigPath)
        val hashedCombinations = new CryptoShredding(spark, cryptoShreddingConfig)
            .applyHashCryptoShredding("common", Map("dataColName" -> CxiIdentityId), combinationsIds)
        hashedCombinations
    }

    private def getBinExpTypePanCombination(fullCustomerData: DataFrame) = {
        fullCustomerData
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
    }

    private def getNameExpTypePanCombination(fullCustomerData: DataFrame) = {
        fullCustomerData
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
    }

    private def commonCombinationFilters: Column =
        col("exp_month").isNotNull and
            col("exp_year").isNotNull and
            col("card_brand").isNotNull and
            col("pan").isNotNull

    def writeCxiIdentities(spark: SparkSession, allCustomerIds: DataFrame, destTable: String, contract: ContractUtils): Unit = {

        val workspaceConfigPath: String = contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)
        val privacyFunctions = new PrivacyFunctions(spark, workspaceConfig)

        try {
            privacyFunctions.authorize()
            val privacyTable = readPrivacyLookupTable(spark, contract)
            val cxiIdentities = allCustomerIds
                .select(CxiIdentityId, Type, Weight)
                .dropDuplicates(CxiIdentityId)
                .join(privacyTable, allCustomerIds(CxiIdentityId) === privacyTable("hashed_value"), "left")

            val metadataUdf = udf(extractMetadata _)
            val cxiIdentitiesWithMetadata = cxiIdentities
                .withColumn(Metadata, metadataUdf(col(Type), col("original_value")))
                .drop("original_value", "hashed_value")

            val srcTable = "newIdentities"

            cxiIdentitiesWithMetadata.createOrReplaceTempView(srcTable)
            cxiIdentitiesWithMetadata.sqlContext.sql(
                s"""
                   |MERGE INTO $destTable
                   |USING $srcTable
                   |ON $destTable.$CxiIdentityId = $srcTable.$CxiIdentityId
                   |WHEN NOT MATCHED
                   |  THEN INSERT *
                   |""".stripMargin)
        } finally {
            privacyFunctions.unauthorize()
        }
    }

    def readPrivacyLookupTable(spark: SparkSession, contract: ContractUtils): DataFrame = {
        val lookupDbName = contract.prop[String]("schema.crypto.db_name")
        val lookupTableName = contract.prop[String]("schema.crypto.lookup_table")
        spark.sql(
            s"""
               |SELECT original_value, hashed_value
               |FROM $lookupDbName.$lookupTableName
               |""".stripMargin
        )
    }

}
