package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, Tender}
import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity._
import refined_zone.pos_square.config.ProcessorConfig
import refined_zone.pos_square.RawRefinedSquarePartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}
import refined_zone.service.MetadataService.extractMetadata
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.CryptoShredding
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.utils.ContractUtils
import support.WorkspaceConfigReader

import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object CxiIdentityProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, payments: DataFrame): DataFrame = {

        val refinedHubDbName = config.contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val identityIntermediateTable =
            config.contract.prop[String](getSchemaRefinedHubPath("identity_intermediate_table"))

        val refinedDbTable = config.contract.prop[String](getSchemaRefinedPath("db_name"))
        val customersDimTable = config.contract.prop[String](getSchemaRefinedPath("customer_table"))

        val workspaceConfigPath: String = config.contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)

        val customers = broadcast(readAllCustomersDim(spark, refinedDbTable, customersDimTable))
        val orderCustomersPickupData =
            readOrderCustomersPickupData(spark, config.dateRaw, config.srcDbName, config.srcTable)
        val orderCustomersData = readOrderCustomersData(spark, config.dateRaw, config.srcDbName, config.srcTable)

        val (transformedOrderCustomersPickupData, fullCustomerData) =
            transform(config, orderCustomersData, orderCustomersPickupData, customers, payments)

        val strongIds = computeWeight3CxiCustomerId(fullCustomerData, transformedOrderCustomersPickupData)
        val cryptoShreddingConfig: CryptoShreddingConfig = getCryptoShreddingConfig(config, workspaceConfigPath)
        val hashedCombinations = computeWeight2CxiCustomerId(cryptoShreddingConfig, fullCustomerData)(spark)

        val allIdentitiesIds = strongIds
            .unionAll(hashedCombinations)
            .cache()

        inAuthorizedContext(spark, workspaceConfig) {
            val privacyTable = readPrivacyLookupTable(spark, config.contract, cryptoShreddingConfig)
            val cxiIdentitiesWithMetadata = addCxiIdentitiesMetadata(privacyTable, allIdentitiesIds)

            writeCxiIdentities(
                cxiIdentitiesWithMetadata,
                s"$refinedHubDbName.$identityIntermediateTable",
                cryptoShreddingConfig.dateRaw,
                cryptoShreddingConfig.runId
            )
        }

        val cxiIdentitiesIdsByOrder = allIdentitiesIds
            .groupBy("ord_id")
            .agg(collect_list(struct(col(Type) as "identity_type", col(CxiIdentityId))) as CxiIdentityIds)
        cxiIdentitiesIdsByOrder
    }

    def transform(
        config: ProcessorConfig,
        orderCustomersData: DataFrame,
        orderCustomersPickupData: DataFrame,
        customers: DataFrame,
        payments: DataFrame
    ): (DataFrame, DataFrame) = {

        val transformedOrderCustomersData = transformOrderCustomersData(orderCustomersData, config.cxiPartnerId)
        val transformedOrderCustomersPickupData =
            transformOrderCustomersPickupData(orderCustomersPickupData, config.cxiPartnerId)
                .cache()
        val fullCustomerData = transformCustomers(transformedOrderCustomersData, customers, payments)
            .cache()

        (transformedOrderCustomersPickupData, fullCustomerData)
    }

    def readOrderCustomersData(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
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
            .withColumn(
                "tender_array",
                from_json(col("tender_array"), DataTypes.createArrayType(Encoders.product[Tender].schema))
            )
            .withColumn("tender", explode(col("tender_array")))
            .withColumn("tender_type", col("tender.type"))
            .withColumn("ord_payment_id", col("tender.id"))
            .withColumn("ord_customer_id_2", col("tender.customer_id"))
            .withColumn(
                "ord_customer_id",
                when(col("ord_customer_id_1").isNull or col("ord_customer_id_1") === "", col("ord_customer_id_2"))
                    .otherwise(col("ord_customer_id_1"))
            )
            .drop("tender_array", "tender", "ord_customer_id_1", "ord_customer_id_2")
    }

    def readOrderCustomersPickupData(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
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
            .withColumn(
                "fulfillments",
                from_json(col("fulfillments"), DataTypes.createArrayType(Encoders.product[Fulfillment].schema))
            )
            .withColumn("fulfillment", explode(col("fulfillments")))
            .withColumn("email_address", col("fulfillment.pickup_details.recipient.email_address"))
            .withColumn("phone_number", col("fulfillment.pickup_details.recipient.phone_number"))
            .drop("fulfillment")
    }

    def readAllCustomersDim(spark: SparkSession, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
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

    def transformCustomers(
        transformedOrderCustomersData: DataFrame,
        customers: DataFrame,
        transformedPayments: DataFrame
    ): DataFrame = {

        val transformedCustomers = customers
            .dropDuplicates("customer_id")

        val fullCustomerData = transformedOrderCustomersData
            .join(
                transformedCustomers,
                transformedOrderCustomersData("ord_customer_id") === transformedCustomers("customer_id"),
                "left"
            )
            .join(
                transformedPayments,
                transformedOrderCustomersData("ord_payment_id") === transformedPayments("payment_id"),
                "left"
            )
        fullCustomerData
    }

    def computeWeight3CxiCustomerId(
        fullCustomerData: DataFrame,
        transformedOrderCustomersPickupData: DataFrame
    ): DataFrame = {
        val allEmails = computeWeight3EmailAddressCxiCustomerId(fullCustomerData, transformedOrderCustomersPickupData)
        val allPhones = computeWeight3PhoneNumberCxiCustomerId(fullCustomerData, transformedOrderCustomersPickupData)

        // Not hashing cause it is supposed to be hash already (weight 3 was in crypto for Landing/Raw)
        allEmails.unionByName(allPhones)
    }

    private def computeWeight3EmailAddressCxiCustomerId(
        fullCustomerData: DataFrame,
        transformedOrderCustomersPickupData: DataFrame
    ): DataFrame = {
        val emailsSource = fullCustomerData
            .filter(col("email_address").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("email_address").as(CxiIdentityId),
                lit(IdentityType.Email.code).as(Type),
                lit(3).as(Weight)
            )

        val emailsPickup = transformedOrderCustomersPickupData
            .filter(col("email_address").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("email_address").as(CxiIdentityId),
                lit(IdentityType.Email.code).as(Type),
                lit(3).as(Weight)
            )

        emailsSource.unionByName(emailsPickup)
    }

    private def computeWeight3PhoneNumberCxiCustomerId(
        fullCustomerData: DataFrame,
        transformedOrderCustomersPickupData: DataFrame
    ): DataFrame = {
        val phonesSource = fullCustomerData
            .filter(col("phone_number").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("phone_number").as(CxiIdentityId),
                lit(IdentityType.Phone.code).as(Type),
                lit(3).as(Weight)
            )

        val phonesPickup = transformedOrderCustomersPickupData
            .filter(col("phone_number").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                col("phone_number").as(CxiIdentityId),
                lit(IdentityType.Phone.code).as(Type),
                lit(3).as(Weight)
            )

        phonesSource.unionByName(phonesPickup)
    }

    def computeWeight2CxiCustomerId(cryptoShreddingConfig: CryptoShreddingConfig, fullCustomerData: DataFrame)(implicit
        spark: SparkSession
    ): DataFrame = {

        val nameExpTypePanCombination = getNameExpTypePanCombination(fullCustomerData)

        val binExpTypePanCombination = getBinExpTypePanCombination(fullCustomerData)

        // for weight 2 source has already created combination, simply hash it and treat it as weight 3
        // consider combinations ids as PII
        val cryptoShredding = new CryptoShredding(spark, cryptoShreddingConfig)

        val hashedNameExpTypePanCombination = cryptoShredding
            .applyHashCryptoShredding(
                "common",
                Map(
                    "pii_columns" -> Seq(
                        Map("column" -> CxiIdentityId, "identity_type" -> IdentityType.CombinationCard.code)
                    )
                ),
                nameExpTypePanCombination
            )
        val hashedBinExpTypePanCombination = cryptoShredding
            .applyHashCryptoShredding(
                "common",
                Map(
                    "pii_columns" -> Seq(
                        Map("column" -> CxiIdentityId, "identity_type" -> IdentityType.CombinationBin.code)
                    )
                ),
                binExpTypePanCombination
            )

        hashedNameExpTypePanCombination.unionByName(hashedBinExpTypePanCombination)
    }

    private def getBinExpTypePanCombination(fullCustomerData: DataFrame) = {
        fullCustomerData
            .filter(col("bin").isNotNull and commonCombinationFilters)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                concat(
                    lower(col("bin")),
                    lit("-"),
                    lower(col("exp_month")),
                    lit("-"),
                    lower(col("exp_year")),
                    lit("-"),
                    lower(col("card_brand")),
                    lit("-"),
                    lower(col("pan"))
                ).as(CxiIdentityId),
                lit(IdentityType.CombinationBin.code).as(Type),
                lit(2).as(Weight)
            )
    }

    private def getNameExpTypePanCombination(fullCustomerData: DataFrame) = {
        fullCustomerData
            .filter(commonCombinationFilters)
            .withColumn(
                "name_transformed",
                when(
                    col("name").isNotNull and (col("name") notEqual "") and !(col("name") contains "CARDHOLDER"),
                    trim(lower(col("name")))
                ).otherwise(lit(null))
            )
            .withColumn(
                "first_and_last_name",
                when(
                    col("name_transformed").isNull and
                        col("first_name").isNotNull and (col("first_name") notEqual "") and
                        col("last_name").isNotNull and (col("last_name") notEqual ""),
                    concat(trim(lower(col("first_name"))), lit("/"), trim(lower(col("last_name"))))
                ).otherwise(lit(null))
            )
            .withColumn(
                "final_name",
                when(col("name_transformed").isNotNull, col("name_transformed")).otherwise(col("first_and_last_name"))
            )
            .drop("name_transformed", "first_and_last_name")
            .filter(col("final_name").isNotNull)
            .select(
                col("ord_id"),
                col("ord_timestamp"),
                col("ord_location_id"),
                concat(
                    col("final_name"),
                    lit("-"),
                    lower(col("exp_month")),
                    lit("-"),
                    lower(col("exp_year")),
                    lit("-"),
                    lower(col("card_brand")),
                    lit("-"),
                    lower(col("pan"))
                ).as(CxiIdentityId)
            )
            .withColumn(Type, lit(IdentityType.CombinationCard.code))
            .withColumn(Weight, lit(2))
    }

    private def commonCombinationFilters: Column =
        col("exp_month").isNotNull and
            col("exp_year").isNotNull and
            col("card_brand").isNotNull and
            col("pan").isNotNull

    def addCxiIdentitiesMetadata(privacyTable: DataFrame, allCustomerIds: DataFrame): DataFrame = {

        val cxiIdentities = allCustomerIds
            .select(CxiIdentityId, Type, Weight)
            .dropDuplicates(CxiIdentityId, Type)
            .join(
                privacyTable,
                allCustomerIds(CxiIdentityId) === privacyTable("hashed_value") &&
                    allCustomerIds(Type) === privacyTable("identity_type"),
                "left"
            )

        val metadataUdf = udf(extractMetadata _)
        cxiIdentities
            .withColumn(Metadata, metadataUdf(col(Type), col("original_value")))
            .drop("original_value", "hashed_value", "identity_type")
    }

    def writeCxiIdentities(
        cxiIdentitiesWithMetadata: DataFrame,
        destTable: String,
        feedDate: String,
        runId: String
    ): Unit = {

        val srcTable = "newIdentities"
        cxiIdentitiesWithMetadata.createOrReplaceTempView(srcTable)
        cxiIdentitiesWithMetadata.sqlContext.sql(s"""
               |INSERT OVERWRITE TABLE $destTable
               |PARTITION(feed_date = '$feedDate', run_id = '$runId')
               |SELECT * FROM $srcTable
               |""".stripMargin)
    }

    def readPrivacyLookupTable(
        spark: SparkSession,
        contract: ContractUtils,
        cryptoShreddingConfig: CryptoShreddingConfig
    ): DataFrame = {
        val lookupDbName = contract.prop[String]("schema.crypto.db_name")
        val lookupTableName = contract.prop[String]("schema.crypto.lookup_table")
        spark.sql(
            s"""
               |SELECT original_value, hashed_value, identity_type
               |FROM $lookupDbName.$lookupTableName
               |WHERE cxi_source='${cryptoShreddingConfig.cxiSource}'
               | AND feed_date='${cryptoShreddingConfig.dateRaw}'
               | AND run_id='${cryptoShreddingConfig.runId}'
               |""".stripMargin
        )
    }

    private def getCryptoShreddingConfig(config: ProcessorConfig, workspaceConfigPath: String) = {
        CryptoShreddingConfig(
            cxiSource = config.cxiPartnerId,
            lookupDestDbName = config.contract.prop[String]("schema.crypto.db_name"),
            lookupDestTableName = config.contract.prop[String]("schema.crypto.lookup_table"),
            workspaceConfigPath = workspaceConfigPath,
            date = config.date,
            runId = config.runId
        )
    }

}
