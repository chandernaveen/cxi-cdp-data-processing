package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import raw_zone.pos_omnivore.model.Payment
import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity._
import refined_zone.pos_omnivore.config.ProcessorConfig
import refined_zone.pos_omnivore.RawRefinedOmnivorePartnerJob.getSchemaRefinedHubPath
import refined_zone.service.MetadataService.extractMetadata
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.CryptoShredding
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.utils.ContractUtils
import support.WorkspaceConfigReader

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object PosIdentityProcessor {

    def process(spark: SparkSession, config: ProcessorConfig): DataFrame = {

        val refinedHubDbName = config.contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val identityIntermediateTable =
            config.contract.prop[String](getSchemaRefinedHubPath("identity_intermediate_table"))

        val workspaceConfigPath: String = config.contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)
        val cryptoShreddingConfig: CryptoShreddingConfig = getCryptoShreddingConfig(config, workspaceConfigPath)

        val orderPaymentData = readOrderPaymentData(spark, config.dateRaw, config.srcDbName, config.srcTable)
        val transformedOrderPaymentData = transformOrderPayment(config.cxiPartnerId, orderPaymentData)

        val computedPosIdentitiesWeight2 =
            computePosIdentitiesWeight2(cryptoShreddingConfig, transformedOrderPaymentData)(spark)

        inAuthorizedContext(spark, workspaceConfig) {
            val privacyTable = readPrivacyLookupTable(spark, config.contract, cryptoShreddingConfig)
            val posIdentitiesAllWeights = computedPosIdentitiesWeight2
            val posIdentitiesWithMetadata = enrichPosIdentityWithMetadata(privacyTable, posIdentitiesAllWeights)

            writePosIdentities(
                posIdentitiesWithMetadata,
                s"$refinedHubDbName.$identityIntermediateTable",
                cryptoShreddingConfig.dateRaw,
                cryptoShreddingConfig.runId
            )

            val posIdentitiesByOrderIdAndLocationId = posIdentitiesAllWeights
                .groupBy("ord_id", "location_id")
                .agg(collect_list(struct(col(Type) as "identity_type", col(CxiIdentityId))) as CxiIdentityIds)

            posIdentitiesByOrderIdAndLocationId
        }
    }

    def readOrderPaymentData(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as ord_id,
               |get_json_object(record_value, "$$._links.self.href") as location_url,
               |get_json_object(record_value, "$$._embedded.payments") as payments
               |FROM $dbName.$table
               |WHERE record_type="tickets" AND get_json_object(record_value, "$$.open")="false" AND get_json_object(record_value, "$$.void")="false"
               |    AND get_json_object(record_value, "$$._embedded.payments") != "[]" AND feed_date="$date"
               |""".stripMargin)
    }

    def transformOrderPayment(
        cxiPartnerId: String,
        orderPaymentData: DataFrame
    ): DataFrame = {

        val locIdIndex: Int = 5
        orderPaymentData
            .withColumn("location_id", split(col("location_url"), "/")(locIdIndex))
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn(
                "payments",
                from_json(col("payments"), DataTypes.createArrayType(Encoders.product[Payment].schema))
            )
            .withColumn("payment", explode(col("payments")))
            .withColumn("tender_type", col("payment._embedded.tender_type.name"))
            .withColumn("ord_payment_last4", col("payment.last4"))
            .withColumn(
                "ord_customer_full_name",
                when(col("payment.full_name").isNull, lit("")).otherwise(col("payment.full_name"))
            )
            .select(
                "ord_id",
                "location_id",
                "cxi_partner_id",
                "tender_type",
                "ord_payment_last4",
                "ord_customer_full_name"
            )
    }

    def computePosIdentitiesWeight2(cryptoShreddingConfig: CryptoShreddingConfig, orderCardPaymentData: DataFrame)(
        implicit spark: SparkSession
    ): DataFrame = {

        val combinationCardIdentities = createCombinationCardIdentities(orderCardPaymentData)

        val cryptoShredding = new CryptoShredding(spark, cryptoShreddingConfig)

        val hashedCombinationCardIdentities = cryptoShredding
            .applyHashCryptoShredding(
                "common",
                Map(
                    "pii_columns" -> Seq(
                        Map("column" -> CxiIdentityId, "identity_type" -> IdentityType.CombinationCardSlim.code)
                    )
                ),
                combinationCardIdentities
            )

        hashedCombinationCardIdentities
    }

    private def createCombinationCardIdentities(orderCardPaymentData: DataFrame): DataFrame = {

        orderCardPaymentData
            .filter(col("tender_type").isNotNull and col("ord_payment_last4").isNotNull)
            .select(
                col("ord_id"),
                col("location_id"),
                concat(
                    lower(col("ord_customer_full_name")),
                    lit("-"),
                    lower(col("tender_type")),
                    lit("-"),
                    lower(col("ord_payment_last4"))
                ).as(CxiIdentityId)
            )
            .withColumn(Type, lit(IdentityType.CombinationCardSlim.code))
            .withColumn(Weight, lit(2))
    }

    def enrichPosIdentityWithMetadata(privacyTable: DataFrame, posIdentities: DataFrame): DataFrame = {

        val posIdentitiesWithPrivacyInfo = posIdentities
            .select(CxiIdentityId, Type, Weight)
            .dropDuplicates(CxiIdentityId, Type)
            .join(
                privacyTable,
                posIdentities(CxiIdentityId) === privacyTable("hashed_value") &&
                    posIdentities(Type) === privacyTable("identity_type"),
                "left"
            )

        val metadataUdf = udf(extractMetadata _)
        posIdentitiesWithPrivacyInfo
            .withColumn(Metadata, metadataUdf(col(Type), col("original_value")))
            .drop("original_value", "hashed_value", "identity_type")
    }

    def writePosIdentities(
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

        import spark.implicits._
        spark
            .table(s"$lookupDbName.$lookupTableName")
            .filter(
                $"feed_date" === cryptoShreddingConfig.dateRaw and
                    $"run_id" === cryptoShreddingConfig.runId and
                    $"cxi_source" === cryptoShreddingConfig.cxiSource
            )
            .select("original_value", "hashed_value", "identity_type")
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
