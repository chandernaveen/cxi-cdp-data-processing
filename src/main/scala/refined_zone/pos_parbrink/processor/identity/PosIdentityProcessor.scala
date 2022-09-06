package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity._
import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedHubPath
import refined_zone.service.MetadataService.extractMetadata
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.CryptoShredding
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.utils.ContractUtils
import support.WorkspaceConfigReader.readWorkspaceConfig

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, struct, udf}

object PosIdentityProcessor {

    val HashFunctionConfig = Map(
        "pii_columns" -> Seq(
            Map("column" -> CxiIdentityId, "identity_type" -> IdentityType.CardHolderNameTypeNumber.code)
        )
    )

    def process(
        spark: SparkSession,
        config: ProcessorConfig,
        cryptoShreddingConfig: CryptoShreddingConfig,
        payments: DataFrame,
        customers: DataFrame
    ): DataFrame = {

        val refinedHubDbName = config.contract.prop[String](getSchemaRefinedHubPath("db_name"))

        val posIdentityIntermediateTable =
            config.contract.prop[String](getSchemaRefinedHubPath("identity_intermediate_table"))

        val fromHouseAccountsIdentities = HouseAccountsIdentitiesProcessor.process(spark, config)

        val cryptoShredding = new CryptoShredding(spark, cryptoShreddingConfig)
        val fromPaymentsIdentities =
            PaymentsIdentitiesProcessor.computeIdentitiesFromPayments(payments, cryptoShredding)

        val allIdentities =
            inAuthorizedContext(spark, readWorkspaceConfig(spark, cryptoShreddingConfig.workspaceConfigPath)) {
                val fromCustomersIdentities = CustomersIdentitiesProcessor.process(spark, config, customers)

                val allIdentities = fromHouseAccountsIdentities
                    .unionByName(fromPaymentsIdentities)
                    .unionByName(fromCustomersIdentities)

                val privacyTable = readPrivacyLookupTable(spark, config.contract, cryptoShreddingConfig)
                val identitiesWithMetadata = addCxiIdentitiesMetadata(privacyTable, allIdentities)
                writeIdentities(
                    identitiesWithMetadata,
                    s"$refinedHubDbName.$posIdentityIntermediateTable",
                    config.dateRaw,
                    config.runId
                )
                allIdentities
            }

        groupIdentitiesByOrder(allIdentities)

    }

    def groupIdentitiesByOrder(allIdentities: DataFrame): DataFrame = {
        allIdentities
            .groupBy("order_id", "location_id")
            .agg(collect_list(struct(col(Type) as "identity_type", col(CxiIdentityId))) as CxiIdentityIds)
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

    def addCxiIdentitiesMetadata(privacyTable: DataFrame, allIdentities: DataFrame): DataFrame = {
        val cxiIdentities = allIdentities
            .select(CxiIdentityId, Type, Weight)
            .dropDuplicates(CxiIdentityId, Type)
            .join(
                privacyTable,
                allIdentities(CxiIdentityId) === privacyTable("hashed_value") &&
                    allIdentities(Type) === privacyTable("identity_type"),
                "left"
            )

        val metadataUdf = udf(extractMetadata _)
        cxiIdentities
            .withColumn(Metadata, metadataUdf(col(Type), col("original_value")))
            .select(CxiIdentityId, Type, Weight, Metadata)
    }

    def writeIdentities(
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

}
