package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity._
import refined_zone.pos_toast.config.ProcessorConfig
import refined_zone.pos_toast.RawRefinedToastPartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}
import refined_zone.service.MetadataService.extractMetadata
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.CryptoShredding
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.utils.ContractUtils
import support.WorkspaceConfigReader

import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object PosIdentityProcessor {

    // scalastyle:off method.length
    def process(spark: SparkSession, config: ProcessorConfig, payments: DataFrame): DataFrame = {
        val refinedHubDbName = config.contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val identityIntermediateTable =
            config.contract.prop[String](getSchemaRefinedHubPath("identity_intermediate_table"))
        val refinedDbName = config.contract.prop[String](getSchemaRefinedPath("db_name"))
        val customerTableName = config.contract.prop[String](getSchemaRefinedPath("customer_table"))

        val workspaceConfigPath: String = config.contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)
        val cryptoShreddingConfig: CryptoShreddingConfig = getCryptoShreddingConfig(config, workspaceConfigPath)

        val customersDimension = readCustomerDim(spark, s"$refinedDbName.$customerTableName", config.cxiPartnerId)
        val orderCustomerDataWithAppliedPreauthInfo =
            readOrderCustomerDataWithAppliedPreauthInfoIfAvailable(
                spark,
                config.dateRaw,
                s"${config.srcDbName}.${config.srcTable}"
            )

        val transformedOrderCustomerDataWithAppliedPreauthInfo =
            transformOrderCustomerDataWithAppliedPreauthInfoIfAvailable(
                orderCustomerDataWithAppliedPreauthInfo,
                payments
            )
        val computedPosIdentitiesWeight2 =
            computePosIdentitiesWeight2(cryptoShreddingConfig, transformedOrderCustomerDataWithAppliedPreauthInfo)(
                spark
            )

        inAuthorizedContext(spark, workspaceConfig) {
            val privacyTable = readPrivacyLookupTable(spark, config.contract, cryptoShreddingConfig)
            val posIdentitiesWeight3 = findPosIdentitiesWeight3ForAnOrder(
                transformedOrderCustomerDataWithAppliedPreauthInfo,
                customersDimension,
                privacyTable
            )
            val posIdentitiesAllWeights = posIdentitiesWeight3.unionAll(computedPosIdentitiesWeight2)
            val cxiIdentitiesWithMetadata = enrichPosIdentityWithMetadata(privacyTable, posIdentitiesAllWeights)

            writePosIdentities(
                cxiIdentitiesWithMetadata,
                s"$refinedHubDbName.$identityIntermediateTable",
                cryptoShreddingConfig.dateRaw,
                cryptoShreddingConfig.runId
            )

            val posIdentitiesByOrderIdAndLocationId = posIdentitiesAllWeights
                .groupBy("order_id", "location_id")
                .agg(collect_list(struct(col(Type) as "identity_type", col(CxiIdentityId))) as CxiIdentityIds)

            posIdentitiesByOrderIdAndLocationId
        }
    }

    def readCustomerDim(spark: SparkSession, tableName: String, cxiPartnerId: String): DataFrame = {
        import spark.implicits._

        spark
            .table(tableName)
            .where($"cxi_partner_id" === cxiPartnerId)
            .select($"customer_id", $"email_address", $"phone_number")
            .filter($"email_address".isNotNull or $"phone_number".isNotNull)
    }

    // scalastyle:off method.length
    def readOrderCustomerDataWithAppliedPreauthInfoIfAvailable(
        spark: SparkSession,
        date: String,
        table: String
    ): DataFrame = {
        import spark.implicits._

        val rawOrdersAsStrings = spark
            .table(table)
            .filter($"record_type" === "orders" && $"feed_date" === date)
            .select("record_value", "location_id")

        val ordersRecordTypeDf = spark.read.json(rawOrdersAsStrings.select("record_value").as[String](Encoders.STRING))

        val rawChecksAsJsonStructs = rawOrdersAsStrings
            .withColumn("record_value", from_json(col("record_value"), ordersRecordTypeDf.schema))
            .select(
                $"record_value.guid".as("order_id"),
                $"location_id",
                explode($"record_value.checks").as("check")
            )
            .withColumn("customer_id", col("check.customer.guid"))
            .select("order_id", "location_id", "customer_id", "check.*")

        val rawChecksWithAppliedPreauthInfo = if (rawChecksAsJsonStructs.columns.contains("appliedPreauthInfo")) {
            rawChecksAsJsonStructs
        } else {
            rawChecksAsJsonStructs.withColumn("appliedPreauthInfo", lit(null).cast(new StructType()))
        }

        val df = rawChecksWithAppliedPreauthInfo
            .select($"order_id", $"location_id", $"customer_id", $"appliedPreauthInfo.*")

        Seq(
            "cardType",
            "last4CardDigits",
            "cardHolderFirstName",
            "cardHolderLastName",
            "cardHolderExpirationMonth",
            "cardHolderExpirationYear"
        )
            .foldLeft(df) { case (acc, col_name) =>
                if (df.columns.contains(col_name)) {
                    acc.withColumn(col_name, col(col_name))
                } else {
                    acc.withColumn(col_name, lit(null))
                }
            }
            .select(
                $"order_id",
                $"location_id",
                $"customer_id",
                $"cardType" as "card_brand",
                $"last4CardDigits" as "pan",
                $"cardHolderFirstName" as "first_name",
                $"cardHolderLastName" as "last_name",
                $"cardHolderExpirationMonth" as "exp_month",
                $"cardHolderExpirationYear" as "exp_year"
            )
    }

    def transformOrderCustomerDataWithAppliedPreauthInfoIfAvailable(
        customersPreAuth: DataFrame,
        payments: DataFrame
    ): DataFrame = {
        import customersPreAuth.sparkSession.implicits._

        val customerPreAuthAliased = customersPreAuth.as("pre_auth")
        val paymentsAliased = payments.as("p")
        customerPreAuthAliased
            .join(paymentsAliased, customerPreAuthAliased("order_id") === paymentsAliased("order_id"), "full_outer")
            .withColumn("order_id_final", coalesce($"pre_auth.order_id", $"p.order_id"))
            .withColumn("card_brand_final", coalesce($"pre_auth.card_brand", $"p.card_brand"))
            .withColumn("bin_final", $"p.bin") // no bin in schema in preAuth
            .withColumn("pan_final", coalesce($"pre_auth.pan", $"p.pan"))
            .withColumn("first_name_final", coalesce($"pre_auth.first_name", $"p.first_name"))
            .withColumn("last_name_final", coalesce($"pre_auth.last_name", $"p.last_name"))
            .withColumn("exp_month_final", coalesce($"pre_auth.exp_month", $"p.exp_month"))
            .withColumn("exp_year_final", coalesce($"pre_auth.exp_year", $"p.exp_year"))
            .select(
                $"order_id_final" as "order_id",
                $"p.location_id" as "location_id",
                $"customer_id",
                $"card_brand_final" as "card_brand",
                $"bin_final" as "bin",
                $"pan_final" as "pan",
                $"first_name_final" as "first_name",
                $"last_name_final" as "last_name",
                $"exp_month_final" as "exp_month",
                $"exp_year_final" as "exp_year"
            )
    }

    def computePosIdentitiesWeight2(
        cryptoShreddingConfig: CryptoShreddingConfig,
        customerCardPaymentDetails: DataFrame
    )(implicit spark: SparkSession): DataFrame = {

        val combinationBinIdentities = createCombinationBinIdentities(customerCardPaymentDetails)
        val combinationCardIdentities = createCombinationCardIdentities(customerCardPaymentDetails)

        val cryptoShredding = new CryptoShredding(spark, cryptoShreddingConfig)

        val hashedCombinationBinIdentities = cryptoShredding
            .applyHashCryptoShredding(
                "common",
                Map(
                    "pii_columns" -> Seq(
                        Map("column" -> CxiIdentityId, "identity_type" -> IdentityType.CombinationBin.code)
                    )
                ),
                combinationBinIdentities
            )
        val hashedCombinationCardIdentities = cryptoShredding
            .applyHashCryptoShredding(
                "common",
                Map(
                    "pii_columns" -> Seq(
                        Map("column" -> CxiIdentityId, "identity_type" -> IdentityType.CombinationCard.code)
                    )
                ),
                combinationCardIdentities
            )

        hashedCombinationBinIdentities.unionByName(hashedCombinationCardIdentities)
    }

    private def createCombinationBinIdentities(fullCustomerData: DataFrame): DataFrame = {
        fullCustomerData
            .filter(col("bin").isNotNull and commonCombinationFilters)
            .select(
                col("order_id"),
                col("location_id"),
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

    private def createCombinationCardIdentities(fullCustomerData: DataFrame): DataFrame = {
        fullCustomerData
            .filter(commonCombinationFilters)
            .withColumn(
                "first_and_last_name",
                when(
                    col("first_name").isNotNull and (col("first_name") notEqual "") and
                        col("last_name").isNotNull and (col("last_name") notEqual ""),
                    concat(trim(lower(col("first_name"))), lit("/"), trim(lower(col("last_name"))))
                ).otherwise(lit(null))
            )
            .filter(col("first_and_last_name").isNotNull)
            .select(
                col("order_id"),
                col("location_id"),
                concat(
                    col("first_and_last_name"),
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

    def findPosIdentitiesWeight3ForAnOrder(
        transformedOrderCustomerData: DataFrame,
        customers: DataFrame,
        privacyTable: DataFrame
    ): DataFrame = {
        import customers.sparkSession.implicits._

        val privacyIntermediateEmailsAndPhones = privacyTable
            .filter(col("identity_type").isInCollection(Array(IdentityType.Phone.code, IdentityType.Email.code)))
            .select(
                col("original_value"),
                col("hashed_value").as(CxiIdentityId),
                col("identity_type").as(Type)
            )

        transformedOrderCustomerData
            .select("order_id", "location_id", "customer_id")
            .as("transformed_ord_cust_data")
            .join(customers.as("cust"), $"transformed_ord_cust_data.customer_id" === $"cust.customer_id", "inner")
            .select($"order_id", $"location_id", $"email_address", $"phone_number")
            .join(
                privacyIntermediateEmailsAndPhones,
                ($"email_address" === privacyIntermediateEmailsAndPhones(CxiIdentityId) &&
                    lit(IdentityType.Email.code) === privacyIntermediateEmailsAndPhones(Type)) ||
                    ($"phone_number" === privacyIntermediateEmailsAndPhones(CxiIdentityId) &&
                        lit(IdentityType.Phone.code) === privacyIntermediateEmailsAndPhones(Type)),
                "inner"
            )
            .select(
                "order_id",
                "location_id",
                CxiIdentityId,
                Type
            )
            .withColumn(Weight, lit(3))
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

        val srcTable = "newPosIdentities"
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
