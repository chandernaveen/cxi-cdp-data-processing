package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.hub.identity.model.IdentityType
import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.ParbrinkRawModels.PhoneNumber
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedPath
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.CryptoShredding
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.normalization.udf.TimestampNormalizationUdfs.parseToTimestampWithPatternAndTimezone
import support.normalization.PhoneNumberNormalization
import support.utils.ContractUtils
import support.WorkspaceConfigReader.readWorkspaceConfig

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object CustomersProcessor {

    private val CustomerTimestampPattern = "yyyy-MM-dd'T'HH:mm:ss.[SSS][SS][S]"
    private val CustomerTimestampTimeZone = "UTC"

    val HashFunctionConfig = Map(
        "pii_columns" -> Seq(
            Map("column" -> "phone_number_normalized", "identity_type" -> IdentityType.Phone.code)
        )
    )

    def process(
        spark: SparkSession,
        config: ProcessorConfig,
        cryptoShreddingConfig: CryptoShreddingConfig,
        destDbName: String
    ): DataFrame = {

        val customersTable = config.contract.prop[String](getSchemaRefinedPath("customer_table"))

        val customers =
            readCustomers(spark, config.dateRaw, config.refinedFullProcess, s"${config.srcDbName}.${config.srcTable}")

        inAuthorizedContext(spark, readWorkspaceConfig(spark, cryptoShreddingConfig.workspaceConfigPath)) {
            val privacyLookupPhones = readPrivacyLookupPhones(spark, config.contract, cryptoShreddingConfig)

            val cryptoShredding = new CryptoShredding(spark, cryptoShreddingConfig)
            val transformedCustomers =
                transformCustomers(customers, privacyLookupPhones, cryptoShredding, config.cxiPartnerId)

            writeCustomers(transformedCustomers, config.cxiPartnerId, s"$destDbName.$customersTable")

            transformedCustomers
        }

    }

    def readCustomers(spark: SparkSession, date: String, refinedFullProcess: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter(
                $"record_type" === ParbrinkRecordType.Customers.value && $"feed_date" === when(
                    lit(refinedFullProcess).equalTo("true"),
                    $"feed_date"
                ).otherwise(date)
            )
            .filter(col("record_value").isNotNull)
            .filter(fromRecordValue("$.IsDisabled") === false)
            .select(
                fromRecordValue("$.Id").as("customer_id"),
                fromRecordValue("$.EmailAddress").as("email_address"),
                fromRecordValue("$.PhoneNumbers").as("phones"),
                fromRecordValue("$.FirstName").as("first_name"),
                fromRecordValue("$.LastName").as("last_name"),
                fromRecordValue("$.RegistrationTime").as("created_at")
            )
    }

    def transformCustomers(
        customers: DataFrame,
        privacyLookupPhones: DataFrame,
        cryptoShredding: CryptoShredding,
        cxiPartnerId: String
    ): DataFrame = {

        val customersWithNormalizedPhones = customers
            .dropDuplicates("customer_id")
            .withColumn(
                "created_at",
                parseToTimestampWithPatternAndTimezone(
                    col("created_at"),
                    lit(CustomerTimestampPattern),
                    lit(CustomerTimestampTimeZone)
                )
            )
            .withColumn(
                "phones",
                from_json(
                    col("phones"),
                    DataTypes.createArrayType(Encoders.product[PhoneNumber].schema)
                )
            )
            .withColumn("phone", explode_outer(col("phones")))
            .join(privacyLookupPhones, col("phone.Number") === privacyLookupPhones("hashed_value"), "left_outer")
            .withColumn("phone_number_full", concat(col("phone.AreaCode"), col("original_value")))
            .withColumn("phone_number_normalized", normalizePhoneNumber(col("phone_number_full")))

        val hashedTransformedCustomers =
            cryptoShredding.applyHashCryptoShredding("common", HashFunctionConfig, customersWithNormalizedPhones)

        hashedTransformedCustomers
            .groupBy("customer_id", "email_address", "first_name", "last_name", "created_at")
            .agg(collect_set(col("phone_number_normalized")).as("phone_numbers"))
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("version", lit(null))
    }

    def writeCustomers(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newCustomers"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId"
               |  AND $destTable.customer_id = $srcTable.customer_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def normalizePhoneNumber: UserDefinedFunction = {
        udf((value: String) => PhoneNumberNormalization.normalizePhoneNumber(value))
    }

    def readPrivacyLookupPhones(
        spark: SparkSession,
        contract: ContractUtils,
        cryptoShreddingConfig: CryptoShreddingConfig
    ): DataFrame = {
        val lookupDbName = contract.prop[String]("schema.crypto.db_name")
        val lookupTableName = contract.prop[String]("schema.crypto.lookup_table")

        spark
            .table(s"$lookupDbName.$lookupTableName")
            .where(col("cxi_source") === cryptoShreddingConfig.cxiSource)
            .where(col("feed_date") === cryptoShreddingConfig.dateRaw)
            .where(col("run_id") === cryptoShreddingConfig.runId)
            .where(col("identity_type") === IdentityType.Phone.code)

    }

}
