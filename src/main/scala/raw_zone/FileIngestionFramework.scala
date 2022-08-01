package com.cxi.cdp.data_processing
package raw_zone

import raw_zone.file_ingestion_framework.FileIngestionFrameworkConfig
import raw_zone.file_ingestion_framework.FileIngestionFrameworkTransformations.transformationFunctionsMap
import raw_zone.file_ingestion_framework.WriteOptionsFunctions._
import support.audit.{AuditLogs, LogContext}
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.CryptoShredding
import support.normalization.DateNormalization
import support.utils.{ContractUtils, DatalakeFiles}
import support.utils.ContractUtils.jobConfigPropName
import support.SparkSessionFactory.getSparkSession

import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, input_file_name, lit, udf}

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import scala.util.{Failure, Success, Try}

object FileIngestionFramework {

    private val logger = Logger.getLogger(this.getClass.getName)

    val basePropName = "jobs.databricks.landing_to_raw_job.job_config"

    def main(args: Array[String]): Unit = {
        logger.info("Main class arguments: " + args.mkString(", "))

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed CLI arguments: $cliArgs")

        val contractPath = "/mnt/" + cliArgs.contractPath
        val np = new ContractUtils(java.nio.file.Paths.get(contractPath))
        val config = FileIngestionFrameworkConfig(cliArgs.date, cliArgs.sourceDateDirFormatter, np, basePropName)
        val feedDate = DateNormalization.formatFromLocalDate(cliArgs.date).get

        val spark: SparkSession = getSparkSession()
        applySparkConfigOptions(spark, config.configOptions)

        try {
            val landingDf = read(config)(spark)
            val finalDf = transform(landingDf, cliArgs, np, config)(spark)
            write(finalDf, config, getWriteOptions(finalDf, feedDate, config))

            val logContext = buildLogContext(config = config, writeStatus = "0", errorMessage = "")
            AuditLogs.write(logContext)(spark)
        } catch {
            case e: RuntimeException =>
                // If run failed write Audit log table to indicate data processing failure
                val logContext = buildLogContext(config = config, writeStatus = "1", errorMessage = e.toString)
                AuditLogs.write(logContext)(spark)
                logger.error(s"Failed to process new files from ${config.sourcePath}. Due to error: ${e.toString}")
                throw e
        }
    }

    def read(config: FileIngestionFrameworkConfig)(implicit spark: SparkSession): DataFrame = {
        if (config.fileFormat == null || config.fileFormat.isEmpty) {
            throw new IllegalArgumentException("The fileFormat parameter is null or empty.")
        }
        if (config.fileFormatOptions == null) {
            throw new IllegalArgumentException("The fileFormatOptions parameter is null.")
        }

        val allFiles = DatalakeFiles.listAllFiles(spark.sparkContext.hadoopConfiguration, config.sourcePath)

        if (allFiles.isEmpty) {
            throw new IllegalStateException(
                s"The folder $config.sourcePath does not contain data files we are looking for."
            )
        }

        val reader = spark.read
            .format(config.fileFormat)
            .options(config.fileFormatOptions)

        reader.load(allFiles: _*)
    }

    def transform(landingDf: DataFrame, cliArgs: CliArgs, np: ContractUtils, config: FileIngestionFrameworkConfig)(
        implicit spark: SparkSession
    ): DataFrame = {
        val pathFileName =
            udf((fileName: String, pathParts: Int) => fileName.split("/").takeRight(pathParts).mkString("/"))
        val enrichedDf = landingDf
            .withColumn("feed_date", lit(DateNormalization.formatFromLocalDate(cliArgs.date).get))
            .withColumn("file_name", pathFileName(input_file_name, lit(config.pathParts)))
            .withColumn("cxi_id", expr("uuid()"))
        val transformationFunction = transformationFunctionsMap(config.transformationName)
        val transformedDf = transformationFunction(enrichedDf)

        val finalDf = if (np.propIsSet(jobConfigPropName(basePropName, "crypto"))) {
            applyCryptoShredding(spark, transformedDf, np, cliArgs.date, cliArgs.runId)
        } else {
            transformedDf
        }
        finalDf
    }

    def write(
        finalDf: DataFrame,
        config: FileIngestionFrameworkConfig,
        writeOptions: Map[String, String]
    ): Unit = {
        val saveMode =
            if (DeltaTable.isDeltaTable(finalDf.sparkSession, config.targetPath)) {
                config.saveModeFromContract
            } else {
                "errorifexists"
            }

        finalDf.write
            .format("delta")
            .partitionBy(config.targetPartitionColumns: _*)
            .mode(saveMode)
            .options(writeOptions)
            .save(config.targetPath)
    }

    def applyCryptoShredding(
        spark: SparkSession,
        transformedDf: DataFrame,
        np: ContractUtils,
        date: LocalDate,
        runId: String
    ): DataFrame = {
        val cryptoShreddingConf = CryptoShreddingConfig(
            cxiSource = np.prop[String](jobConfigPropName(basePropName, "crypto.cxi_source")),
            lookupDestDbName = np.prop[String]("schema.crypto.db_name"),
            lookupDestTableName = np.prop[String]("schema.crypto.lookup_table"),
            workspaceConfigPath = np.prop[String]("databricks_workspace_config"),
            date = date,
            runId = runId
        )
        val cryptoShredding = new CryptoShredding(spark, cryptoShreddingConf)
        val hashFunctionType = np.prop[String](jobConfigPropName(basePropName, "crypto.hash_function_type"))
        val hashFunctionConfig =
            np.prop[Map[String, Any]](jobConfigPropName(basePropName, "crypto.hash_function_config"))
        val cryptoHashedDf = cryptoShredding
            .applyHashCryptoShredding(hashFunctionType, hashFunctionConfig, transformedDf)
        cryptoHashedDf
    }

    def buildLogContext(
        config: FileIngestionFrameworkConfig,
        writeStatus: String,
        errorMessage: String,
        processEndTime: String = java.time.LocalDateTime.now.toString
    ): LogContext = {
        LogContext(
            logTable = config.logTable,
            processName = config.processName,
            entity = config.sourceEntity,
            runID = config.auditRunId,
            dpYear = config.dpYear,
            dpMonth = config.dpMonth,
            dpDay = config.dpDay,
            dpHour = config.dpHour,
            processStartTime = config.notebookStartTime,
            processEndTime = processEndTime,
            writeStatus = writeStatus,
            errorMessage = errorMessage
        )
    }

    def applySparkConfigOptions(spark: SparkSession, configOptions: Map[String, Any]): Unit = {
        configOptions.foreach {
            case (key, value: String) => spark.conf.set(key, value)
            case (key, value: Boolean) => spark.conf.set(key, value)
            case (key, value: Long) => spark.conf.set(key, value)
            case (key, value: Integer) => spark.conf.set(key, value.toLong)
            case (key, value) =>
                throw new IllegalArgumentException(s"""
                       |Haven't found how to alter Apache Spark configuration
                       |for key:value = $key:$value""".stripMargin)
        }
    }

    case class CliArgs(contractPath: String, date: LocalDate, runId: String, sourceDateDirFormat: String = "yyyyMMdd") {
        val sourceDateDirFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(sourceDateDirFormat)
    }

    object CliArgs {
        @throws(classOf[IllegalArgumentException])
        def parse(args: Seq[String]): CliArgs = {
            args match {
                case Seq(contractPath, rawDate, runId, sourceDateDirFormat) =>
                    CliArgs(contractPath, parseDate(rawDate), runId, sourceDateDirFormat)
                case Seq(contractPath, rawDate, runId) => CliArgs(contractPath, parseDate(rawDate), runId)
                case _ =>
                    throw new IllegalArgumentException(
                        "Expected CLI arguments: <contractPath> <date (yyyy-MM-dd)> <runId> <sourceDateDirFormat?>"
                    )
            }
        }

        @throws(classOf[IllegalArgumentException])
        private def parseDate(rawDate: String): LocalDate = {
            Try(LocalDate.parse(rawDate, DateTimeFormatter.ISO_DATE)) match {
                case Success(date) => date
                case Failure(e) =>
                    throw new IllegalArgumentException(
                        s"Unable to parse date from $rawDate, expected format is yyyy-MM-dd",
                        e
                    )
            }
        }
    }

}
