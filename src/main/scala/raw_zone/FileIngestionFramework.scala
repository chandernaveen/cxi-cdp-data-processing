package com.cxi.cdp.data_processing
package raw_zone

import raw_zone.FileIngestionFrameworkTransformations.transformationFunctionsMap
import raw_zone.config.FileIngestionFrameworkConfig
import support.SparkSessionFactory.getSparkSession
import support.crypto_shredding.CryptoShredding
import support.crypto_shredding.config.CryptoShreddingConfig
import support.exceptions.CryptoShreddingException
import support.functions.LogContext
import support.functions.UnifiedFrameworkFunctions._
import support.packages.utils.ContractUtils
import support.packages.utils.ContractUtils.jobConfigPropName

import com.databricks.service.DBUtils
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{expr, input_file_name, lit, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.Seq
import scala.util.{Failure, Success, Try}

object FileIngestionFramework {

    val basePropName = "jobs.databricks.landing_to_raw_job.job_config"

    def main(args: Array[String]): Unit = {
        val logger: Logger = configureLogger()
        logger.info("Main class arguments: " + args.mkString(", "))

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed CLI arguments: $cliArgs")

        val contractPath = "/mnt/" + cliArgs.contractPath
        val np = new ContractUtils(java.nio.file.Paths.get(contractPath))
        val config = FileIngestionFrameworkConfig(cliArgs.date, cliArgs.sourceDateDirFormatter, np, basePropName)

        val spark: SparkSession = getSparkSession()
        applySparkConfigOptions(spark, config.configOptions)

        var landingDF: DataFrame = null
        var filesProcessed: Seq[String] = null
        try {
            val allFiles = getAllFiles(spark.sparkContext.hadoopConfiguration, config.sourcePath)
            val processedResult = if (config.fileFormat.isEmpty) {
                throw new RuntimeException("The fileFormat parameter is empty.")
            } else {
                processFilesBasedOnFileFormat(config.sourcePath, allFiles, config.fileFormat, config.fileFormatOptions, config.schema)(spark)
            }
            filesProcessed = processedResult._1
            landingDF = processedResult._2
        }
        catch {
            case e: Throwable =>
                // If run failed write Audit log table to indicate data processing failure
                val logContext = buildLogContext(
                    config = config, processEndTime = java.time.LocalDateTime.now.toString, writeStatus = "0", errorMessage = e.toString)

                fnWriteAuditTable(logContext, logger = logger, spark = spark)
                logger.error(s"Failed to new files from ${config.sourcePath}. Due to error: ${e.toString}")
                throw e
        }

        processLandingDF(logger, cliArgs, np, config, spark, landingDF)

        val files = filesProcessed.size
        val logContext = buildLogContext(
            config = config, processEndTime = java.time.LocalDateTime.now.toString, writeStatus = "1", errorMessage = "")
        fnWriteAuditTable(logContext = logContext, logger = logger, spark = spark)
        logger.info(s"""Files processed: $files""")
    }

    private def processLandingDF
        (logger: Logger, cliArgs: CliArgs, np: ContractUtils, config: FileIngestionFrameworkConfig, spark: SparkSession, landingDF: DataFrame): Unit = {
        try {
            val pathFileName = udf((fileName: String, pathParts: Int) => fileName.split("/").takeRight(pathParts).mkString("/"))
            val feedDate = cliArgs.date.toString
            val finalDF = landingDF
                .withColumn("feed_date", lit(feedDate))
                .withColumn("file_name", pathFileName(input_file_name, lit(config.pathParts)))
                .withColumn("cxi_id", expr("uuid()"))

            val saveMode = if (DeltaTable.isDeltaTable(spark, config.targetPath)) config.saveModeFromContract else "errorifexists"

            val transformationFunction = transformationFunctionsMap(config.transformationName)
            val transformedDf = transformationFunction(finalDF)

            val finalDf = if (np.propIsSet(jobConfigPropName(basePropName, "crypto"))) {
                val cryptoShreddingConf = CryptoShreddingConfig(np)
                val cryptoShredding = new CryptoShredding(spark, cryptoShreddingConf)
                val hashFunctionType = np.prop[String](jobConfigPropName(basePropName, "crypto.hash_function_type"))
                val hashFunctionConfig = np.prop[Map[String, Any]](jobConfigPropName(basePropName, "crypto.hash_function_config"))
                val cryptoHashedDf = cryptoShredding
                    .applyHashCryptoShredding(hashFunctionType, hashFunctionConfig, transformedDf)
                cryptoHashedDf
            } else {
                transformedDf
            }

            finalDf
                .write
                .format("delta")
                .partitionBy(config.targetPartitionColumns: _*)
                .mode(saveMode)
                .options(getWriteOptions(transformedDf, feedDate, config))
                .save(config.targetPath)
        } catch {
            case cryptoShreddingEx: CryptoShreddingException =>
                logger.error(s"Failed to apply crypto shredding ${cryptoShreddingEx.getMessage}", cryptoShreddingEx)
                val logContext = buildLogContext(
                    config = config, processEndTime = java.time.LocalDateTime.now.toString, writeStatus = "0", errorMessage = cryptoShreddingEx.toString)
                fnWriteAuditTable(logContext = logContext, logger = logger, spark = spark)
                throw cryptoShreddingEx
            case e: Throwable =>
                // If run failed write Audit log table to indicate data processing failure
                val logContext = buildLogContext(
                    config = config, processEndTime = java.time.LocalDateTime.now.toString, writeStatus = "0", errorMessage = e.toString)
                fnWriteAuditTable(logContext = logContext, logger = logger, spark = spark)
                logger.error(s"Failed to write to delta location ${config.targetPath}. Due to error: ${e.toString}")
                throw e
        }
    }

    def processFilesBasedOnFileFormat(sourcePath: String,
                                      files: Seq[String],
                                      format: String,
                                      options: Map[String, String],
                                      schema: Option[StructType])(implicit spark: SparkSession): (Seq[String], DataFrame) = {
        if (files.nonEmpty) {
            val reader = spark.read
                .format(format)
                .options(options)
            schema.foreach(reader.schema(_))
            (files, reader.load(files: _*))
        } else {
            throw new RuntimeException(s"The folder $sourcePath does not contain data files we are looking for.")
        }
    }

    def getWriteOptions(df: DataFrame, feedDate: String, config: FileIngestionFrameworkConfig): Map[String, String] = {
        val functionsMap: Map[String, WriteOptionsFunction] =
            writeOptionsFunctionsMap + ("replaceWhereForFeedDate" -> replaceWhereForFeedDate(feedDate))

        config.writeOptionsFunctionName match {
            case None => config.writeOptions
            case Some(functionName) => functionsMap(functionName)(df, config.writeOptionsFunctionParams)
        }
    }

    def buildLogContext(
                           config: FileIngestionFrameworkConfig,
                           processEndTime: String,
                           writeStatus: String,
                           errorMessage: String): LogContext = {
        LogContext(
            logTable = config.logTable,
            processName = config.processName,
            entity = config.sourceEntity,
            runID = config.runID,
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

    private def configureLogger(): Logger = {
        val loggerName = Try(DBUtils.widgets.get("loggerName")).getOrElse("RawLogger")
        val logSystem = Try(DBUtils.widgets.get("logSystem")).getOrElse("App")
        val logLevel = Try(DBUtils.widgets.get("logLevel")).getOrElse("INFO")
        val logAppender = Try(DBUtils.widgets.get("logAppender")).getOrElse("RawFile")
        val isRootLogEnabled = Try(DBUtils.widgets.get("isRootLogEnabled")).getOrElse("False")
        fnInitializeLogger(loggerName, logSystem, logLevel, logAppender, isRootLogEnabled)
    }

    case class CliArgs(contractPath: String,
                       date: LocalDate,
                       sourceDateDirFormat: String = "yyyyMMdd") {
        val sourceDateDirFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(sourceDateDirFormat)
    }

    object CliArgs {
        @throws(classOf[IllegalArgumentException])
        def parse(args: Seq[String]): CliArgs = {
            args match {
                case Seq(contractPath, rawDate, sourceDateDirFormat) =>
                    CliArgs(contractPath, parseDate(rawDate), sourceDateDirFormat)
                case Seq(contractPath, rawDate) => CliArgs(contractPath, parseDate(rawDate))
                case _ => throw new IllegalArgumentException("Expected CLI arguments: <contractPath> <date (yyyy-MM-dd)> <sourceDateDirFormat?>")
            }
        }

        @throws(classOf[IllegalArgumentException])
        private def parseDate(rawDate: String): LocalDate = {
            Try(LocalDate.parse(rawDate, DateTimeFormatter.ISO_DATE)) match {
                case Success(date) => date
                case Failure(e) => throw new IllegalArgumentException(s"Unable to parse date from $rawDate, expected format is yyyy-MM-dd", e)
            }
        }
    }

}
