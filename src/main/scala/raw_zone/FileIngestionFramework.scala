package com.cxi.cdp.data_processing
package raw_zone

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import raw_zone.FileIngestionFrameworkTransformations.transformationFunctionsMap
import support.SparkSessionFactory.getSparkSession
import support.crypto_shredding.CryptoShredding
import support.functions.UnifiedFrameworkFunctions._
import support.packages.utils.ContractUtils

import com.cxi.cdp.data_processing.support.exceptions.CryptoShreddingException
import com.databricks.service.DBUtils
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{expr, input_file_name, lit, udf}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Seq
import scala.util.{Failure, Success, Try}

object FileIngestionFramework {

    def main(args: Array[String]): Unit = {
        val logger: Logger = configureLogger()

        logger.info("Main class arguments: " + args.mkString(", "))

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed CLI arguments: $cliArgs")

        val contractPath = "/mnt/" + cliArgs.contractPath
        val np = new ContractUtils(java.nio.file.Paths.get(contractPath))

        val sourcePath: String = getSourcePath("/mnt/" + np.prop(jobConfigPropName("read.path")), cliArgs.date, cliArgs.sourceDateDirFormatter)
        val targetPath: String = "/mnt/" + np.prop(jobConfigPropName("write.path"))
        val targetPartitionColumns: Seq[String] = try {
            np.prop(jobConfigPropName("write.partitionColumns"))
        } catch {
            case _: Throwable => Seq[String]("feed_date")
        }
        val processName: String = np.prop(jobConfigPropName("audit.processName"))
        val sourceEntity: String = np.prop(jobConfigPropName("audit.entity"))
        val logTable: String = np.prop(jobConfigPropName("audit.logTable"))
        val pathParts: Int = np.propOrElse[Int](jobConfigPropName("read.pathParts"), 1)
        val fileFormat: String = np.propOrElse[String](jobConfigPropName("read.fileFormat"), "")
        val fileFormatOptions: Map[String, String] = np.propOrElse[Map[String, String]](jobConfigPropName("read.fileFormatOptions"), Map[String, String]())
        val transformationName: String = np.propOrElse[String](jobConfigPropName("transform.transformationName"), "identity")
        val schemaPath: String = np.propOrElse[String](jobConfigPropName("read.schemaPath"), "")
        val saveModeFromContract: String = np.propOrElse[String](jobConfigPropName("write.saveMode"), "append")
        val configOptions: Map[String, Any] = np.propOrElse[Map[String, Any]](jobConfigPropName("spark.configOptions"), Map[String, Any]())
        val writeOptions: Map[String, String] = np.propOrElse[Map[String, String]](jobConfigPropName("write.writeOptions"), Map[String, String]())
        val writeOptionsFunctionName: String = np.propOrElse[String](jobConfigPropName("write.writeOptionsFunction"), "")
        val writeOptionsFunctionParams: Map[String, String] = np.propOrElse[Map[String, String]](jobConfigPropName("write.writeOptionsFunctionParams"), Map[String, String]())

        val spark: SparkSession = getSparkSession()
        val runID: Int = 1
        val dpYear = cliArgs.date.getYear.toString
        val dpMonth = cliArgs.date.getMonthValue.toString
        val dpDay = cliArgs.date.getDayOfMonth.toString
        val dpHour = 0.toString // data is being processed on a daily basis

        val notebookStartTime = java.time.LocalDateTime.now.toString

        val schema: Option[StructType] = if (schemaPath.nonEmpty) Some(DataType.fromJson(DBUtils.fs.head(s"/mnt/$schemaPath")).asInstanceOf[StructType]) else None

        applySparkConfigOptions(spark, configOptions)

        var landingDF: DataFrame = null
        var filesProcessed: Seq[String] = null
        try {
            val allFiles = getAllFiles(spark.sparkContext.hadoopConfiguration, sourcePath)
            val processedResult = if (fileFormat.isEmpty) {
                throw new RuntimeException(s"The fileFormat parameter is empty.")
            } else {
                processFilesBasedOnFileFormat(sourcePath, allFiles, fileFormat, fileFormatOptions, schema)(spark)
            }
            filesProcessed = processedResult._1
            landingDF = processedResult._2
        }
        catch {
            case e: Throwable =>
                // If run failed write Audit log table to indicate data processing failure
                fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "0", logger = logger,
                    dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour,
                    processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString, errorMessage = e.toString, spark = spark)
                logger.error(s"Failed to new files from $sourcePath. Due to error: ${e.toString}")
                throw e
        }

        try {
            val pathFileName = udf((fileName: String, pathParts: Int) => fileName.split("/").takeRight(pathParts).mkString("/"))

            val finalDF = landingDF
                .withColumn("feed_date", lit(cliArgs.date.toString))
                .withColumn("file_name", pathFileName(input_file_name, lit(pathParts)))
                .withColumn("cxi_id", expr("uuid()"))

            val saveMode = if (DeltaTable.isDeltaTable(spark, targetPath)) saveModeFromContract else "errorifexists"

            val transformationFunction = transformationFunctionsMap(transformationName)
            val transformedDf = transformationFunction(finalDF)


            val finalDf = if (np.propIsSet(jobConfigPropName("crypto"))) {
                val country: String = np.prop[String]("partner.country")
                val cxiPartnerId: String = np.prop[String]("partner.cxiPartnerId")
                val lookupDestDbName: String = np.prop[String]("schema.crypto.db_name")
                val lookupDestTableName: String = np.prop[String]("schema.crypto.lookup_table")
                val workspaceConfigPath: String = np.prop[String]("databricks_workspace_config")

                val cryptoShredding = new CryptoShredding(spark, country, cxiPartnerId, lookupDestDbName, lookupDestTableName, workspaceConfigPath)
                val hashFunctionType = np.prop[String](jobConfigPropName("crypto.hash_function_type"))
                val hashFunctionConfig = np.prop[Map[String, Any]](jobConfigPropName("crypto.hash_function_config"))
                val cryptoHashedDf = cryptoShredding.applyHashCryptoShredding(hashFunctionType, hashFunctionConfig, transformedDf)
                cryptoHashedDf
            } else {
                transformedDf
            }

            finalDf
                .write
                .format("delta")
                .partitionBy(targetPartitionColumns: _*)
                .mode(saveMode)
                .options(getWriteOptions(transformedDf, writeOptionsFunctionName, writeOptionsFunctionParams, writeOptions))
                .save(targetPath)

        }
        catch {
            case cryptoShreddingEx: CryptoShreddingException =>
                logger.error(s"Failed to apply crypto shredding ${cryptoShreddingEx.getMessage}", cryptoShreddingEx)
                fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "0", logger = logger, dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour, processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString, errorMessage = cryptoShreddingEx.toString, spark = spark)
                throw cryptoShreddingEx
            case e: Throwable =>
                // If run failed write Audit log table to indicate data processing failure
                fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "0", logger = logger,
                    dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour,
                    processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString, errorMessage = e.toString, spark = spark)
                logger.error(s"Failed to write to delta location $targetPath. Due to error: ${e.toString}")
                throw e
        }

        val files = filesProcessed.size
        fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "1", logger = logger,
            dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour,
            processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString, errorMessage = "", spark = spark)
        logger.info(s"""Files processed: $files""")
    }

    private def jobConfigPropName(relativePropName: String): String = {
        "jobs.databricks.landing_to_raw_job.job_config." + relativePropName
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

    def getWriteOptions(df: DataFrame, functionName: String, functionParams: Map[String, String], options: Map[String, String]): Map[String, String] = {
        if (functionName.isEmpty) {
            options
        } else {
            writeOptionsFunctionsMap(functionName)(df, functionParams)
        }
    }

    private def configureLogger(): Logger = {
        val loggerName = Try(DBUtils.widgets.get("loggerName")).getOrElse("RawLogger")
        val logSystem = Try(DBUtils.widgets.get("logSystem")).getOrElse("App")
        val logLevel = Try(DBUtils.widgets.get("logLevel")).getOrElse("INFO")
        val logAppender = Try(DBUtils.widgets.get("logAppender")).getOrElse("RawFile")
        val isRootLogEnabled = Try(DBUtils.widgets.get("isRootLogEnabled")).getOrElse("False")
        fn_initializeLogger(loggerName, logSystem, logLevel, logAppender, isRootLogEnabled)
    }

    private final val PathDelimiter = "/"

    private def concatPaths(parent: String, child: String) = {
        if (parent.endsWith(PathDelimiter)) {
            parent + child
        } else {
            parent + PathDelimiter + child
        }
    }

    /** Creates the final source path from the base path (which comes from a contract) and the processing date. */
    private def getSourcePath(basePath: String, date: LocalDate, sourceDateDirFormat: DateTimeFormatter): String = {
        concatPaths(basePath, sourceDateDirFormat.format(date))
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
