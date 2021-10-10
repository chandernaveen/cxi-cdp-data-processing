package com.cxi.cdp.data_processing
package raw_zone

import raw_zone.FileIngestionFrameworkTransformations.transformationFunctionsMap
import support.functions.UnifiedFrameworkFunctions._
import support.packages.utils.ContractUtils

import com.cxi.cdp.data_processing.support.SparkSessionFactory.getSparkSession
import com.databricks.service.DBUtils
import io.delta.tables.DeltaTable
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{expr, input_file_name, lit, udf}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Seq

object FileIngestionFramework {
    def main(args: Array[String]): Unit = {
        val loggerName = try {
            DBUtils.widgets.get("loggerName")
        } catch {
            case e: Throwable => "RawLogger"
        }
        val logSystem = try {
            DBUtils.widgets.get("logSystem")
        } catch {
            case e: Throwable => "App"
        }
        val logLevel = try {
            DBUtils.widgets.get("logLevel")
        } catch {
            case e: Throwable => "INFO"
        }
        val logAppender = try {
            DBUtils.widgets.get("logAppender")
        } catch {
            case e: Throwable => "RawFile"
        }
        val isRootLogEnabled = try {
            DBUtils.widgets.get("isRootLogEnabled")
        } catch {
            case e: Throwable => "False"
        }
        val logger: Logger = fn_initializeLogger(loggerName, logSystem, logLevel, logAppender, isRootLogEnabled)

        logger.info("Main class arguments: " + args.mkString(", "))
        val contractPath = "/mnt/" + args(0)
        val pipelineName = args(1)
        val pipelineID = args(2)
        val np = new ContractUtils(java.nio.file.Paths.get(contractPath))

        val sourcePath: String = "/mnt/" + np.prop("landing.path")
        val targetPath: String = "/mnt/" + np.prop("raw.path")
        val targetPartitionColumns: Seq[String] = try {
            np.prop("raw.partitionColumns")
        } catch {
            case _: Throwable => Seq[String]("feed_date")
        }
        val processName: String = np.prop("raw.processName")
        val sourceEntity: String = np.prop("landing.entity")
        val logTable: String = np.prop("log.logTable")
        val pathParts: Int = np.prop("source.pathParts")
        val fileFormat: String = np.propOrElse[String]("landing.fileFormat", "")
        val fileFormatOptions: Map[String, String] = np.propOrElse[Map[String, String]]("landing.fileFormatOptions", Map[String, String]())
        val transformationName: String = np.propOrElse[String]("raw.transformationName", "identity")
        val schemaPath: String = np.propOrElse[String]("raw.schemaPath", "")
        // We need to add Crypto Functionality for our PII, this needs to be defined val cryptoHash: Option[Map[String, Any]] = np.propOrNone[Map[String, Any]]("crypto")
        val saveModeFromContract: String = np.propOrElse[String]("raw.saveMode", "append")
        val configOptions: Map[String, Any] = np.propOrElse[Map[String, Any]]("raw.configOptions", Map[String, Any]())
        val writeOptions: Map[String, String] = np.propOrElse[Map[String, String]]("raw.writeOptions", Map[String, String]())
        val writeOptionsFunctionName: String = np.propOrElse[String]("raw.writeOptionsFunction", "")
        val writeOptionsFunctionParams: Map[String, String] = np.propOrElse[Map[String, String]]("raw.writeOptionsFunctionParams", Map[String, String]())

        val spark: SparkSession = getSparkSession()
        var runID: Int = 1
        var dpYear = try {
            DBUtils.widgets.get("dpYear")
        } catch {
            case e: Throwable => java.time.LocalDateTime.now.getYear.toString
        }
        var dpMonth = try {
            DBUtils.widgets.get("dpMonth")
        } catch {
            case e: Throwable => java.time.LocalDateTime.now.getMonthValue.toString
        }
        var dpDay = try {
            DBUtils.widgets.get("dpDay")
        } catch {
            case e: Throwable => java.time.LocalDateTime.now.getDayOfMonth.toString
        }
        var dpHour = try {
            DBUtils.widgets.get("dpHour")
        } catch {
            case e: Throwable => java.time.LocalDateTime.now.getHour.toString
        }
        val notebookStartTime = java.time.LocalDateTime.now.toString

        // COMMAND ----------

        val schema: Option[StructType] = if (schemaPath.nonEmpty) Some(DataType.fromJson(DBUtils.fs.head(s"/mnt/$schemaPath")).asInstanceOf[StructType]) else None

        // COMMAND ----------

        def processFilesBasedOnFileFormat(files: Seq[String], format: String, options: Map[String, String], schema: Option[StructType]): (Seq[String], DataFrame) = {
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

        // COMMAND ----------

        def getWriteOptions(df: DataFrame, functionName: String, functionParams: Map[String, String], options: Map[String, String]): Map[String, String] = {
            if (functionName.isEmpty) {
                options
            } else {
                writeOptionsFunctionsMap(functionName)(df, functionParams)
            }
        }

        // COMMAND ----------

        applySparkConfigOptions(spark, configOptions)

        // COMMAND ----------

        var landingDF: DataFrame = null
        var filesProcessed: Seq[String] = null
        try {
            val allFiles = getAllFiles(spark.sparkContext.hadoopConfiguration, sourcePath)
            val processedResult = if (fileFormat.isEmpty) {
                throw new RuntimeException(s"The fileFormat parameter is empty.")
            } else {
                processFilesBasedOnFileFormat(allFiles, fileFormat, fileFormatOptions, schema)
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

        // COMMAND ----------

        try {
            val pathFileName = udf((fileName: String, pathParts: Int) => fileName.split("/").takeRight(pathParts).mkString("/"))

            val dateNow = java.time.LocalDate.now.toString
            val finalDF = landingDF
                .withColumn("feed_date", lit(dateNow))
                .withColumn("file_name", pathFileName(input_file_name, lit(pathParts)))
                .withColumn("cxi_id", expr("uuid()"))

            val saveMode = if (DeltaTable.isDeltaTable(spark, targetPath)) saveModeFromContract else "errorifexists"
            val transformationFunction = transformationFunctionsMap(transformationName)
            val transformedDf = transformationFunction(finalDF)
            //No Crypto at the moment, need to define val cryptoHashedDf = applyCryptoHashIfNeeded(cryptoHash, transformedDf)

            transformedDf
                .write
                .format("delta")
                .partitionBy(targetPartitionColumns: _*)
                .mode(saveMode)
                .options(getWriteOptions(transformedDf, writeOptionsFunctionName, writeOptionsFunctionParams, writeOptions))
                .save(targetPath)

        }
        catch {
            case e: Throwable =>
                // If run failed write Audit log table to indicate data processing failure
                fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "0", logger = logger,
                    dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour,
                    processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString, errorMessage = e.toString, spark = spark)
                logger.error(s"Failed to write to delta location $targetPath. Due to error: ${e.toString}")
                throw e
        }

        // COMMAND ----------

        val files = filesProcessed.size
        fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "1", logger = logger,
            dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour,
            processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString, errorMessage = "", spark = spark)
        logger.info(s"""Files processed: $files""")
    }
}
