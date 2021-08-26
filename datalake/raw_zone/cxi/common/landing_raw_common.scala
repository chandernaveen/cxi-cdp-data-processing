// Databricks notebook source
// MAGIC %run "/datalake/functions/unified_framework_functions"

// COMMAND ----------

// MAGIC %run "datalake/packages/utils/contract_utils"

// COMMAND ----------

// MAGIC %run "./landing_raw_transformations"

// COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("pipelineID","123","")
dbutils.widgets.text("pipelineName","Test","")
dbutils.widgets.text("contractPath","metadata/template/contracts/landing_raw_contract.json","")
dbutils.widgets.text("pipelineID","123","")
dbutils.widgets.text("pipelineName","Test","")
dbutils.widgets.text("contractPath","metadata/template/contracts/landing_raw_contract.json","")

// COMMAND ----------

val pipelineID   = dbutils.widgets.get("pipelineID")
val pipelineName = dbutils.widgets.get("pipelineName")
val contractPath = "/mnt/" + dbutils.widgets.get("contractPath")

// COMMAND ----------

import com.cxi.utils.ContractUtils
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.DataFrame
import scala.collection.Seq
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.lit
import io.delta.tables._

// COMMAND ----------

val np = new ContractUtils(java.nio.file.Paths.get(contractPath))
val loggerName       = try { dbutils.widgets.get("loggerName") }       catch { case e: Throwable => "RawLogger" }
val logSystem        = try { dbutils.widgets.get("logSystem") }        catch { case e: Throwable => "App" }
val logLevel         = try { dbutils.widgets.get("logLevel") }         catch { case e: Throwable => "INFO" }
val logAppender      = try { dbutils.widgets.get("logAppender") }      catch { case e: Throwable => "RawFile" }
val isRootLogEnabled = try { dbutils.widgets.get("isRootLogEnabled") } catch { case e: Throwable => "False" }
val logger:Logger    = fn_initializeLogger(loggerName, logSystem, logLevel, logAppender, isRootLogEnabled)

val sourcePath:String = "/mnt/" + np.prop("landing.path")
val targetPath:String = "/mnt/" + np.prop("raw.path")
val targetPartitionColumns:Seq[String] = try { np.prop("raw.partitionColumns") } catch { case _ : Throwable => Seq[String]("feed_date") }
val processName:String = np.prop("raw.processName")
val sourceEntity:String = np.prop("landing.entity")
val logTable:String = np.prop("log.logTable")
val pathParts:Int = np.prop("source.pathParts")
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

var runID : Int = 1
var dpYear    = try { dbutils.widgets.get("dpYear") }  catch { case e: Throwable => java.time.LocalDateTime.now.getYear().toString() }
var dpMonth   = try { dbutils.widgets.get("dpMonth") } catch { case e: Throwable => java.time.LocalDateTime.now.getMonthValue().toString() }
var dpDay     = try { dbutils.widgets.get("dpDay") }   catch { case e: Throwable => java.time.LocalDateTime.now.getDayOfMonth().toString() }
var dpHour    = try { dbutils.widgets.get("dpHour")  } catch { case e: Throwable => java.time.LocalDateTime.now.getHour().toString() }
val notebookStartTime  = java.time.LocalDateTime.now.toString() 

// COMMAND ----------

val schema: Option[StructType] = if (schemaPath.nonEmpty) Some(DataType.fromJson(dbutils.fs.head(s"/mnt/$schemaPath")).asInstanceOf[StructType]) else None

// COMMAND ----------

def processFilesBasedOnFileFormat(files: Seq[String], format: String, options: Map[String, String], schema: Option[StructType]): (Seq[String], DataFrame) = {
  if (files.nonEmpty) {
    val reader = spark.read
                      .format(format)
                      .options(options)
    schema.foreach(reader.schema(_))
    (files, reader.load(files: _*))
  } else {
    throw new RuntimeException(s"The folder ${sourcePath} does not contain data files we are looking for.")
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

applySparkConfigOptions(configOptions)

// COMMAND ----------

var landingDF: DataFrame = _
var filesProcessed: Seq[String] = _
try {
  val allFiles = getAllFiles(sourcePath)
  val processedResult = if (fileFormat.isEmpty) {
    //processFilesBasedOnFileExtension(allFiles
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
      processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString(), errorMessage = e.toString())
    logger.error(s"Failed to new files from ${sourcePath}. Due to error: ${e.toString()}")
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
      processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString(), errorMessage = e.toString())
    logger.error(s"Failed to write to delta location ${targetPath}. Due to error: ${e.toString()}")
    throw e
}

// COMMAND ----------

val files = filesProcessed.size
fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "1", logger = logger,
  dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour,
  processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString(), errorMessage = "")
dbutils.notebook.exit(s"""Files processed: ${files}""")
