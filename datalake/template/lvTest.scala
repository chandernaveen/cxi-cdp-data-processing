// Databricks notebook source
// MAGIC %run "/datalake/functions/unified_framework_functions"

// COMMAND ----------

val dfTest = spark.read.format("csv").option("header", true).option("inferSchema","true").load("/databricks-datasets/COVID/coronavirusdataset/Case.csv")

// COMMAND ----------

display(dfTest)

// COMMAND ----------

dfTest.select($"province", $"city").write.format("delta").mode("append").save("/mnt/raw_zone/cases")

// COMMAND ----------

val dfTest2 = spark.read.format("delta").load("/mnt/raw_zone/cases")
println(dfTest2.count())

// COMMAND ----------

var dpYear    = try { dbutils.widgets.get("dpYear") }  catch { case e: Throwable => java.time.LocalDateTime.now.getYear().toString() }
var dpMonth   = try { dbutils.widgets.get("dpMonth") } catch { case e: Throwable => java.time.LocalDateTime.now.getMonth().toString() }
var dpDay     = try { dbutils.widgets.get("dpDay") }   catch { case e: Throwable => java.time.LocalDateTime.now.getDayOfMonth().toString() }
var dpHour    = try { dbutils.widgets.get("dpHour")  } catch { case e: Throwable => java.time.LocalDateTime.now.getHour().toString() }

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from logs.application_audit_logs

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC describe logs.application_audit_logs

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls "/mnt/logs/databricks/adb-322708817339647.7.azuredatabricks.net/?o=322708817339647/"

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from delta.`/mnt/logs/databricks/adb-322708817339647.7.azuredatabricks.net/?o=322708817339647/application_audit_logs/`

// COMMAND ----------

// MAGIC %fs 
// MAGIC 
// MAGIC ls "/mnt/raw_zone/"

// COMMAND ----------

var loggingDB     : String = "logs"
val logTable = s"$loggingDB.application_audit_logs"
val processName = "template"
val sourceEntity = "test"
var dpYear    = try { dbutils.widgets.get("dpYear") }  catch { case e: Throwable => java.time.LocalDateTime.now.getYear().toString() }
var dpMonth   = try { dbutils.widgets.get("dpMonth") } catch { case e: Throwable => java.time.LocalDateTime.now.getMonthValue().toString() }
var dpDay     = try { dbutils.widgets.get("dpDay") }   catch { case e: Throwable => java.time.LocalDateTime.now.getDayOfMonth().toString() }
var dpHour    = try { dbutils.widgets.get("dpHour")  } catch { case e: Throwable => java.time.LocalDateTime.now.getHour().toString() }
val runID = 1
val notebookStartTime = java.time.LocalDateTime.now.toString()

// COMMAND ----------

   fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "1", 
                     dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour, 
                     processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString(), errorMessage="Successfully created delta files")

// COMMAND ----------

// Logger Configuration
var logger : Logger  =  null
try {
  val loggerName       = try { dbutils.widgets.get("loggerName") }       catch { case e: Throwable => "App" }          
  val logSystem        = try { dbutils.widgets.get("logSystem") }        catch { case e: Throwable => "RawLogger" } 
  val logLevel         = try { dbutils.widgets.get("logLevel") }         catch { case e: Throwable => "INFO" }           
  val logAppender      = try { dbutils.widgets.get("logAppender") }      catch { case e: Throwable => "RawFile" } 
  val isRootLogEnabled = try { dbutils.widgets.get("isRootLogEnabled") } catch { case e: Throwable => "False" }  
  
  logger = fn_initializeLogger(loggerName, logSystem, logLevel, logAppender, isRootLogEnabled)
}
catch {
  case e: Throwable =>
  println(e)
}

// COMMAND ----------

logger.error(s"Hello this is a fyou message") 

// COMMAND ----------

logger.info(s"Hello this is a fyou message")

// COMMAND ----------

logger.debug("Hello this is a debug message");

// COMMAND ----------

// MAGIC %sh 
// MAGIC 
// MAGIC ls logs/

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC grep "Hello this is a fyou" logs/stdout.log4j-refined-active.log

// COMMAND ----------

// MAGIC %fs 
// MAGIC 
// MAGIC ls "/mnt/logs/0720-222356-bakes163/init_scripts/0720-222356-bakes163_25_0_240_5/" 

// COMMAND ----------

// MAGIC %fs 
// MAGIC 
// MAGIC head "dbfs:/mnt/logs/0720-222356-bakes163/init_scripts/0720-222356-bakes163_25_0_240_5/20210720_222647_00_log4j-refined-config.sh.stderr.log" 

// COMMAND ----------

8897224607710586934/eventlog/
