// Databricks notebook source
// MAGIC %run "/datalake/functions/unified_framework_functions"

// COMMAND ----------

// DBTITLE 1,Set read & write root path
// Application Configuration for write and logging paths during feature development
var runID : Int = 1
val rootFolders = List[String]("datalake")  
var loggingDB     : String = "logs"
val logTable = s"$loggingDB.application_audit_logs"
var readRootPath  : String = "/mnt/"
var writeRootPath : String = "/mnt/"

// Notebook Context utility to get the notebook_path - used to determine if running from main code branch notebook 
val notebookPath  = dbutils.notebook.getContext.toMap.get("extraContext").get.asInstanceOf[Map[String,String]]("notebook_path").drop(1)
// Get root directory of the notebook_path - used for validation againest rootFolders
val notebookRoot  = notebookPath.take(notebookPath.indexOf("/"))
val notebookStartTime = java.time.LocalDateTime.now.toString()
var dpYear    = try { dbutils.widgets.get("dpYear") }  catch { case e: Throwable => java.time.LocalDateTime.now.getYear().toString() }
var dpMonth   = try { dbutils.widgets.get("dpMonth") } catch { case e: Throwable => java.time.LocalDateTime.now.getMonthValue().toString() }
var dpDay     = try { dbutils.widgets.get("dpDay") }   catch { case e: Throwable => java.time.LocalDateTime.now.getDayOfMonth().toString() }
var dpHour    = try { dbutils.widgets.get("dpHour")  } catch { case e: Throwable => java.time.LocalDateTime.now.getHour().toString() }
val processName = "template"
val sourceEntity = "test"


// If you need parameters from Data Factory 
val variableExample:String  = try { dbutils.widgets.get("variableName") } catch { case e: Throwable => "default-value" } 

if(rootFolders.contains(notebookRoot)) {} else
{  
  // if 'notebookRoot' NOT contained in 'rootFolders' -
  // Notebook Context utility to get user running notebook
  val currentUser = dbutils.notebook.getContext.toMap.get("tags").get.asInstanceOf[Map[String,String]]("user")
  // Set new 'writeRootPath' to users work_area
  writeRootPath = s"/mnt/work_area/$currentUser/"  
}
val mntPointRefined      = readRootPath + "refined_zone"
val mntPointCurated      = writeRootPath + "curated_zone"

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

import org.apache.spark.sql.functions._

try
{
    val dfTest = spark.read.format("csv")
                   .option("header","true")
                   .option("inferSchema","true")
                   .load("/databricks-datasets/COVID/coronavirusdataset/Case.csv")
  
    
    dfTest.select($"province", $"city").write.mode(SaveMode.Overwrite)
                          .format("delta")
                          .save("/mnt/raw_zone/cases")
   fn_writeAuditTable(logTable = logTable, processName = "template1", entity = "test", runID = runID, writeStatus = "1", logger = logger,
                     dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour, 
                     processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString(), errorMessage="Successfully created delta files")
    dbutils.notebook.exit("0") 
}

catch
{
  case e: Exception =>             
        val notebookEndTime = java.time.LocalDateTime.now.toString()   
        val exitValue = 1
        fn_writeAuditTable(logTable = logTable, processName = processName, entity = sourceEntity, runID = runID, writeStatus = "0", logger = logger,
               dpYear = dpYear, dpMonth = dpMonth, dpDay = dpDay, dpHour = dpHour, 
               processStartTime = notebookStartTime, processEndTime = java.time.LocalDateTime.now.toString(), errorMessage=e.toString())
        logger.error(s"Failed due to error: ${e.toString()}") 
        dbutils.notebook.exit("NOTEBOOK ERROR: " +e.toString() ) 
}
