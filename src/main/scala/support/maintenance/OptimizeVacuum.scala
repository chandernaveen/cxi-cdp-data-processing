package com.cxi.cdp.data_processing
package support.maintenance

import support.functions.UnifiedFrameworkFunctions.fn_initializeLogger

import com.databricks.service.DBUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object OptimizeVacuum {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        run(spark)
    }
    def run(spark: SparkSession): Unit = {
        // Databricks notebook source
        // DBTITLE 1,Getting common functions
        // MAGIC %run "../functions/unified_framework_functions"

        // COMMAND ----------

        // DBTITLE 1,Application Logging
        // Logger Configuration
        val loggerName       = try { DBUtils.widgets.get("loggerName") }       catch { case e: Throwable => "CuratedLogger" }
        val logSystem        = try { DBUtils.widgets.get("logSystem") }        catch { case e: Throwable => "App" }
        val logLevel         = try { DBUtils.widgets.get("logLevel") }         catch { case e: Throwable => "INFO" }
        val logAppender      = try { DBUtils.widgets.get("logAppender") }      catch { case e: Throwable => "CuratedFile" }
        val isRootLogEnabled = try { DBUtils.widgets.get("isRootLogEnabled") } catch { case e: Throwable => "False" }
        val logger:Logger    = fn_initializeLogger(loggerName,logSystem,logLevel,logAppender,isRootLogEnabled)

        // COMMAND ----------

        /*DBUtils.widgets.text("deltaPath","raw_zone/cases/","")
        DBUtils.widgets.text("optimizePartExpr","","")
        DBUtils.widgets.text("vacuumRetainHours","30","")*/

        // COMMAND ----------

        // DBTITLE 1,Getting widgets
        val dataPath="/mnt/"+DBUtils.widgets.get("deltaPath")
        val optimizePartExpr=DBUtils.widgets.get("optimizePartExpr")
        val vacuumRetainHours=DBUtils.widgets.get("vacuumRetainHours")
        var exitValue="1"

        // COMMAND ----------

        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        // COMMAND ----------

        // DBTITLE 1,Making partition expression based on yesterday date and input optimizePartExpr parameter
        import java.text.SimpleDateFormat
        import java.util.Calendar

        val cal = Calendar.getInstance
        val dateTime = cal.getTime
        cal.setTime(dateTime);
        cal.add(Calendar.DATE, -1);
        val processDate=cal.getTime()

        //val optimizePartExpr: String = "record=10/year=yyyy/month=MM/day=dd"
        //val optimizePartExpr: String = "date=yyyy-MM-dd"
        var optimizeFilter = ""
        if(optimizePartExpr != ""){

            val prtList= optimizePartExpr.split("/")
            var dateFormat=null
            var optimizePathExt :List[String] =List()
            for(a <- prtList)
            {
                try{
                    val prtName=a.split("=")(0)
                    val prtValExp=a.split("=")(1)
                    var dateFormat = new SimpleDateFormat(a.split("=")(1))
                    val prtVal = dateFormat.format(processDate)
                    optimizePathExt=optimizePathExt ::: List(prtName+"='"+prtVal+"'")
                    //val ab =1/0
                }catch {
                    case ex: java.lang.IllegalArgumentException =>{
                        optimizePathExt=optimizePathExt ::: List(a.split("=")(0)+"='"+a.split("=")(1)+"'")}
                    case ex : Exception =>
                        println(ex)
                }
            }
            optimizeFilter=s" WHERE " + optimizePathExt.mkString(" and ")
        }else{
            optimizeFilter = ""
        }


        // COMMAND ----------

        // DBTITLE 1,Executing optimize and vacuum
        try {
            val optimizeQuery = s"OPTIMIZE delta.`$dataPath` $optimizeFilter"
            println(java.time.LocalDateTime.now.toString() + s": Beginning Optimize Query - $optimizeQuery")
            spark.sql(optimizeQuery)
            val vacuumQuery = s"VACUUM delta.`$dataPath` RETAIN $vacuumRetainHours HOURS"
            println(java.time.LocalDateTime.now.toString() + s": Beginning Vacuum Query - $vacuumRetainHours")
            spark.sql(vacuumQuery)
        }
        catch {
            case e: Throwable =>
                exitValue = s"optimize/vacuum process is failed for $dataPath delta path due to ${e.toString}"
                logger.error(s"optimize/vacuum process is failed for $dataPath delta path due to ${e.toString}")
                //DBUtils.notebook.exit(exitValue)
                throw e
        }

        // COMMAND ----------

        logger.info(s"optimize/vacuum process is Successful for $dataPath delta path")
//        DBUtils.notebook.exit(exitValue)
    }
}
