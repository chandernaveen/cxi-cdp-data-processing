package com.cxi.cdp.data_processing
package support.maintenance

import com.databricks.service.DBUtils
import org.apache.hadoop.fs.FileStatus

object ContractsCrawler {
    def run(): Unit = {
        // Databricks notebook source
        //DBUtils.widgets.removeAll()

        // COMMAND ----------

        // DBTITLE 1,Passing widgets
        /*DBUtils.widgets.text("contractsPath","metadata/","")
        DBUtils.widgets.text("isTesting","true","")
        DBUtils.widgets.text("testingReadRootPath","work_area/user@email.com","")*/

        // COMMAND ----------

        // DBTITLE 1,Getting widgets
        val contractsPath=DBUtils.widgets.get("contractsPath")
        val isTesting:Boolean = try { DBUtils.widgets.get("isTesting").toBoolean } catch { case e: Throwable => false }
        val testingReadRootPath=DBUtils.widgets.get("testingReadRootPath")

        // COMMAND ----------

        // DBTITLE 1,Setting user/root Path
        val rootFolders = List[String]("datalake")
        var readRootPath  : String = "/mnt/"
        val notebookPath  = DBUtils.notebook.getContext.asInstanceOf[Map[String, Any]]("extraContext").asInstanceOf[Map[String,String]]("notebook_path").drop(1)
        val notebookRoot  = notebookPath.take(notebookPath.indexOf("/"))

        if(rootFolders.contains(notebookRoot)) {} else
        {
            val currentUser = DBUtils.notebook.getContext.asInstanceOf[Map[String, Any]]("tags").asInstanceOf[Map[String,String]]("user")
            readRootPath = s"/mnt/work_area/$currentUser/"
        }

        if(isTesting)
        {
            val testingReadRootPath=try { DBUtils.widgets.get("testingReadRootPath") } catch { case e: Throwable => "" }
            if(testingReadRootPath!="")
            {
                readRootPath = "/mnt/" + testingReadRootPath + "/"
            }
            else{
                val exitValue = "Process exit with error :: testingReadRootPath is null "
                DBUtils.notebook.exit(exitValue)
            }
        }

        val mntContractsPath= readRootPath+contractsPath
        val exitValue : List[String] = List()


        // COMMAND ----------

        // DBTITLE 1,Crawler Script
        def contractCrawler(location: String): List[String] = {
            def go(items: List[FileStatus], results: List[String]): List[String] = items match {
                case head :: tail =>
                    val files = DBUtils.fs.ls(head.getPath.toString)
                    val directories = files.map(f => DBUtils.fs.dbfs.getFileStatus(new org.apache.hadoop.fs.Path(f.path))).filter(f => f.isDirectory)
                    val updated:List[String] = results ::: files.map(f => DBUtils.fs.dbfs.getFileStatus(new org.apache.hadoop.fs.Path(f.path))).filter(_.isFile).filter{file => file.getPath.getName.contains("contract.json")}.map(_.getPath.toString).toList
                    go(tail ++ directories, updated)
                case _ => results
            }
            go(DBUtils.fs.ls(location).map(f => DBUtils.fs.dbfs.getFileStatus(new org.apache.hadoop.fs.Path(f.path))).toList, List())
        }

        // COMMAND ----------

        // DBTITLE 1,Making list as string and Removing mnt
        val df_contractList = contractCrawler(mntContractsPath)
        val ContractString = df_contractList.mkString(",").replace("dbfs:/mnt/", "")

        // COMMAND ----------

        // DBTITLE 1,Passing output in exit
        DBUtils.notebook.exit(ContractString)
    }
}
