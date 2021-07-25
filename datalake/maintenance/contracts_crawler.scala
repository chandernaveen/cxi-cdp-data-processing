// Databricks notebook source
//dbutils.widgets.removeAll()

// COMMAND ----------

// DBTITLE 1,Passing widgets
/*dbutils.widgets.text("contractsPath","metadata/","")
dbutils.widgets.text("isTesting","true","")
dbutils.widgets.text("testingReadRootPath","work_area/user@email.com","")*/

// COMMAND ----------

// DBTITLE 1,Getting widgets
val contractsPath=dbutils.widgets.get("contractsPath")
val isTesting=dbutils.widgets.get("isTesting")
val testingReadRootPath=dbutils.widgets.get("testingReadRootPath")

// COMMAND ----------

// DBTITLE 1,Setting user/root Path
val rootFolders = List[String]("datalake")  
var readRootPath  : String = "/mnt/"
val notebookPath  = dbutils.notebook.getContext.toMap.get("extraContext").get.asInstanceOf[Map[String,String]]("notebook_path").drop(1)
val notebookRoot  = notebookPath.take(notebookPath.indexOf("/"))

if(rootFolders.contains(notebookRoot)) {} else
{ 
  val currentUser = dbutils.notebook.getContext.toMap.get("tags").get.asInstanceOf[Map[String,String]]("user")
  readRootPath = s"/mnt/work_area/$currentUser/" 
}

val isTesting:Boolean = try { dbutils.widgets.get("isTesting").toBoolean } catch { case e: Throwable => false }
if(isTesting)
{ 
  val testingReadRootPath=try { dbutils.widgets.get("testingReadRootPath") } catch { case e: Throwable => "" }
  if(testingReadRootPath!="")
  {
    readRootPath = "/mnt/" + testingReadRootPath + "/"   
  }
  else{
    val exitValue = "Process exit with error :: testingReadRootPath is null " 
    dbutils.notebook.exit(exitValue)
  }
}

val mntContractsPath= readRootPath+contractsPath
val exitValue : List[String] = List()


// COMMAND ----------

// DBTITLE 1,Crawler Script
import com.databricks.backend.daemon.dbutils.FileInfo
 def contractCrawler(location: String): List[String] = {
   def go(items: List[FileInfo], results: List[String]): List[String] = items match {
     case head :: tail =>
        val files = dbutils.fs.ls(head.path)
        val directories = files.filter(_.isDir)
        val updated:List[String] = results ::: files.filter(_.isFile).filter{file => file.name.contains("contract.json")}.map(_.path).toList
        go(tail ++ directories, updated)
     case _ => results
   }
   go(dbutils.fs.ls(location).toList, List())
 }

// COMMAND ----------

// DBTITLE 1,Making list as string and Removing mnt
val df_contractList = contractCrawler(mntContractsPath)
val ContractString = df_contractList.mkString(",").replace("dbfs:/mnt/", "")

// COMMAND ----------

// DBTITLE 1,Passing output in exit
dbutils.notebook.exit(ContractString)
