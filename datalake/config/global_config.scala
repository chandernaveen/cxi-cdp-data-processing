// Databricks notebook source
import java.time.Instant
import java.time._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

// COMMAND ----------

var exitValue=""
var envType=""
var region=""
var sevicePrincipleClientId=""
var sevicePrincipleTenantId=""
var productName=""
try{
  
  val globalConfigDetails=spark.sparkContext.wholeTextFiles("dbfs:/databricks/config/workspace_details.json").collect.take(1)(0)._2
  val mapper = new ObjectMapper();
  val glConfig :java.util.Map[String, String] = mapper.readValue(globalConfigDetails, classOf[java.util.Map[String, String]]);
  
  envType=glConfig.get("envType")
  region=glConfig.get("region")
  sevicePrincipleClientId=glConfig.get("sevicePrincipleClientId")
  sevicePrincipleTenantId = glConfig.get("sevicePrincipleTenantId")
  productName = glConfig.get("productName")
  /*
  println(glConfig.get("envType"));
  println(glConfig.get("region"));
  println(glConfig.get("sevicePrincipleClientId"));
  println(glConfig.get("sevicePrincipleTenantId"));
  */
  if (envType==null || envType=="" ){
     exitValue = "envType is null" 
  }else if (region==null || region=="" ){
     exitValue = "region is null"
  }else if (sevicePrincipleClientId==null || sevicePrincipleClientId=="" ){
     exitValue = "sevicePrincipleClientId is null"
  }else if (sevicePrincipleTenantId==null || sevicePrincipleTenantId=="" ){
     exitValue = "sevicePrincipleTenantId is null"
  }else if (productName==null || productName=="" ){
     exitValue = "productName is null"
  }
  
if (exitValue != ""){
     dbutils.notebook.exit(exitValue) 
  }
} catch {
    case e: com.fasterxml.jackson.core.JsonParseException =>
      exitValue=e.toString()
      //logger.error(exitValue)
      dbutils.notebook.exit(exitValue)
    case g: com.databricks.workflow.NotebookExit =>
      //logger.error(exitValue)
      dbutils.notebook.exit(exitValue)
    case h: Throwable =>
      exitValue="Error: " + h.toString()
      //logger.error(exitValue)
      dbutils.notebook.exit(exitValue)
  }


// COMMAND ----------

// Vault config
val kvSecretScope = envType+"-"+region+"-keyvault-scope"

//datalake access config
val saName = "dls2"+envType+region+productName
val fsName = "data-lake"
val drNames = Array("raw_zone") //"refined_zone",,"landing_zone","work_area"

// data-service access config
val fsNameDS = "data-service"
val drNameDS = Array("logs","metadata")

val spClientId =sevicePrincipleClientId
val spClientSecret = dbutils.secrets.get(scope = kvSecretScope, key = "sp-storage-rw-secret") 
val spTenantId = sevicePrincipleTenantId
val spTenantEndpoint = "https://login.microsoftonline.com/" + spTenantId + "/oauth2/token"

// COMMAND ----------

//Set required Spark Configurations for counting
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> spClientId,
  "fs.azure.account.oauth2.client.secret" -> spClientSecret,
  "fs.azure.account.oauth2.client.endpoint" -> spTenantEndpoint) 

// COMMAND ----------

// Mount logs folder in data-lake filesystem
for(dirName <- drNames){ 
  try {
    dbutils.fs.mount(
      source = "abfss://" + fsName + "@" + saName + ".dfs.core.windows.net/" + dirName,
      mountPoint = "/mnt/" + dirName,
      extraConfigs = configs)
    println("Mounted Directory: /mnt/" + dirName)
  } catch {
    case e: java.rmi.RemoteException =>
      println(e.toString().drop(82).take(32 + dirName.length()))
    case f: Throwable =>
      println("Error: " + f.toString())
  }
}

// COMMAND ----------

// Mount logs folder in data-service filesystem
for(drName <- drNameDS){ 
  try {
    dbutils.fs.mount(
      source = "abfss://" + fsNameDS + "@" + saName + ".dfs.core.windows.net/" + drName,
      mountPoint = "/mnt/" + drName,
      extraConfigs = configs)
    println("Mounted Directory: /mnt/" + drName)
  } catch {
    case e: java.rmi.RemoteException =>
      println(e.toString().drop(82).take(32 + drName.length()))
    case f: Throwable =>
        println("Error: " + f.toString())
  }
}
