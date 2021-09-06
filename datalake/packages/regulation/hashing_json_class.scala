// Databricks notebook source
// MAGIC %run ./hashing_properties

// COMMAND ----------

// MAGIC %run "../lake/lookup_table_lake_class"

// COMMAND ----------

package com.cxi.regulation.classes

import com.cxi.regulation.{IHashFunction, IHashProperties, IInputValue, IOutputValue, JsonInputValue, JsonOptionInput, JsonOutputValueHashFunction}
import org.apache.spark.sql.DataFrame
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.functions._
import com.cxi.lake._

//TODO: Has not been tested yet
class JsonHashingFunction (val kvKeyScope: String, val kvKeyOfSalt: String) extends IHashFunction {
  import scala.util.matching.Regex
  import com.cxi.regulation.Hash
  
  //Obtain the SALT value from workspace scope
  val salt: String = getSalt()
  private var outputContext: JsonOutputValueHashFunction = null;
  
  private def getSalt(): String = {
    Hash.getSecret(kvKeyScope, kvKeyOfSalt) match {
      case Some(x) => x
      case None => throw new RuntimeException(s"Secret does not exist with scope: ${kvKeyScope} and key: ${kvKeyOfSalt}")
    }    
  }
  
  //Hash processing attributes
  //Parse contract values
  override def transformInput(hashInput: Map[String, Any]): IHashProperties = {
    JsonOptionInput(
        hashInput("name").asInstanceOf[String],
        hashInput("controlColName").asInstanceOf[String],
        hashInput("dataColName").asInstanceOf[String]
    )
  }


  //Transform processing attributes
  override protected def transform(in: IInputValue): IInputValue = {
    in match {
        case x: JsonInputValue => JsonInputValue(x.jsonCol, x.jsonValueName.trim)
        case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
    }
  }
  

 
  //Validate processing attributes
  override protected def validate(in: IInputValue): Boolean = {
    return false
  }
  
  override protected def valueHash(in: IInputValue): IOutputValue = {
     in match {
        case x: JsonInputValue => {
            try {
                val input = x match {
                  case y: JsonInputValue => y
                  case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
                }
                val jsonValue = input.jsonCol.get(input.jsonValueName) match {
                  case z: Option[String] => z.get.trim
                  case _ =>  ""
                }
                val jsonHashValue = Hash.sha256Hash(jsonValue)
                //TODO: val jsonHashValue = Hash.sha256Hash(jsonValue, salt)
                //TODO: val jsonHashMap = input.jsonCol.map(e => if (e._1 == input.jsonValueName) (e._1 -> Hash.sha256Hash(e._2.trim, salt)) else (e._1 -> e._2)) 
                val jsonHashMap = input.jsonCol.map(e => if (e._1 == input.jsonValueName) (e._1 -> Hash.sha256Hash(e._2.trim)) else (e._1 -> e._2))
                JsonOutputValueHashFunction(true, jsonValue, jsonHashValue, jsonHashMap) 
            }
            catch {
                case e: java.util.NoSuchElementException => {
                    JsonOutputValueHashFunction(false, "", "", null) 
                }
                case e: Throwable => {
                    JsonOutputValueHashFunction(false, "", "", null) 
                }
            }
        }
        case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
     }
  }

    //Restructure failure on processing attributes
  override protected def failValueOfReturn(in: IInputValue, message: String): IOutputValue = {
    in match {
        case x : JsonInputValue => JsonOutputValueHashFunction(false, x.jsonValueName, "From Fail Method " + message, null)
        case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
    }
  }




  override def hash(hashProp: IHashProperties, srcDf: DataFrame): (DataFrame, DataFrame) = {
    val hashPropClass = hashProp.asInstanceOf[JsonOptionInput]
    val jsonColName = hashPropClass.jsonColName
    val jsonValueName = hashPropClass.jsonValueName
    //val jsonCol = srcDf(jsonColName)
    def hashFunction() = udf((jsonCol: Map[String,String], jsonValueName: String) => {
        valueHash(JsonInputValue(jsonCol, jsonValueName)) match {
            case x: JsonOutputValueHashFunction => x.jsonHashMap
            case _ => throw new RuntimeException(s"Unknown type")
        } 
    })

  
    val tgtPrepDf = srcDf.withColumn("jsonValueName", lit(jsonValueName)).withColumn("jsonMapped", from_json(col("record_value"),MapType(StringType,StringType)))
    val tgtDf = tgtPrepDf.withColumn("jsonHashed", hashFunction()(col("jsonMapped"), col("jsonValueName")))
    val lookupDf = tgtDf.select("jsonMapped", "jsonHashed")
      .withColumn("cxi_customer_id", col("jsonMapped."+jsonValueName))
      .withColumn("hashof_cxi_customer_id", col("jsonHashed."+jsonValueName))
      .withColumn("process_name", lit("json-crypto-hash"))
      .withColumn("country", lit("USA"))
      .withColumn("cxi_partner_id", lit("USA-123-123"))
      .drop("jsonMapped","jsonHashed")
      .dropDuplicates
    (tgtDf, lookupDf)
  }
  
  def writeLookup(lookupDf: DataFrame): Unit = {
    LookupTableLake.upsert(lookupDf)
  }
  
}


