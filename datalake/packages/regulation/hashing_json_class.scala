// Databricks notebook source
// MAGIC %run ./hashing_properties

// COMMAND ----------

package com.cxi.regulation.classes

import com.cxi.regulation.{IHashFunction, IHashProperties, IInputValue, IOutputValue, JSONInputValue, JSONOptionInput, OutputValueHashFunction}
import org.apache.spark.sql.DataFrame
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._


class JSONHashingFunction (val kvKeyScope: String, val kvKeyOfSalt: String) extends IHashFunction {
  import scala.util.matching.Regex
  import com.cxi.regulation.Hash
  
  //Obtain the SALT value from workspace scope
  val salt: String = getSalt()
  
  private def getSalt(): String = {
    Hash.getSecret(kvKeyScope, kvKeyOfSalt) match {
      case Some(x) => x
      case None => throw new RuntimeException(s"Secret does not exist with scope: ${kvKeyScope} and key: ${kvKeyOfSalt}")
    }    
  }
  
  //Parse contract values
  override def transformInput(hashInput: Map[String, Any]): IHashProperties = {
    
    JSONOptionInput(
      hashInput("name").asInstanceOf[String],
      hashInput("controlColName").asInstanceOf[String],
      hashInput("dataColName").asInstanceOf[String]
    )
  }
  
 
  //Transform processing attributes
  override protected def transform(in: IInputValue): IInputValue = {
    return null;
  }
  
  //Validate processing attributes
  override protected def validate(in: IInputValue): Boolean = {
    
    def check(v: String): Boolean = {
      if (!v.isEmpty) {
        return true
      } else return false
    }
    in match {
      case x: JSONInputValue => check(x.pii)
      case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
    }
  }
  
  //Hash processing attributes
  override protected def valueHash(in: IInputValue): IOutputValue = {
    return null;
  }
  
  //Restructure failure on processing attributes
  override protected def failValueOfReturn(in: IInputValue, message: String): IOutputValue = {
      return null;
  }
  
  override def hash(hashProp: IHashProperties, srcDf: DataFrame): (DataFrame, DataFrame) = {
    (null, null)
  }
  def writeLookup(lookupDf: DataFrame): Unit = {
    //To-Be-Define.upsert(lookupDf)
    null
  }
  
}


