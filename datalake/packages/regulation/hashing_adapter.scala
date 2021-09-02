// Databricks notebook source
// MAGIC %run ./hashing_class

// COMMAND ----------

// MAGIC %run ./hashing_json_class

// COMMAND ----------

package com.cxi.regulation.classes.adapter

import com.cxi.regulation._
import com.cxi.regulation.classes._

/**
 * The HashFunctionFactory will allow us to call multiple different classes using the same Object
 */
object HashFunctionFactory {

  def getFunction(entityType: String, kvKeyScope: String, kvKeyOfSalt: String): IHashFunction = {
    entityType match {
      case "common" => new CommonHashingFunction(kvKeyScope, kvKeyOfSalt)
      case "json" => new JSONHashingFunction(kvKeyScope, kvKeyOfSalt)
      case _ => throw new IllegalArgumentException(s"Unknown process '${entityType}'.")
    }
  }
}

// COMMAND ----------

package com.cxi.regulation.classes.adapter

import com.cxi.regulation.{IHashProperties, IHashFunction}
import org.apache.spark.sql.DataFrame

/**
 * The IngestionHashAdapter will work as a single point of entry to activate the Factory object above
 */
object IngestionHashAdapter {

  def hashDf(hashSpecs: Map[String, Any], kvScope: String, df: DataFrame, writeLookup: Boolean): DataFrame = {
    val functionType: String = hashSpecs("type").asInstanceOf[String]
    val functionSecret: String = hashSpecs("kvKeyOfSalt").asInstanceOf[String]
    val function: IHashFunction = HashFunctionFactory.getFunction(functionType, kvScope, functionSecret)
    val transformedInput: IHashProperties = function.transformInput(hashSpecs)

    val dataFrames = function.hash(transformedInput, df)
    if (writeLookup) {
      function.writeLookup(dataFrames._2)
    }
    dataFrames._1
  }

}

// COMMAND ----------

/*

Common - Single DF Value
"crypto": {
		"name": "<name>",
		"type": "<type-of-hash>",
		"processName": "<process-name>",
		"kvKeyOfSalt": "<name-of-secret>",
		"dataColName": "<df-column-to-hash>",
		"options": [
		]
	}
    
JSON - Single JSON Value, complext structure
"crypto": {
		"name": "<name>",
		"processName": "<process-name>",
		"kvKeyOfSalt": "<name-of-secret>",
		"controlColName": "<df-column-with-JSON-to-hash>",
		"dataColName": "<df-column-in-JSON-to-hash>",
		"options": [
		]
	}
    
Delimited - Single String Value, complext structure
"crypto": {
		"name": "<name>",
		"processName": "<process-name>",
		"kvKeyOfSalt": "<name-of-secret>",
		"controlColName": "<df-column-with-JSON-to-hash>",
		"dataColName": "<df-column-in-JSON-to-hash>",
		"options": [
			{
				"controlValues": [
					"<attribute-in-df-column-to-hash>"
				],
				"delimiter": "|",
				"valueToHashColPosition": 7
			}
		]
	}
    
*/
