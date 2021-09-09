package com.cxi.cdp.data_processing
package support.packages.regulation

import support.packages.lake.LookupTableLake

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, _}
// Databricks notebook source
// MAGIC %run "./hashing_properties"

// COMMAND ----------

// MAGIC %run "../lake/lookup_table_lake_class"

// COMMAND ----------

class CommonHashingFunction (val kvKeyScope: String, val kvKeyOfSalt: String) extends IHashFunction {

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

        CommonOptionInput(
            hashInput("name").asInstanceOf[String],
            hashInput("dataColName").asInstanceOf[String]
        )
    }


    //Transform processing attributes
    override protected def transform(in: IInputValue): IInputValue = {
        in match {
            case x: CommonInputValue => CommonInputValue(x.pii.trim)
            case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
        }
    }

    //Validate processing attributes
    override protected def validate(in: IInputValue): Boolean = {

        def check(v: String): Boolean = {
            if (!v.isEmpty) {
                return true
            } else return false
        }
        in match {
            case x: CommonInputValue => check(x.pii)
            case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
        }
    }

    //Hash processing attributes
    override protected def valueHash(in: IInputValue): IOutputValue = {
        in match {
            case x: CommonInputValue => {
                try {
                    val value = transform(in)
                    if (validate(value)) {
                        val pii = value match {
                            case x: CommonInputValue => x.pii
                            case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
                        }
                        OutputValueHashFunction(true, pii, Hash.sha256Hash(pii, salt))
                        //OutputValueHashFunction(true, pii, pii)
                    }
                    else failValueOfReturn(value, "The validation failed")
                }
                catch {
                    case e: java.util.NoSuchElementException => {
                        failValueOfReturn(in, "Exception NoSuchElementException happened. Did not find the expected key")
                    }
                    case e: Throwable => {
                        failValueOfReturn(in, s"Exception ${e.getClass.getName} happened. ${e.toString}")
                    }
                }
            }
            case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
        }
    }

    //Restructure failure on processing attributes
    override protected def failValueOfReturn(in: IInputValue, message: String): IOutputValue = {
        in match {
            case x : CommonInputValue => OutputValueHashFunction(false, x.pii, "From Fail Method " + message)
            case _ => throw new IllegalArgumentException(s"Unknown type of input value '${in.getClass.getName}'.")
        }
    }

    //Public function to hash dataframe, turns rows into processing attributes
    override def hash(hashProp: IHashProperties, srcDf: DataFrame): (DataFrame, DataFrame) = {
        val hashPropClass = hashProp.asInstanceOf[CommonOptionInput]
        val dataColName = hashPropClass.dataColName
        val dataCol = srcDf(dataColName)
        def hashFunction() = udf((value: String) => {
            valueHash(CommonInputValue(value)) match {
                case x: OutputValueHashFunction => x.hash
                case _ => throw new RuntimeException(s"Unknown type")
            }
        })

        val tgtDf = srcDf.withColumn("hashof_cxi_customer_id", hashFunction()(col(dataColName)))
        //TODO: Need to remove hardcoded value once cxi_partner_id and country are defined.
        val lookupDf = tgtDf.select(col("hashof_cxi_customer_id"), col(dataColName).as("cxi_customer_id"))
            .withColumn("process_name", lit("common-crypto-hash"))
            .withColumn("country", lit("USA"))
            .withColumn("cxi_partner_id", lit("USA-123-123"))
            .dropDuplicates

        //TODO: Define dataframe to write back into lookup table
        (tgtDf, lookupDf)
    }

    //TODO: Betterway to define account key
    override def writeLookup(lookupDf: DataFrame): Unit  = {
        LookupTableLake.upsert(lookupDf)
    }
}

// COMMAND ----------
