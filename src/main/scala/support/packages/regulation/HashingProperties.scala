package com.cxi.cdp.data_processing
package support.packages.regulation

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.DataFrame


/**
 * The IHashProperties will represent the property section of the Contract
 */
trait IHashProperties extends Serializable

/**
 * The IInput/Output values will represent the basic values the class will operate with, they will be extended to fit specific class needs
 */
trait IInputValue extends Serializable
trait IOutputValue extends Serializable


/**
 * The IHashFunction will be the base of the different classes, we will build classes using this functions depending on our needs
 */
trait IHashFunction extends Serializable {
  def transformInput(hashInput: Map[String, Any]): IHashProperties                 //Parses the contract for the Properties the class will need
  protected def transform(in: IInputValue): IInputValue                            //Performs any transformation steps needed to prep the hashing process
  protected def validate(in: IInputValue): Boolean                                 //Performs any validations the hashing process needs before starting
  protected def valueHash(in: IInputValue): IOutputValue                           //Function that performs the actual hash
  protected def failValueOfReturn(in: IInputValue, message: String) : IOutputValue //Function to return error in process
  def hash(hashProp: IHashProperties, srcDf: DataFrame): (DataFrame, DataFrame)    //Entry function that the adapter will call to start the hashing process
  def writeLookup(lookupDf: DataFrame): Unit                                       //TODO: We need to write all info back into the PRIVATE container
}

/**
 * The Hash object will perform the actual SHA 256 as well as obtain the secret we will use to SALT the original value
 */
object Hash {

    //Two value signature, will add salt value to ID and generate a SHA 256 Hash
    def sha256Hash(id: String, salt: String): String = {
    var hashValue: String = null
    try {

      val hashValue = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest((id + salt).getBytes("UTF-8"))))
      return hashValue

    }
    catch {
      case e: Throwable =>
        println("Function 'sha256Hash' failed with error:" + e.toString())
        null
    }
  }

  //Single value signature, will call two value signature with blank salt value
  def sha256Hash(id: String) : String = {
    sha256Hash(id, "")
  }

  //Simple logic to pick up SALT from workspace scope using provided values
  def getSecret(kvKeyScope: String, kvKeyOfSalt: String): Option[String] =  {
    try{
      val value = dbutils.secrets.get(kvKeyScope, kvKeyOfSalt)
      Some(value)
    } catch {
      case e: IllegalArgumentException => None
      case _: Throwable => None
    }
  }

}

// COMMAND ----------

//Extension of IInputValue for Common Class, simple 1 value pii
case class CommonInputValue(pii: String) extends IInputValue

// COMMAND ----------

//Extension of IInputValue for JSON Class, TBD
case class JSONInputValue(pii: String) extends IInputValue

// COMMAND ----------

//Extension of IHashProperties will define the contract needs for Common Class
case class CommonOptionInput(name: String, dataColName: String) extends IHashProperties

// COMMAND ----------

//Extension of IHashProperties will define the contract needs for JSON Class
case class JSONOptionInput(name: String, controlColName:String, dataColName: String) extends IHashProperties

// COMMAND ----------

//Extension of IOutputValue will define the output from the Hashing Process, may have more than one for different classes
case class OutputValueHashFunction(isSucceeded: Boolean, pii: String, hash: String) extends IOutputValue

