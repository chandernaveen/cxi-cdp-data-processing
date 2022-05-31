package com.cxi.cdp.data_processing
package refined_zone.throtle

import refined_zone.throtle.model.{ThrotleDictionary, TransformedField}
import refined_zone.throtle.service.ThrotleValidator.{
    validateCodeIsPresentInDictionary,
    validateColumnConfigurationIsPresent
}
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, udf}

import java.lang.Boolean.{FALSE, TRUE}
import java.nio.file.Paths
import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object RawRefinedThrotleTidAttJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()

        val contractPath = "/mnt/" + args(0)
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))
        val feedDate = args(1)

        run(spark, contract, feedDate)
    }

    def run(spark: SparkSession, contract: ContractUtils, feedDate: String): Unit = {

        val rawDb = contract.prop[String]("schema.raw.db_name")
        val rawDataTable = contract.prop[String]("schema.raw.data_table")
        val refinedThrotleDb = contract.prop[String]("schema.refined.throtle_db_name")
        val tidAttTableName = contract.prop[String]("schema.refined.tid_att_table")
        val throtleDictionaryTableName = contract.prop[String]("schema.refined.throtle_dictionary_table")

        // columns that are not supposed to be transformed (e.g. throtle_id)
        val doNotTransformColumnNames = contract.prop[Seq[String]]("schema.do_not_transform_columns").toSet
        // columns that contain 1/Yes/null value and converted to Boolean
        val booleanColumnNames = contract.prop[Seq[String]]("schema.boolean_columns").toSet
        // columns that contain numeric value and converted to Integer
        val integerColumnNames = contract.prop[Seq[String]]("schema.integer_columns").toSet

        val refinedDf =
            process(
                s"$rawDb.$rawDataTable",
                s"$refinedThrotleDb.$throtleDictionaryTableName",
                booleanColumnNames,
                doNotTransformColumnNames,
                integerColumnNames,
                feedDate,
                spark
            )

        write(s"$refinedThrotleDb.$tidAttTableName", refinedDf)

    }

    def process(
        rawTable: String,
        dictTable: String,
        booleanColumnNames: Set[String],
        doNotTransformColumnNames: Set[String],
        integerColumnNames: Set[String],
        feedDate: String,
        spark: SparkSession
    ): DataFrame = {

        val rawDf = readRawData(rawTable, feedDate, spark)

        val dictionaryDataset = readThrottleDictionary(dictTable, spark)
        val dictionary = transformThrottleDictionaryToMap(dictionaryDataset)
        val dictionaryBroadcast = spark.sparkContext.broadcast(dictionary)

        val allColumnNames = rawDf.schema.fieldNames.to[ListSet]

        validateColumnConfigurationIsPresent(
            allColumnNames,
            booleanColumnNames,
            doNotTransformColumnNames,
            integerColumnNames,
            dictionaryBroadcast.value.keySet
        )
        val refinedDf = transform(
            rawDf,
            doNotTransformColumnNames,
            booleanColumnNames,
            integerColumnNames,
            dictionaryBroadcast.value,
            allColumnNames,
            spark
        )

        refinedDf
    }

    private[throtle] def transform(
        rawDf: DataFrame,
        doNotTransformColumnNames: Set[String],
        booleanColumnNames: Set[String],
        integerColumnNames: Set[String],
        dictionary: Map[String, Map[String, String]],
        allColumnNames: Set[String],
        spark: SparkSession
    ): DataFrame = {
        val getThrotleCodeValueUdf =
            udf((code_category: String, code: String) => getThrotleTransformedField(code_category, code, dictionary))
        val parseBooleanFieldUdf = udf(parseBooleanField _)
        val castToIntUdf = udf((columnName: String, value: String) => castToInt(columnName, value))

        val transformedWithDictColumnsNames = new ArrayBuffer[String]
        val transformedWithDictColumns = new ArrayBuffer[Column]
        val booleanColumns = new ArrayBuffer[Column]
        val integerColumns = new ArrayBuffer[Column]
        val doNotTransformColumns = new ArrayBuffer[Column]

        for (columnName <- allColumnNames) {
            if (doNotTransformColumnNames.contains(columnName)) {
                doNotTransformColumns += col(columnName)
            } else if (booleanColumnNames.contains(columnName)) {
                booleanColumns += (parseBooleanFieldUdf(col(columnName)) as columnName)
            } else if (integerColumnNames.contains(columnName)) {
                integerColumns += (castToIntUdf(lit(columnName), col(columnName)) as columnName)
            } else {
                transformedWithDictColumns += (getThrotleCodeValueUdf(lit(columnName), col(columnName)) as columnName)
                transformedWithDictColumnsNames += columnName
            }
        }

        val transformedDf =
            rawDf.select(doNotTransformColumns ++ booleanColumns ++ integerColumns ++ transformedWithDictColumns: _*)

        validateCodeIsPresentInDictionary(transformedDf, transformedWithDictColumnsNames, spark)

        val finalColumns = allColumnNames
            .map(colName => {
                if (transformedWithDictColumnsNames.contains(colName)) {
                    col(s"$colName.transformedValue").as(colName)
                } else {
                    col(colName)
                }
            })
            .toSeq

        transformedDf.select(finalColumns: _*)
    }

    private[throtle] def castToInt(columnName: String, value: String): Integer = {
        value match {
            case null => null
            case _ =>
                Try(new Integer(value)) match {
                    case Success(intValue) => intValue
                    case Failure(_) =>
                        throw new IllegalArgumentException(
                            s"Column '$columnName' contains value that cannot be cast to Integer: '$value'"
                        )
                }
        }
    }

    private[throtle] def parseBooleanField(value: String): java.lang.Boolean = {
        val trueValues = Set("true", "y", "yes", "1", "t")
        val falseValues = Set("false", "n", "no", "0", "f")
        val unknownValues = Set("u", "unknown")

        if (value == null || unknownValues.contains(value.toLowerCase())) {
            null
        } else if (trueValues.contains(value.toLowerCase())) {
            TRUE
        } else if (falseValues.contains(value.toLowerCase())) {
            FALSE
        } else {
            throw new IllegalArgumentException(s"Unable to convert to Bollean: '$value'")
        }
    }

    private[throtle] def getThrotleTransformedField(
        code_category: String,
        code: String,
        throtleDict: Map[String, Map[String, String]]
    ): TransformedField = {
        if (code == null) {
            TransformedField(code_category, null, null, isValid = true)
        } else {
            throtleDict.get(code_category) match {
                case Some(codeToValue) =>
                    codeToValue.get(code) match {
                        case Some(value) =>
                            TransformedField(
                                code_category,
                                code,
                                value,
                                isValid = true
                            ) // found a code in the dictionary - true
                        case _ =>
                            TransformedField(
                                code_category,
                                code,
                                null,
                                isValid = false
                            ) // not found a code in the dictionary - false
                    }
                // this should never happen as we validate code category before actual processing
                case _ =>
                    throw new IllegalArgumentException(
                        s"Code category '$code_category' not found in throtle dictionary: $throtleDict"
                    )
            }
        }
    }

    private def transformThrottleDictionaryToMap(
        dataset: Dataset[ThrotleDictionary]
    ): Map[String, Map[String, String]] = {
        dataset
            .collect()
            .toSeq
            .groupBy(_.code_category)
            .mapValues(v => v.map(throtleDict => throtleDict.code -> throtleDict.code_value).toMap)
            // need the line below to avoid mapValues serialization exception
            // visit https://github.com/scala/bug/issues/7005 and https://github.com/scala/scala/pull/2308 for more insights
            .map(identity)
    }

    private def readThrottleDictionary(dictTable: String, spark: SparkSession): Dataset[ThrotleDictionary] = {
        import spark.implicits._
        spark
            .table(dictTable)
            .as[ThrotleDictionary]
    }

    private def readRawData(srcTable: String, feedDate: String, spark: SparkSession) = {
        spark
            .table(srcTable)
            .where(col("feed_date") === feedDate)
            .drop("feed_date", "file_name", "cxi_id")
            .dropDuplicates("throtle_id")
    }

    private[throtle] def write(destTable: String, refinedDf: DataFrame): Unit = {
        val srcTable = "newTidAtt"

        refinedDf.createOrReplaceTempView(srcTable)
        refinedDf.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.throtle_id <=> $srcTable.throtle_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
