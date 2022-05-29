package com.cxi.cdp.data_processing
package refined_zone.throtle.service

import refined_zone.throtle.model.TransformedField

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, explode}

import scala.collection.mutable.ArrayBuffer

object ThrotleValidator {

    private val logger = Logger.getLogger(this.getClass.getName)

    private val NumOfFailedRecordsToDisplay = 100

    private[throtle] def validateColumnConfigurationIsPresent(
        allColumnNames: Set[String],
        booleanColumnNames: Set[String],
        doNotTransformColumnNames: Set[String],
        integerColumnNames: Set[String],
        dictionaryColumnNames: Set[String]
    ): Unit = {

        val configurationNotFoundFields = allColumnNames
            .diff(booleanColumnNames)
            .diff(doNotTransformColumnNames)
            .diff(dictionaryColumnNames)
            .diff(integerColumnNames)

        if (configurationNotFoundFields.nonEmpty) {
            val errorMsg =
                s"""Configuration not found for columns in raw dataset: ${configurationNotFoundFields.mkString(", ")}.
                Please make sure the field is present in any of the following:
                1. 'schema.do_not_transform_columns' property
                2. 'schema.boolean_columns' property
                3. 'schema.integer_columns' property
                4. throtle dictionary"""
            logger.error(errorMsg)
            throw new IllegalArgumentException(errorMsg)
        }
    }

    private[throtle] def validateCodeIsPresentInDictionary(
        transformedDf: DataFrame,
        transformedColumnsNames: ArrayBuffer[String],
        spark: SparkSession
    ): Unit = {

        import spark.implicits._

        val transformedColumns = transformedColumnsNames.map(col _)

        val failedRecords = transformedDf
            .select(explode(array(transformedColumns: _*)).as("transformed_column"))
            .select($"transformed_column.*")
            .as[TransformedField]
            .filter(!_.isValid)
            .distinct()
            .limit(NumOfFailedRecordsToDisplay)
            .collect()

        if (failedRecords.nonEmpty) {
            val errorMessage =
                s"""Code values are missing from the throtle dictionary: (show first $NumOfFailedRecordsToDisplay records):
                   |${failedRecords.mkString("(", ", ", ")")}""".stripMargin
            logger.error(errorMessage)

            throw new IllegalArgumentException(errorMessage)
        }
    }

}
