// Databricks notebook source
/**
 * @author - Luis Velez
 * @createdOn - Date
 * @version - 1.0
 * @Ticket - N/A
 * @App-Dependency - N/A
 * @function-desc - Generic Transformation (no changes)
 */
def identity(df: DataFrame): DataFrame = {
  df
}

// COMMAND ----------

def spaceToUnderScoreInColumnNamesTransformation(df: DataFrame): DataFrame = {
  val colsRenamed = df.columns.zip(df.columns.map(col => col.replace(' ', '_'))).map(el => col(el._1).as(el._2))
  df.select(colsRenamed: _*)
}

// COMMAND ----------

val transformationFunctionsMap = Map[String, DataFrame => DataFrame](
  "identity" -> identity,
  "spaceToUnderscore" -> spaceToUnderScoreInColumnNamesTransformation
)
