package com.cxi.cdp.data_processing
package support.change_data_feed

import com.cxi.cdp.data_processing.support.change_data_feed.ChangeDataFeedService._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct, when}
import org.apache.spark.sql.types.{StructField, StructType}

import java.util.Objects

/** Contains utility methods to transform Change Data Feed datasets into the CDF diff format.
  *
  * The original Change Data Feed format tracks each change event along with the change type and version.
  * But often it is not very convenient, and consumers are interested only in states before and after
  * the changes were applied.
  *
  * The CDF diff format provides exactly that: it takes the Change Data Feed format and transforms it to represent
  * changes to each record as the previous (before the changes) and the current (after the changes) state of the record.
  * Either the previous or the current state could be null:
  * - if previous IS NULL, and current IS NOT NULL - then the record is newly created
  * - if previous IS NOT NULL, and current IS NULL - then the record is deleted
  * - if previous IS NOT NULL, and current IS NOT NULL - then the record is updated
  *
  * Example: maintain the total amount of money for all accounts in a bank using Change Data Feed.
  *
  * Let's say we have the following table describing each account:
  * CREATE TABLE bank_account(account_id LONG, balance DOUBLE)
  * USING DELTA
  * TBLPROPERTIES (delta.enableChangeDataFeed = true);
  *
  * Option 1: use default Change Data Feed format
  *
  * Let's say that at the version 10 of the `bank_account` table the total amount stored in all accounts is 10_000:
  * +------------+---------+
  * | account_id | balance |
  * +------------+---------+
  * |          1 |    6000 |
  * |          2 |    4000 |
  * +------------+---------+
  *
  * Suppose there were several updates to the `bank_account` table since version 10,
  * and the change data feed looks like this (ordered by account_id for convenience):
  * +------------+---------+-----------------+------------------+
  * | account_id | balance | _commit_version |   _change_type   |
  * +------------+---------+-----------------+------------------+
  * |          1 |    6000 |              11 | update_preimage  |
  * |          1 |    6200 |              11 | update_postimage |
  * |          1 |    6200 |              13 | update_preimage  |
  * |          1 |    6150 |              13 | update_postimage |
  * |          2 |    4000 |              12 | update_preimage  |
  * |          2 |    4100 |              12 | update_postimage |
  * |          2 |    4100 |              13 | delete           |
  * |          3 |    3000 |              15 | insert           |
  * |          3 |    3000 |              16 | update_preimage  |
  * |          3 |    3700 |              16 | update_postimage |
  * +------------+---------+-----------------+------------------+
  * Account 1 had balance updates, account 2 was eventually removed, and a new account 3 was added and then updated.
  * Now we have to calculate total balance changes, grouping by account and commit version:
  * account 1: +200 (update), -50 (update)
  * account 2: +100 (update), -4100 (delete)
  * account 3: +3000 (insert), +700 (update)
  * After adding everything up, the total balance is 10000 + 200 - 50 + 100 - 4100 + 3000 + 700 = 9850
  *
  * Option 2: let the diff format handle intermediate updates
  * To calculate a balance change for account 1, we don't actually need to track intermediate changes,
  * all we need is the balance before the changes (6000) and the balance after the changes (6150).
  * After converting the Change Data Feed to the diff format we get the following (using account_id as a primary key):
  * +----------------------+----------------------+
  * | previous_record      | current_record       |
  * +------------+---------+------------+---------+
  * | account_id | balance | account_id | balance |
  * +------------+---------+------------+---------+
  * | 1          | 6000    | 1          | 6150    |
  * | 2          | 4000    | NULL       | NULL    |
  * | NULL       | NULL    | 3          | 3700    |
  * +------------+---------+------------+---------+
  * Calculate diffs per account:
  * account 1: +150 (update)
  * account 2: -4000 (delete)
  * account 3: +3700 (insert)
  * After adding everything up, the total balance is 10000 + 150 - 4000 + 3700 = 9850
  */
object CdfDiffFormat {

    final val PreviousRecordColumnName = "previous_record"

    final val CurrentRecordColumnName = "current_record"

    /** Transform a Change Data Feed dataset into the diff format. */
    def transformChangeDataFeed(changeDataFeedDF: DataFrame, compositeKeyColumns: Seq[String])(implicit
        spark: SparkSession
    ): DataFrame = {
        verifyCdfColumnsPresent(changeDataFeedDF)

        val diffSchema = StructType(
            Seq(
                StructField(PreviousRecordColumnName, changeDataFeedDF.schema),
                StructField(CurrentRecordColumnName, changeDataFeedDF.schema)
            )
        )

        val diffRDD = changeDataFeedDF.rdd
            .map(row => {
                val key = extractCompositeKey(row, compositeKeyColumns)
                val intermediateDiff = updateIntermediateDiff(IntermediateCdfRowDiff(None, None), row)
                key -> intermediateDiff
            })
            .reduceByKey(reduceIntermediateDiffs _)
            .map({ case (_, intermediateDiff) => finalizeDiff(intermediateDiff) })

        val diffDF = spark.createDataFrame(diffRDD, diffSchema)

        val finalTransformations = Seq(
            dropNestedCdfColumns _,
            removeEmptyDiffs _
        )

        finalTransformations.foldLeft(diffDF)({ case (df, f) => f.apply(df) })
    }

    /** Transforms a regular dataset into the diff format by copying all the records as `current_record`s and
      * setting all `previous_record`s to be NULLs.
      *
      * This is useful to process both CDF and regular datasets in the same way.
      */
    def transformRegularDataFrame(df: DataFrame): DataFrame = {
        val originalColumns = df.columns.map(col(_))
        df.select(
            lit(null).cast(df.schema).as(PreviousRecordColumnName),
            // `when` is needed to make this column nullable
            when(lit(true), struct(originalColumns: _*).cast(df.schema)).as(CurrentRecordColumnName)
        )
    }

    private def dropNestedCdfColumns(df: DataFrame): DataFrame = {
        df
            .withColumn(PreviousRecordColumnName, col(PreviousRecordColumnName).dropFields(CdfColumnNames: _*))
            .withColumn(CurrentRecordColumnName, col(CurrentRecordColumnName).dropFields(CdfColumnNames: _*))
    }

    /** If a record was inserted and then deleted inside a single diff, both previous_record and current_record fields
      * are null, and we don't want that in the output.
      */
    private def removeEmptyDiffs(df: DataFrame): DataFrame = {
        df.filter(col(PreviousRecordColumnName).isNotNull || col(CurrentRecordColumnName).isNotNull)
    }

    private def verifyCdfColumnsPresent(df: DataFrame): Unit = {
        val cdfColumnsPresent = CdfColumnNames.forall(cdfColumn => df.columns.contains(cdfColumn))

        if (!cdfColumnsPresent) {
            throw new IllegalArgumentException(
                s"The provided DataFrame is not a Change Data Feed. Its columns are: ${df.columns.toList}"
            )
        }
    }

    private def extractCompositeKey(row: Row, compositeKeyColumns: Seq[String]): Seq[java.io.Serializable] = {
        compositeKeyColumns.map(key => row.getAs[java.io.Serializable](key))
    }

    /** This is the intermediate record that contains rows for the same composite key
      * with the lowest and the highest commit version.
      *
      * The `lowestVersionCandidate` is the candidate row for the `previous_record` row.
      * The `highestVersionCandidate` is the candidate row for the `current_record` row.
      *
      * Note: as updates result in two CDF records (UpdatePreImage and UpdatePostImage), we store only one of them
      * in the intermediate record - the one we will actually need for the final diff:
      * - for the `lowestVersionCandidate` it is UpdatePreImage record
      * - for the `highestVersionCandidate` it is UpdatePostImage record
      */
    private case class IntermediateCdfRowDiff(
        lowestVersionCandidate: Option[Row],
        highestVersionCandidate: Option[Row]
    )

    private final val LowestVersionCandidateChangeTypes = Set(
        ChangeType.Insert,
        ChangeType.UpdatePreImage,
        ChangeType.Delete
    )

    private final val HighestVersionCandidateChangeTypes = Set(
        ChangeType.Insert,
        ChangeType.UpdatePostImage,
        ChangeType.Delete
    )

    private def updateIntermediateDiff(diff: IntermediateCdfRowDiff, row: Row): IntermediateCdfRowDiff = {
        sealed abstract class VersionPreference(val cmpFn: (Long, Long) => Boolean)
        case object PreferLowest extends VersionPreference(_ < _)
        case object PreferHighest extends VersionPreference(_ > _)

        def updateCandidate(
            maybeCandidate: Option[Row],
            changeTypes: Set[String],
            versionPreference: VersionPreference
        ): Option[Row] = {
            if (changeTypes.contains(getChangeType(row))) {
                maybeCandidate
                    .map(candidate => {
                        if (versionPreference.cmpFn(getCommitVersion(candidate), getCommitVersion(row))) {
                            candidate
                        } else {
                            row
                        }
                    })
                    .orElse(Some(row))
            } else {
                maybeCandidate
            }
        }

        diff.copy(
            lowestVersionCandidate =
                updateCandidate(diff.lowestVersionCandidate, LowestVersionCandidateChangeTypes, PreferLowest),
            highestVersionCandidate =
                updateCandidate(diff.highestVersionCandidate, HighestVersionCandidateChangeTypes, PreferHighest)
        )
    }

    private def reduceIntermediateDiffs(
        first: IntermediateCdfRowDiff,
        second: IntermediateCdfRowDiff
    ): IntermediateCdfRowDiff = {
        val rowsFromSecond = Seq(second.lowestVersionCandidate, second.highestVersionCandidate).flatten
        rowsFromSecond.foldLeft(first)({ case (diff, row) => updateIntermediateDiff(diff, row) })
    }

    private def finalizeDiff(intermediateDiff: IntermediateCdfRowDiff): Row = {
        val previous = intermediateDiff.lowestVersionCandidate.flatMap({ candidate =>
            getChangeType(candidate) match {
                case ChangeType.Delete | ChangeType.UpdatePreImage =>
                    // if change type is Delete or UpdatePreImage, it means the record existed before
                    Some(candidate)
                case _ => None
            }
        })

        val current = intermediateDiff.highestVersionCandidate.flatMap({ candidate =>
            getChangeType(candidate) match {
                case ChangeType.Insert | ChangeType.UpdatePostImage =>
                    // if change type is Insert or UpdatePostImage, it means the record currently exists
                    Some(candidate)
                case _ => None
            }
        })

        Row(previous.getOrElse(null): Row, current.getOrElse(null): Row)
    }

}
