package com.cxi.cdp.data_processing
package curated_zone.signal_framework.demographics

import refined_zone.hub.identity.model.IdentityType
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import scala.collection.immutable.ListSet

class DemographicsSignalsJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("test readCustomer360") {
        // given
        val customer360TableName = "customer360TableNameTemp"
        val df = spark
            .createDataFrame(
                List(
                    (
                        "uuid1",
                        Map(
                            IdentityType.ThrotleId.code -> Array("throtle_id_1", "throtle_id_2"),
                            IdentityType.Email.code -> Array("one@gmail.com", "two@gmail.com")
                        ),
                        true
                    ),
                    (
                        "uuid2",
                        Map(
                            IdentityType.ThrotleId.code -> Array("throtle_id_11"),
                            IdentityType.MaidAAID.code -> Array("maid_aaid_11", "maid_aaid_22")
                        ),
                        true
                    ),
                    (
                        "uuid3",
                        Map(
                            IdentityType.Phone.code -> Array("phone_111", "phone_222")
                        ),
                        true
                    ),
                    (
                        "uuid4",
                        Map(
                            IdentityType.ThrotleId.code -> Array("throtle_id_4444")
                        ),
                        false
                    ), // inactive customer
                    (
                        "uuid5",
                        Map(
                            IdentityType.ThrotleId.code -> Array.empty[String]
                        ),
                        true
                    ), // array is empty
                    (
                        "uuid6",
                        Map(
                            IdentityType.ThrotleId.code -> null
                        ),
                        true
                    ) // array is null
                )
            )
            .toDF("customer_360_id", "identities", "active_flag")
        df.createOrReplaceTempView(customer360TableName)

        // when
        val actual = DemographicsSignalsJob.readCustomer360WithThrotleIds(spark, customer360TableName)

        // then
        val actualData = actual.collect()
        withClue(
            "Read data does not match." + actualData.mkString("\nActual data:\n", "\n-----------------------\n", "\n\n")
        ) {
            val expected = Seq(
                ("uuid1", "throtle_id_1"),
                ("uuid1", "throtle_id_2"),
                ("uuid2", "throtle_id_11")
            ).toDF("customer_360_id", "throtle_id")
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test readThrotleTidAtt") {
        // given
        val tidAttTableName = "tid_attTableTemp"
        val signals = ListSet("age", "children")
        val df = spark
            .createDataFrame(
                List(
                    ("throtle_id1", 1, true, null, null),
                    ("throtle_id2", 2, false, null, null),
                    ("throtle_id3", 3, false, null, null),
                    ("throtle_id4", 4, true, null, null)
                )
            )
            .toDF("throtle_id", "age", "children", "unused_signal1", "unused_signal2")
        df.createOrReplaceTempView(tidAttTableName)

        // when
        val actual = DemographicsSignalsJob.readThrotleTidAttAttributesAsSignals(spark, tidAttTableName, signals)

        // then
        val actualData = actual.collect()
        withClue(
            "Read data does not match." + actualData.mkString("\nActual data:\n", "\n-----------------------\n", "\n\n")
        ) {
            val expected = Seq(
                ("throtle_id1", 1, true),
                ("throtle_id2", 2, false),
                ("throtle_id3", 3, false),
                ("throtle_id4", 4, true)
            ).toDF("throtle_id", "age", "children")
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test transform") {
        // given
        val customer360Df = Seq(
            ("uuid1", "throtle_id_1"),
            ("uuid2", "throtle_id_2"),
            ("uuid3", "throtle_id_3"),
            ("uuid3", "throtle_id_4"), // two diff throtle_ids matched with same customer
            ("uuid11", "throtle_id_11")
        ).toDF("customer_360_id", "throtle_id")
        val tidAttschema = StructType(
            Array(
                StructField("throtle_id", StringType, true),
                StructField("age", IntegerType, true),
                StructField("children", BooleanType, true)
            )
        )
        import collection.JavaConverters._
        val refinedThrotleTidAttDf = spark.createDataFrame(
            Seq(
                Row("throtle_id_1", 1, true),
                Row("throtle_id_2", 2, false),
                Row("throtle_id_3", 3, null), // children signal is null -> filtered out
                Row(
                    "throtle_id_4",
                    4,
                    null
                ), // age signal for customer uuid3 contains age values [3, 4], only first value will be picked, children signal is null -> filtered out
                Row("throtle_id_5", 4, true) // filtered out, no matched throtle_id
            ).asJava,
            tidAttschema
        )
        val feedDate = "2022-02-24"
        val signalNameToSignalDomain = Map("age" -> "profile", "children" -> "some_other_domain")

        // when
        val transformedDfs = DemographicsSignalsJob
            .transform(customer360Df, refinedThrotleTidAttDf, signalNameToSignalDomain, feedDate)

        // then
        val ageSignalDf = transformedDfs.find(data => data._1 == "profile" && data._2 == "age").map(_._3).get
        val ageSignalActualData = ageSignalDf.collect()
        val schema = StructType(
            Array(
                StructField("customer_360_id", StringType, true),
                StructField("signal_generation_date", StringType, false),
                StructField("signal_domain", StringType, false),
                StructField("signal_name", StringType, false),
                StructField("signal_value", StringType, true)
            )
        )

        withClue(
            "Transformed data does not match." + ageSignalActualData
                .mkString("\nActual data:\n", "\n-----------------------\n", "\n\n")
        ) {
            val expected_1 = spark.createDataFrame(
                Seq(
                    Row("uuid1", feedDate, "profile", "age", "1"),
                    Row("uuid2", feedDate, "profile", "age", "2"),
                    Row("uuid3", feedDate, "profile", "age", "3")
                ).asJava,
                schema
            )
            ageSignalDf.schema shouldEqual expected_1.schema
            ageSignalActualData should contain theSameElementsAs expected_1.collect()
        }

        val childrenSignalDf =
            transformedDfs.find(data => data._1 == "some_other_domain" && data._2 == "children").map(_._3).get
        val childrenSignalActualData = childrenSignalDf.collect()
        withClue(
            "Transformed data does not match." + childrenSignalActualData
                .mkString("\nActual data:\n", "\n-----------------------\n", "\n\n")
        ) {
            val expected_2 = spark.createDataFrame(
                Seq(
                    Row("uuid1", feedDate, "some_other_domain", "children", "true"),
                    Row("uuid2", feedDate, "some_other_domain", "children", "false")
                ).asJava,
                schema
            )
            childrenSignalDf.schema shouldEqual expected_2.schema
            childrenSignalActualData should contain theSameElementsAs expected_2.collect()
        }
    }

}
