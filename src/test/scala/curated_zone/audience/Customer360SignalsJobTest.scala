package com.cxi.cdp.data_processing
package curated_zone.audience

import curated_zone.audience.Customer360SignalsJob.DatalakeTablesConfig
import curated_zone.audience.model.SignalUniverse
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class Customer360SignalsJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    def signalUniverseDf: DataFrame = {
        val signalConfigCreateDate = sqlDate(2022, 1, 1)
        Seq(
            ("food_and_drink_preference", "Food and Drink Preference", "burger_affinity", "Burger Affinity",
                "food_and_drink_preference.burger_affinity_boolean", "boolean", "test description", "boolean",
                signalConfigCreateDate, signalConfigCreateDate, true, true),
            // another signal in the 'food_and_drink_preference' domain
            ("food_and_drink_preference", "Food and Drink Preference", "cola_affinity", "Cola Affinity",
                "food_and_drink_preference.cola_affinity_boolean", "boolean", "test description", "boolean",
                signalConfigCreateDate, signalConfigCreateDate, true, true),
            ("channel_preferences", "Channel Preferences", "delivery_preferred", "Delivery Preferred",
                "channel_preferences.delivery_preferred_boolean", "boolean", "test description", "boolean",
                signalConfigCreateDate, signalConfigCreateDate, true, true),
            ("campaign", "Campaign", "reachability_email", "Reachability - Email",
                "campaign.reachability_email_boolean", "boolean", "test description", "boolean",
                signalConfigCreateDate, signalConfigCreateDate, true, true),
            ("profile", "Profile", "age_category", "Age Category",
                "profile.age_category_keyword", "keyword", "test description", "enumeration",
                signalConfigCreateDate, signalConfigCreateDate, true, true),
            // inactive signal, should be ignored
            ("profile", "Profile", "gender", "Gender",
                "profile.gender_keyword", "keyword", "test description", "enumeration",
                signalConfigCreateDate, signalConfigCreateDate, false, true)
        ).toDF("domain_name", "ui_domain_label", "signal_name", "ui_signal_label", "es_signal_name", "es_type",
            "signal_description", "ui_type", "create_date", "update_date", "is_active", "ui_is_signal_exposed")
    }

    test("test create DF to save to ES") {
        // given
        val signalUniverseTable = "tempSignalUniverseTable"

        signalUniverseDf.createOrReplaceTempView(signalUniverseTable)

        val customer360Table = "tempCustomer360Table"
        val customer360createDate = sqlDate(2022, 2, 1)
        val customer360Df = Seq(
            ("cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), customer360createDate, customer360createDate, true),
            ("cust_id2", Map("email" -> Array("test@domain.com")), customer360createDate, customer360createDate, true),
            // inactive customer, should be ignored
            ("cust_id3", Map("email" -> Array("test@domain.com")), customer360createDate, customer360createDate, false),
            // there are no signals generated for this customer, though we need to store it to the ES
            ("cust_id4", Map("email" -> Array("no_signals_customer@domain.com")), customer360createDate, customer360createDate, true)
        ).toDF("customer_360_id", "identities", "create_date", "update_date", "active_flag")
        customer360Df.createOrReplaceTempView(customer360Table)

        val customer360SignalsTable = "tempCustomer360SignalsTable"
        val signalGenerationDate = sqlDate(2022, 2, 24)
        val customer360SignalsDf = Seq(
            // customer #1
            ("cust_id1", "campaign", "reachability_email", "true", signalGenerationDate),
            ("cust_id1", "food_and_drink_preference", "burger_affinity", "true", signalGenerationDate),
            ("cust_id1", "food_and_drink_preference", "cola_affinity", "true", signalGenerationDate),
            ("cust_id1", "channel_preferences", "delivery_preferred", "true", signalGenerationDate),
            ("cust_id1", "profile", "age_category", "25-30", signalGenerationDate),
            // signal doesn't exist in the signal universe table, should be ignored
            ("cust_id1", "some_domain", "some_signal", "true", signalGenerationDate),
            // inactive signal, should be ignored
            ("cust_id1", "profile", "gender", "male", signalGenerationDate),

            // customer #2
            ("cust_id2", "food_and_drink_preference", "burger_affinity", "true", signalGenerationDate),
            // signal's generation date is different, should be ignored
            ("cust_id2", "channel_preferences", "delivery_preferred", "true", sqlDate(2021, 12, 10)),

            // inactive customer's signal, should be ignored
            ("cust_id3", "food_and_drink_preference", "burger_affinity", "true", signalGenerationDate)
        ).toDF("customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        customer360SignalsDf.createOrReplaceTempView(customer360SignalsTable)

        // when
        val actual = Customer360SignalsJob.process(spark, DatalakeTablesConfig(signalUniverseTable, customer360Table, customer360SignalsTable), signalGenerationDate.toString)

        // then
        withClue("Transform data does not match") {
            val expected = Seq(
                ("cust_id1",
                    Map("reachability_email_boolean" -> "true"),
                    Map("delivery_preferred_boolean" -> "true"),
                    Map("burger_affinity_boolean" -> "true", "cola_affinity_boolean" -> "true"),
                    Map("age_category_keyword" -> "25-30"),
                    Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2"))),
                ("cust_id2",
                    null,
                    null,
                    Map("burger_affinity_boolean" -> "true"),
                    null,
                    Map("email" -> Array("test@domain.com"))),
                // there are no signals generated for this customer, though we need to store it to the ES
                ("cust_id4",
                    null,
                    null,
                    null,
                    null,
                    Map("email" -> Array("no_signals_customer@domain.com")))
            ).toDF("customer_360_id", "campaign", "channel_preferences", "food_and_drink_preference", "profile", "customer_identities")
            actual.schema shouldEqual expected.schema
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("test normalize signals") {
        // given
        val signals = Seq(
            Row("cust_id1", Seq(), Seq(Row("delivery_preferred_boolean", "true")), Seq(), Seq()),
            Row("cust_id2", Seq(Row("reachability_email_boolean", "true")), Seq(Row("delivery_preferred_boolean", "true")), Seq(Row("burger_affinity_boolean", "true")), Seq(Row("age_category_keyword", "55-59"))),
            Row("cust_id3", Seq(), Seq(), Seq(Row("burger_affinity_boolean", "true"), Row("cola_affinity_boolean", "true")), Seq())
        )

        val signalType = ArrayType(StructType(Array(
            StructField("signal_es_name", StringType),
            StructField("signal_value", StringType)
        )))
        val signalsSchema = StructType(Array(
            StructField("customer_360_id", StringType),
            StructField("campaign", signalType),
            StructField("channel_preferences", signalType),
            StructField("food_and_drink_preference", signalType),
            StructField("profile", signalType)
        ))
        import collection.JavaConverters._
        val signalsDf = spark.createDataFrame(signals.asJava, signalsSchema)

        // when
        val actual = Customer360SignalsJob.normalizeSignals(signalsDf, signalUniverseDf.as[SignalUniverse].collect())

        // then
        withClue("Normalized data data does not match") {
            val expected = Seq(
                ("cust_id1", null, Map("delivery_preferred_boolean" -> "true"), null, null),
                ("cust_id2", Map("reachability_email_boolean" -> "true"), Map("delivery_preferred_boolean" -> "true"), Map("burger_affinity_boolean" -> "true"), Map("age_category_keyword" -> "55-59")),
                ("cust_id3", null, null, Map("burger_affinity_boolean" -> "true", "cola_affinity_boolean" -> "true"), null)
            ).toDF("customer_360_id", "campaign", "channel_preferences", "food_and_drink_preference", "profile")
            actual.schema shouldEqual expected.schema
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

    test("test extract signal domain as columns") {
        // given
        val signalsWithCustomerDetails = Seq(
            ("campaign", "cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "reachability_email_boolean", "true"),
            ("food_and_drink_preference", "cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "burger_affinity_boolean", "true"),
            ("food_and_drink_preference", "cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "cola_affinity_boolean", "true"),
            ("channel_preferences", "cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "delivery_preferred_boolean", "true"),
            ("profile", "cust_id1", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "age_category_keyword", "25-30"),
            ("food_and_drink_preference", "cust_id2", Map("email" -> Array("test@domain.com")), "burger_affinity_boolean", "true")
        ).toDF("domain_name", "customer_360_id", "identities", "signal_es_name", "signal_value")

        // when
        val actual = Customer360SignalsJob.extractSignalDomainAsColumn(signalsWithCustomerDetails)

        // then
        withClue("Transformed data data does not match") {
            val signalType = ArrayType(StructType(Array(
                StructField("signal_es_name", StringType),
                StructField("signal_value", StringType)
            )), false)
            val signalsSchema = StructType(Array(
                StructField("customer_360_id", StringType),
                StructField("campaign", signalType, false),
                StructField("channel_preferences", signalType, false),
                StructField("food_and_drink_preference", signalType, false),
                StructField("profile", signalType, false)
            ))
            val expected = Seq(
                Row("cust_id1", Seq(Row("reachability_email_boolean", "true")), Seq(Row("delivery_preferred_boolean", "true")), Seq(Row("burger_affinity_boolean", "true"), Row("cola_affinity_boolean", "true")), Seq(Row("age_category_keyword", "25-30"))),
                Row("cust_id2", Seq(), Seq(), Seq(Row("burger_affinity_boolean", "true")), Seq())
            )
            import collection.JavaConverters._
            val expectedDf = spark.createDataFrame(expected.asJava, signalsSchema)

            actual.schema shouldEqual expectedDf.schema
            actual.collect() should contain theSameElementsAs expectedDf.collect()
        }
    }

    private def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
    }

}
