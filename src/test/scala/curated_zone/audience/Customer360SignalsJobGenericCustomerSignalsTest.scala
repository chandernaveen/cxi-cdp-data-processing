package com.cxi.cdp.data_processing
package curated_zone.audience

import curated_zone.audience.model.{SignalType, SignalUniverse}
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class Customer360SignalsJobGenericCustomerSignalsTest extends BaseSparkBatchJobTest {

    import spark.implicits._
    def signalUniverseDf: Dataset[SignalUniverse] = {
        val signalConfigCreateDate = sqlDate(2022, 1, 1)
        val burgerAffinity = SignalUniverse("food_and_drink_preference", "Food and Drink Preference", "burger_affinity", "Burger Affinity",
            "food_and_drink_preference.burger_affinity_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        // another signal in the 'food_and_drink_preference' domain
        val colaAffinity = SignalUniverse("food_and_drink_preference", "Food and Drink Preference", "cola_affinity", "Cola Affinity",
            "food_and_drink_preference.cola_affinity_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        val deliveryPreferred = SignalUniverse("channel_preferences", "Channel Preferences", "delivery_preferred", "Delivery Preferred",
            "channel_preferences.delivery_preferred_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        val reachibilityEmail = SignalUniverse("campaign", "Campaign", "reachability_email", "Reachability - Email",
            "campaign.reachability_email_boolean", "boolean", "test description", "boolean",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        val ageCategory = SignalUniverse("profile", "Profile", "age_category", "Age Category",
            "profile.age_category_keyword", "keyword", "test description", "enumeration",
            signalConfigCreateDate, signalConfigCreateDate, true, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        // inactive signal, should be ignored
        val gender = SignalUniverse("profile", "Profile", "gender", "Gender",
            "profile.gender_keyword", "keyword", "test description", "enumeration",
            signalConfigCreateDate, signalConfigCreateDate, false, true, SignalType.GeneralCustomerSignal.code, "general_customer_signals")
        Seq(burgerAffinity, colaAffinity, deliveryPreferred, reachibilityEmail, ageCategory, gender).toDS()
    }

    test("test read generic customer signals") {
        // given
        val customer360SignalsTable = "tempCustomer360SignalsTable"
        val signalGenerationDate = sqlDate(2022, 2, 24)
        val customer360SignalsDf = Seq(
            // customer #1
            ("cust_id1", "campaign", "reachability_email", "true", signalGenerationDate),
            ("cust_id1", "campaign", "reachability_phone", "true", signalGenerationDate),
            // signal's generation date is different, should be ignored
            ("cust_id2", "channel_preferences", "delivery_preferred", "true", sqlDate(2021, 12, 10)),
            ("cust_id3", "food_and_drink_preference", "burger_affinity", "true", signalGenerationDate),
        ).toDF("customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        customer360SignalsDf.createOrReplaceTempView(customer360SignalsTable)

        // when
        val actual = Customer360SignalsJob.readGenericDailyCustomerSignals(spark, customer360SignalsTable, "2022-02-24")

        // then
        val actualData = actual.collect()
        withClue("Transform data does not match." + actualData.map(_.json).mkString("\nActual data:\n","\n-----------------------\n", "\n\n")) {
            val expected = Seq(
                ("cust_id1", "campaign", "reachability_email", "true", signalGenerationDate),
                ("cust_id1", "campaign", "reachability_phone", "true", signalGenerationDate),
                ("cust_id3", "food_and_drink_preference", "burger_affinity", "true", signalGenerationDate)
            ).toDF("customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test process generic customer signals") {
        // given
        val signalUniverseTable = "tempSignalUniverseTable"

        signalUniverseDf.createOrReplaceTempView(signalUniverseTable)

        val customer360SignalsTable = "tempCustomer360GenericDailySignalsTable"
        val signalGenerationDate = sqlDate(2022, 2, 24)
        val customer360GenericDailySignalsDf = Seq(
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
            // customer #3
            ("cust_id3", "food_and_drink_preference", "burger_affinity", "true", signalGenerationDate)
        ).toDF("customer_360_id", "signal_domain", "signal_name", "signal_value", "signal_generation_date")
        customer360GenericDailySignalsDf.createOrReplaceTempView(customer360SignalsTable)

        // when
        val actual = Customer360SignalsJob.processGenericCustomerSignals(spark,
            customer360SignalsTable, spark.table(signalUniverseTable).as[SignalUniverse].filter(_.is_active), "2022-02-24")

        // then
        val actualData = actual.collect()
        withClue("Transform data does not match." + actualData.map(_.json).mkString("\nActual data:\n","\n-----------------------\n", "")) {
            val expected = Seq(
                ("cust_id1",
                    Map(
                        "campaign" ->
                            Map("reachability_email_boolean" -> "true"),
                        "channel_preferences" ->
                            Map("delivery_preferred_boolean" -> "true"),
                        "food_and_drink_preference" ->
                            Map("burger_affinity_boolean" -> "true", "cola_affinity_boolean" -> "true"),
                        "profile" ->
                            Map("age_category_keyword" -> "25-30"))
                ),
                ("cust_id2",
                    Map("food_and_drink_preference" ->
                        Map("burger_affinity_boolean" -> "true"))
                ),
                ("cust_id3",
                    Map("food_and_drink_preference" ->
                        Map("burger_affinity_boolean" -> "true")))
            ).toDF("customer_360_id", "general_customer_signals")
            actual.schema shouldEqual expected.schema
            actualData should contain theSameElementsAs expected.collect()
        }
    }

    test("test normalize signals") {
        // given
        val signals = Seq(
            Row("cust_id1", Seq(), Seq(Row( "delivery_preferred_boolean", "true")), Seq(), Seq()),
            Row("cust_id2", Seq(Row( "reachability_email_boolean", "true")), Seq(Row( "delivery_preferred_boolean", "true")), Seq(Row( "burger_affinity_boolean", "true")), Seq(Row( "age_category_keyword", "55-59"))),
            Row("cust_id3", Seq(), Seq(), Seq(Row( "burger_affinity_boolean", "true"), Row( "cola_affinity_boolean", "true")), Seq())
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
            ("campaign", "cust_id1", "general_customer_signal", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "reachability_email_boolean", "true"),
            ("food_and_drink_preference", "cust_id1", "general_customer_signal", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "burger_affinity_boolean", "true"),
            ("food_and_drink_preference", "cust_id1", "general_customer_signal", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "cola_affinity_boolean", "true"),
            ("channel_preferences", "cust_id1", "general_customer_signal", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "delivery_preferred_boolean", "true"),
            ("profile", "cust_id1", "general_customer_signal", Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")), "age_category_keyword", "25-30"),
            ("food_and_drink_preference", "cust_id2", "general_customer_signal", Map("email" -> Array("test@domain.com")), "burger_affinity_boolean", "true")
        ).toDF("domain_name", "customer_360_id", "es_field_parent_section_name", "identities", "signal_es_name", "signal_value")

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
                StructField("es_field_parent_section_name", StringType),
                StructField("campaign", signalType, false),
                StructField("channel_preferences", signalType, false),
                StructField("food_and_drink_preference", signalType, false),
                StructField("profile", signalType, false)
            ))
            val expected = Seq(
                Row("cust_id1", "general_customer_signal", Seq(Row("reachability_email_boolean", "true")), Seq(Row("delivery_preferred_boolean", "true")), Seq(Row("burger_affinity_boolean", "true"), Row("cola_affinity_boolean", "true")), Seq(Row("age_category_keyword", "25-30"))),
                Row("cust_id2", "general_customer_signal", Seq(), Seq(), Seq(Row("burger_affinity_boolean", "true")), Seq())
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
