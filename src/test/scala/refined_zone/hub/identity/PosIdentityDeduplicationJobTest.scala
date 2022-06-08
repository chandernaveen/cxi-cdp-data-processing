package com.cxi.cdp.data_processing
package refined_zone.hub.identity

import refined_zone.hub.identity.model.IdentityType.{Email, IPv4, Phone}
import refined_zone.hub.identity.PosIdentityDeduplicationJobTest.{IdentityIntermediate, IdentityIntermediateDiff}
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class PosIdentityDeduplicationJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("transform CDF data to new/updated and deleted identities") {

        // given

        val diffData = Seq(
            IdentityIntermediateDiff(
                None,
                Some(
                    IdentityIntermediate(
                        "hash_1",
                        Email.value,
                        "1",
                        Map("email_domain" -> "mailbox.com"),
                        "2022-02-24",
                        "run-id-1"
                    )
                ) // new record
            ),
            IdentityIntermediateDiff(
                None,
                Some(
                    IdentityIntermediate(
                        "hash_1",
                        Email.value,
                        "1",
                        Map("email_domain" -> "mailbox.com"),
                        "2022-02-24",
                        "run-id-2"
                    )
                ) // duplicate of new record
            ),
            IdentityIntermediateDiff(
                Some(IdentityIntermediate("hash_2", Phone.value, "2", null, "2022-02-25", "run-id-1")),
                Some(IdentityIntermediate("hash_2", Phone.value, "3", null, "2022-02-25", "run-id-1")) // updated record
            ),
            IdentityIntermediateDiff(
                Some(IdentityIntermediate("hash_3", IPv4.value, "1", null, "2022-02-24", "run-id-2")), // deleted record
                None
            ),
            IdentityIntermediateDiff(
                Some(IdentityIntermediate("hash_3", IPv4.value, "1", null, "2022-02-24", "run-id-3")), // duplicate of deleted record
                None
            )
        ).toDF()

        // when
        val (deletedIdentities, createdOrUpdatedIdentities) = PosIdentityDeduplicationJob.transform(diffData)

        // then
        withClue("Transformed identities do not match") {
            val expectedCreatedOrUpdated = Seq(
                ("hash_1", Email.value, "1", Map("email_domain" -> "mailbox.com")),
                ("hash_2", Phone.value, "3", null)
            ).toDF("cxi_identity_id", "type", "weight", "metadata")

            createdOrUpdatedIdentities.schema.fields.map(_.name) shouldEqual expectedCreatedOrUpdated.schema.fields.map(
                _.name
            )
            createdOrUpdatedIdentities.collect() should contain theSameElementsAs expectedCreatedOrUpdated.collect()

            val expectedDeleted = Seq(
                ("hash_3", IPv4.value)
            ).toDF("cxi_identity_id", "type")

            deletedIdentities.schema.fields.map(_.name) shouldEqual expectedDeleted.schema.fields.map(_.name)
            deletedIdentities.collect() should contain theSameElementsAs expectedDeleted.collect()
        }
    }
}

object PosIdentityDeduplicationJobTest {
    case class IdentityIntermediate(
        cxi_identity_id: String,
        `type`: String,
        weight: String,
        metadata: Map[String, String],
        feed_date: String,
        run_id: String
    )

    case class IdentityIntermediateDiff(
        previous_record: Option[IdentityIntermediate],
        current_record: Option[IdentityIntermediate]
    )
}
