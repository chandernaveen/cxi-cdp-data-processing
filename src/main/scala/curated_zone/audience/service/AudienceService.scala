package com.cxi.cdp.data_processing
package curated_zone.audience.service

import com.cxi.cdp.data_processing.curated_zone.audience.model.Customer360
import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityId
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.graphframes.GraphFrame

import java.time.Instant
import java.util.UUID

object AudienceService {

    /** GraphFrames framework pre-defined column names
      * https://graphframes.github.io/graphframes/docs/_site/user-guide.html#creating-graphframes
      */
    object GraphFrames {
        val VertexIdCol = "id"
        val EdgeSrcCol = "src"
        val EdgeDstCol = "dst"
        val ComponentCol = "component"
    }

    val ComponentIdCol = "component_id"

    def buildConnectedComponents(vertexDf: DataFrame, edgeDf: DataFrame): DataFrame = {
        val identityGraph = GraphFrame(vertexDf, edgeDf)
        identityGraph.connectedComponents
            .run()
            .withColumnRenamed(GraphFrames.VertexIdCol, CxiIdentity.CxiIdentityId)
            .withColumnRenamed(GraphFrames.ComponentCol, ComponentIdCol)
    }

    private final val Customer360IdCol = "customer_360_id"
    private final val CustomerNumIdentitiesCol = "customer_num_identities"
    private final val CustomerCreateDateCol = "customer_create_date"

    /** Creates customer_360 table based on the previous customer_360 table and the current connected components
      * of the identity graph.
      *
      * Every connected component is considered a customer. The tricky part is reusing customer_360 IDs from
      * the previous customer_360 table if possible.
      *
      * To reuse the previously generated customer_360 ID, the connected components table is joined with the active
      * customers from the previous customer_360 on identity ID and the following rules are applied for each component:
      *
      * 1. If the connected component's identities do not exist in the previous customer_360, then create a new UUID
      * and use it as the customer_360 ID.
      *
      * 2. If the connected component has identities that exist in the previous customer_360, then take all matched
      * customer_360 IDs from the previous customer_360 and determine the one that will be used for this component:
      *    - keep matched customer_360 IDs for which the number of identities matched in this connected component
      *      is greater than half of the number of identities in the previous customer_360 for this
      *      customer_360 ID (this additional condition is explained in detail in
      *      AudienceServiceTest."updateCustomer360 for customer unmerge - minimal case" test case)
      *    - among the customer_360 IDs that are left after the previous step, pick the oldest one (based on the
      *      `create_date`), in the case of a tie pick one with the most connections in the previous customer_360 table
      *    - if no customer_360 IDs are matched, create a new UUID and use it as the customer_360 ID
      *
      * 3. Deactivate all customers from the previous customer_360 whose customer_360 IDs were not reused.
      */
    def updateCustomer360(
        spark: SparkSession,
        connectedComponents: DataFrame,
        prevCustomer360: Dataset[Customer360]
    ): Dataset[Customer360] = {
        import spark.implicits._

        val prevActiveCustomer360 = prevCustomer360.filter(_.active_flag)

        val expandedPrevCustomer360 = expandCustomer360ByIdentityId(spark, prevActiveCustomer360)

        val now = new java.sql.Date(Instant.now().toEpochMilli)

        val newActiveCustomer360 = connectedComponents
            .join(expandedPrevCustomer360, Seq(CxiIdentity.CxiIdentityId), "left_outer")
            .map(createIntermediateCustomer _)
            .groupByKey(_.componentId)
            .reduceGroups(IntermediateCustomer.mergeForSameComponent _)
            .map({ case (_, intermediateCustomer) => toCustomer360(intermediateCustomer, now) })

        mergePrevAndNewCustomer360(spark, prevActiveCustomer360, newActiveCustomer360)
    }

    /** A previously created customer that has matched the particular connected component.
      *
      * @param numIdentitiesMatched number of identities in this component that are also present in the customer
      * @param numIdentitiesTotal   total number of identities in the previously created customer
      * @param customerCreateDate   the customer's creation date
      */
    private[audience] case class MatchedPrevCustomer(
        numIdentitiesMatched: Int,
        numIdentitiesTotal: Int,
        customerCreateDate: java.sql.Date
    )

    /** An intermediate customer object created from the connected component.
      *
      * @param componentId
      * @param cxiIdentityIds identity IDs that belong to this component
      * @param prevCustomers  previously created customers that have matched this connected component by identity ID
      */
    private[audience] case class IntermediateCustomer(
        componentId: Long,
        cxiIdentityIds: Set[String],
        prevCustomers: Map[String, MatchedPrevCustomer]
    )

    private[audience] object IntermediateCustomer {

        def mergeForSameComponent(first: IntermediateCustomer, second: IntermediateCustomer): IntermediateCustomer = {
            first.copy(
                cxiIdentityIds = first.cxiIdentityIds ++ second.cxiIdentityIds,
                prevCustomers = mergePrevCustomers(first.prevCustomers, second.prevCustomers)
            )
        }

        private def mergePrevCustomers(
            first: Map[String, MatchedPrevCustomer],
            second: Map[String, MatchedPrevCustomer]
        ): Map[String, MatchedPrevCustomer] = {
            val customerIds = first.keySet ++ second.keySet
            customerIds
                .flatMap(customerId => {
                    val maybePrevCustomer = (first.get(customerId), second.get(customerId)) match {
                        case (Some(firstCustomer), Some(secondCustomer)) =>
                            val numIdentitiesMatched =
                                firstCustomer.numIdentitiesMatched + secondCustomer.numIdentitiesMatched
                            Some(firstCustomer.copy(numIdentitiesMatched = numIdentitiesMatched))
                        case (maybeFirstCustomer, maybeSecondCustomer) => maybeFirstCustomer.orElse(maybeSecondCustomer)
                    }
                    maybePrevCustomer.map(prevCustomer => customerId -> prevCustomer)
                })
                .toMap
        }
    }

    private[audience] def expandCustomer360ByIdentityId(
        spark: SparkSession,
        customer360: Dataset[Customer360]
    ): DataFrame = {
        import spark.implicits._

        customer360
            .flatMap(customer => {
                val qualifiedIdentityIds = for {
                    identitiesForType <- customer.identities
                    (identityType, cxiIdentityIds) = identitiesForType
                    cxiIdentityId <- cxiIdentityIds
                } yield IdentityId.qualifiedIdentityId(identityType, cxiIdentityId)

                val numIdentities = qualifiedIdentityIds.size

                for {
                    qualifiedIdentityId <- qualifiedIdentityIds
                } yield (qualifiedIdentityId, customer.customer_360_id, numIdentities, customer.create_date)
            })
            .toDF(CxiIdentity.CxiIdentityId, Customer360IdCol, CustomerNumIdentitiesCol, CustomerCreateDateCol)
    }

    /** Creates IntermediateCustomer from a row of connected components
      * joined with the previous customers by identity ID.
      */
    private[audience] def createIntermediateCustomer(row: Row): IntermediateCustomer = {
        val componentId = row.getAs[Long](AudienceService.ComponentIdCol)
        val cxiIdentityId = row.getAs[String](CxiIdentity.CxiIdentityId)

        val prevCustomers = Option(row.getAs[String](Customer360IdCol)) match {
            case None => Map.empty[String, MatchedPrevCustomer]

            case Some(customer360Id) =>
                Map(
                    customer360Id -> MatchedPrevCustomer(
                        numIdentitiesMatched = 1,
                        numIdentitiesTotal = row.getAs[Int](CustomerNumIdentitiesCol),
                        customerCreateDate = row.getAs[java.sql.Date](CustomerCreateDateCol)
                    )
                )
        }

        IntermediateCustomer(componentId, Set(cxiIdentityId), prevCustomers)
    }

    private[audience] def toCustomer360(intermediateCustomer: IntermediateCustomer, now: java.sql.Date): Customer360 = {
        val (customer360Id, createDate) = findWinningPrevCustomer(intermediateCustomer.prevCustomers)
            .map({ case (customer360Id, matchedPrevCustomer) =>
                (customer360Id, matchedPrevCustomer.customerCreateDate)
            })
            .getOrElse((UUID.randomUUID().toString, now))

        val identitiesMap = groupIdentitiesByType(intermediateCustomer.cxiIdentityIds)

        Customer360(
            customer_360_id = customer360Id,
            identities = identitiesMap,
            create_date = createDate,
            update_date = now,
            active_flag = true
        )
    }

    private[audience] def findWinningPrevCustomer(
        prevCustomers: Map[String, MatchedPrevCustomer]
    ): Option[(String, MatchedPrevCustomer)] = {
        prevCustomers.toSeq
            .filter({ case (_, matchedPrevCustomer) =>
                // consider only customers that have more than half of their previous identities matched
                2 * matchedPrevCustomer.numIdentitiesMatched > matchedPrevCustomer.numIdentitiesTotal
            })
            .sortBy({ case (_, matchedPrevCustomer) =>
                // prefer older customers first, in the case of a tie prefer customers with more identities
                (matchedPrevCustomer.customerCreateDate.getTime, -matchedPrevCustomer.numIdentitiesTotal)
            })
            .headOption
    }

    private[audience] def groupIdentitiesByType(qualifiedIdentityIds: Set[String]): Map[String, Seq[String]] = {
        qualifiedIdentityIds.toSeq
            .map(IdentityId.fromQualifiedIdentityId)
            .sorted(IdentityId.SourceToTargetOrdering)
            .groupBy(_.identity_type)
            .mapValues(values => values.map(_.cxi_identity_id))
    }

    /** Creates a unified customer360 dataset from the previously active customers and the currently active customers.
      *
      * 1. If the customer is present in the currently active customers, take it as is.
      * 2. If the customer is present in the previously active customers but not in the currently active customers,
      * mark it as inactive, as its Customer ID was not reused and the customer no longer exists.
      */
    private[audience] def mergePrevAndNewCustomer360(
        spark: SparkSession,
        prevActiveCustomer360: Dataset[Customer360],
        newActiveCustomer360: Dataset[Customer360]
    ): Dataset[Customer360] = {
        import spark.implicits._

        newActiveCustomer360
            .joinWith(
                prevActiveCustomer360,
                newActiveCustomer360(Customer360IdCol) === prevActiveCustomer360(Customer360IdCol),
                "full_outer"
            )
            .flatMap({
                case (newCustomer, _) if newCustomer != null => Some(newCustomer)
                case (null, prevCustomer) if prevCustomer != null => Some(prevCustomer.copy(active_flag = false))
                case _ => None // should not actually happen
            })
    }

}
