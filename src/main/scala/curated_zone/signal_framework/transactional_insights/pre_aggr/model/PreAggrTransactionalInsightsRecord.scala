package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.model

case class PreAggrTransactionalInsightsRecord(
                                                 customer_360_id: String,
                                                 cxi_partner_id: String,
                                                 location_id: String,
                                                 ord_date: java.sql.Date,
                                                 signal_domain: String,
                                                 signal_name: String,
                                                 signal_value: Long
                                             )

object PreAggrTransactionalInsightsRecord {
    final val AllLocationsAlias = "_ALL"
}
