package com.cxi.cdp.data_processing
package refined_zone.hub

import support.change_data_feed.{ChangeDataFeedService, ChangeDataFeedSource}

object ChangeDataFeedViews {

    def orderSummary(cdfTrackerTable: String, orderSummaryTables: Seq[String]): ChangeDataFeedSource = {
        val cdfService = new ChangeDataFeedService(cdfTrackerTable)
        new ChangeDataFeedSource.SimpleUnion(cdfService, orderSummaryTables)
    }

    def item(cdfTrackerTable: String, itemTables: Seq[String]): ChangeDataFeedSource = {
        val cdfService = new ChangeDataFeedService(cdfTrackerTable)
        new ChangeDataFeedSource.SimpleUnion(cdfService, itemTables)
    }
    def cdfSingleTable(cdfTrackerTable: String, sourceTableName: String): ChangeDataFeedSource = {
        val cdfService = new ChangeDataFeedService(cdfTrackerTable)
        new ChangeDataFeedSource.SingleTable(cdfService, sourceTableName)
    }

}
