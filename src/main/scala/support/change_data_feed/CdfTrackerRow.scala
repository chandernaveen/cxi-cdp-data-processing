package com.cxi.cdp.data_processing
package support.change_data_feed

case class CdfTrackerRow(consumer_id: String, table_name: String, latest_processed_version: Long)
