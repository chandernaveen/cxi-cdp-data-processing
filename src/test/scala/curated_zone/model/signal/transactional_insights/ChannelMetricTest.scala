package com.cxi.cdp.data_processing
package curated_zone.model.signal.transactional_insights

import com.cxi.cdp.data_processing.refined_zone.hub.model.ChannelType
import org.scalatest.{FunSuite, Matchers}

class ChannelMetricTest extends FunSuite with Matchers {

    test("a specific ChannelMetric should be defined for every ChannelType") {
        ChannelType.values.foreach { channelType =>
            withClue(s"checking ChannelMetric for ChannelType $channelType") {
                noException should be thrownBy ChannelMetric.fromChannelType(channelType)
            }
        }
    }

}
