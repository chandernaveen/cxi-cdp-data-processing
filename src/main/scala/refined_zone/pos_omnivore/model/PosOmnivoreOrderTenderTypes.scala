package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore.model
import refined_zone.hub.model.OrderTenderType
object PosOmnivoreOrderTenderTypes {

    final val PosOmnivoreToCxiTenderType: Map[String, OrderTenderType] = Map(
        "CARD" -> OrderTenderType.CreditCard,
        "CASH" -> OrderTenderType.Cash,
        "NO_SALE" -> OrderTenderType.Other,
        "OTHER" -> OrderTenderType.Other,
        "3RD PARTY" -> OrderTenderType.Other,
        "WALLET" -> OrderTenderType.Wallet
    )

}
