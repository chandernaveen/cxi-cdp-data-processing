package com.cxi.cdp.data_processing
package refined_zone.pos_square.config

import support.packages.utils.ContractUtils

case class ProcessorConfig
    (
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String
    )
