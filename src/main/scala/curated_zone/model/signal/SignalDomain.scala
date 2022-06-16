package com.cxi.cdp.data_processing
package curated_zone.model.signal

trait SignalDomain[T <: Signal] {

    def signalDomainName: String

    def signals: Seq[T]

    def signalNames: Seq[String] = signals.map(_.signalName)

}
