package org.hazelcast.jetpayments

import com.hazelcast.jet.aggregate.AggregateOperations
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks
import com.hazelcast.jet.pipeline.Sources

/*
 * A Hazelcast Jet batch job that uses the payment receipt map to analyze on which
 * members each payment was made. The reason for doing this is to show that for any
 * given merchant, its payments were always handled on one node. Upon failures the
 * distribution of merchants to nodes will change, but we want to show that at no
 * time are payments for a given merchant performed on multiple nodes at once.
 */
class PaymentMemberCheckPipeline(
    client: HzCluster.ClientInstance, jobName: String, receiptsMapName: String,
) : JetPipeline(client, jobName) {
    private val source = Sources.map<Int, PaymentReceipt>(receiptsMapName)

    override fun describePipeline(): String {
        return """
            This batch Jet pipeline reads from the payment receipt map and checks
            that each merchant's payments are handled on the same node, at any given
            point in time. It does this by grouping the receipts by merchant ID, and
            then aggregating to a sorted list, converting each receipt to a
            TimeRange object marked with the node it was paid on. These are written
            to another map, which is read back so that the user can be shown
            visually that payments for a given merchant were handled, for any given
            moment in time, by the same node.
        """.trimIndent()
    }

    override val pipeline = Pipeline.create().apply {
        readFrom(source).map { it.value }
            .groupingKey { receipt: PaymentReceipt -> receipt.merchantId }
            .aggregate(AggregateOperations.toList())
            .map { (merchantId, receipts: Iterable<PaymentReceipt>) ->
                // Create a list of (paidOnMember, timePaid) tuples
                merchantId to receipts.sorted().map { receipt ->
                    TimeRange(fontEtched(receipt.onMember), receipt.timePaid)
                }
            }.writeTo(
                Sinks.map(
                    AppConfig.paymentOnOneNodeCheckMap,
                    { it.first },
                    { it.second })
            )
    }
}
