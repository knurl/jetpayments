package org.hazelcast.jetpayments

import com.hazelcast.core.EntryEvent
import com.hazelcast.map.listener.EntryAddedListener
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/*
 * This class observes the creation of PaymentReceipts, and summarizes these and
 * periodically logs that summary to the log. Additionally, since it's already
 * tracking which nodes are processing the payments, it also keeps track of those
 * for the FailureSimulator so it knows which knows it should bring down and up for
 * maximum visible impact.
 *
 * For each merchant the summary shows the nodes on which the merchant's payment
 * requests were processed, in order. So if, for example, the FailureSimulator
 * brings down node 1, and merchant X's payments were being processed on node 1 but
 * now must be rescheduled to node 2, the ReceiptWatcher will track that as "1->2",
 * showing that the payments were all being done on node 1, but now are being done
 * on node 2.
 *
 * This class creates a Hazelcast entry listener on the payment receipt map, that
 * continuously reports on new insertions to that map. These are transmitted to a
 * coroutine managed by this class, which puts them into a summary map of payment
 * receipts, organized in a way that is optimal for generating the summary shown
 * above. The receipts are summarized by merchant, and for each merchant, a list of
 * nodes that receipts were processed on, in order. We keep track of the nodes used
 * to process receipts, and the count processed on each node, in time order. This
 * makes it easy to generate the summary.
 */
class ReceiptWatcher(
    client: HzCluster.ClientInstance,
    private val numPayments: Int,
    private val merchantMap: Map<String, Merchant>,
    private val getNumIssued: () -> Int,
) {
    private val logger = ElapsedTimeLogger("Watcher")
    private val paymentReceiptMap =
        client.getMap<Int, PaymentReceipt>(AppConfig.paymentReceiptMapName)
    private val screenWidth: Int = AppConfig.screenWidth

    // Data class that tracks the number of receipts processed on a particular node.
    data class NodeTally(val numProcessed: Int, val onMember: Int)

    // Public state that reveals which nodes are currently processing receipts.
    val nodesInUse = MutableStateFlow<List<Int>>(emptyList())

    /*
     * This is a specialized class used to render the per-merchant fields by the
     * ReceiptWatcher. For each merchant, we want to show the number of _consecutive_
     * payments processed for that merchant on a specific node. The TallyField class
     * takes care of rendering this information in a compact way, so that the user can
     * see the number of payments processed by for a merchant on each node in time
     * order.
     *
     * The class is initialized with the per-merchant value from the payment receipt
     * summary map tracked by the ReceiptWatcher.
     */
    class TallyField(
        private val width: Int,
        private val merchant: String,
        private val tally: List<NodeTally>,
    ) {
        init {
            require(width >= merchant.numCodepoints() + 2)
        }

        /*
         * This is the public point of access for the class. It generates the string
         * representation for the per-merchant field.
         */
        fun generate(): String {
            val merchant = "${merchant}▹"
            val widthLeft = width - merchant.numCodepoints()
            val allButLast = tally.dropLast(1).map {
                "${fontEtched(it.onMember)}×${it.numProcessed}"
            }
            val last = tally.lastOrNull()?.let {
                "${fontEtched(it.onMember)}×${underline(it.numProcessed.toString())}"
            } ?: "NONE YET"
            val ticker = (allButLast + last).joinToString(" → ")
            return if (ticker.numCodepoints() > widthLeft) { // too big for field
                "$merchant⋯${ticker.trimStart(widthLeft - 1)}"
            } else {
                "$merchant${ticker.padStart(widthLeft)}"
            }
        }
    }

    /*
     * Data structures needed to communicate with the ReceiptsActor, below. ActorMessage
     * is the input format; Summary is the output format.
     */
    sealed class ActorMessage { // input
        class Update(val receipt: PaymentReceipt) : ActorMessage()
        class Summarize() : ActorMessage()
        class Rebuild() : ActorMessage()
    }
    data class Summary( val numReceipts: Int, val receiptSummary: String)

    /*
     * Use an actor approach for processing messages and modifying state. Only the
     * actor gets to modify the state encapsulated within this inner class, which
     * avoids mutable shared state. Since the event listener on the PaymentReceipt
     * map has to tell us about new receipts, it communicates this to the actor via
     * the ReceiptMessage sealed class.
     */
    private inner class ReceiptsActor() : AutoCloseable {
        /*
         * Scope used to start up our outFlow SharedFlow, which also collects our
         * inFlow. Therefore, this scope ends up governing both.
         */
        private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

        /*
         * This is where receipts are summarized, in a way that makes it easy to
         * report. We basically group by merchant, and for each merchant, we have a
         * series of NodeTally objects, one for each node that processed a receipt
         * for that merchant. Everything is stored in time order, so if there is a
         * topology change, then we'll see a switch, in some cases, from processing
         * receipts on one node to processing them on another.
         */
        private val receiptSummaryMap = sortedMapOf<String, List<NodeTally>>()
        private var numReceipts = 0 // How many receipts have we summarized?

        // Keep track of the last node that processed each merchant.
        private val nodeLastUsedForMerchant = sortedMapOf<String, Int>()

        // This method clears all of the state listed above.
        private fun reset() {
            receiptSummaryMap.clear()
            numReceipts = 0
            nodeLastUsedForMerchant.clear()
        }

        /* Process each new receipt into our map, so that the map tracks, for each
         * merchant, a list of nodes that receipts were paid on, in time order. This
         * function is only ever called sequentially by the single-threaded actor, so we
         * can modify the map, and count receipts, without using a mutex.
         */
        private fun mergeNewReceipt(receipt: PaymentReceipt) {
            val tally = receiptSummaryMap.getOrPut(receipt.merchantId) { listOf() }
            val last = tally.lastOrNull()
            val newTally = if (last?.onMember == receipt.onMember) {
                tally.dropLast(1) + NodeTally(last.numProcessed + 1, receipt.onMember)
            } else {
                // Add a new pair if tally is empty or the node is different
                nodeLastUsedForMerchant[receipt.merchantId] = receipt.onMember
                tally + NodeTally(1, receipt.onMember)
            }
            receiptSummaryMap[receipt.merchantId] = newTally
            numReceipts++
        }

        /*
         * This function is only ever called, like the previous one, by the actor,
         * so the mutable state isn't shared, and we don't need a mutex.
         */
        private fun summarize(): Summary {
            // Update the nodes in use, so our failure simulator can target those
            nodesInUse.value = nodeLastUsedForMerchant.values.toList()

            // Capture snapshot of changing values from Kotlin mutableStateFlows
            val metricsSummary = getMetricsSummary(getNumIssued())
            val numFields = merchantMap.size

            fun generateLogLine(fields: Iterable<String>): String =
                "$metricsSummary⟦ ${fields.joinToString(" | ")} ⟧"

            val prefixLen = 20
            val emptyFieldLine = generateLogLine(List(numFields) { "" })
            val fieldWidth =
                (screenWidth - emptyFieldLine.numCodepoints() - prefixLen) / numFields

            /* Now show, for each merchant, a tally of number of payments, and on which
             * node those payments were made on.
             */
            val merchantFields = merchantMap.values.map { merchant ->
                val tally = receiptSummaryMap[merchant.id] ?: emptyList()
                TallyField(fieldWidth, merchant.shortName, tally).generate()
            }

            return Summary(numReceipts, generateLogLine(merchantFields))
        }

        /*
         * Generate the initial part of the receipt summary, consisting of rolling
         * metrics. QUD = queued payments, WKG = in-process payments, FIN = finished
         * payments.
         */
        private fun getMetricsSummary(numReceived: Int) = mapOf(
            "QUD" to numPayments - numReceived,
            "WKG" to numReceived - numReceipts,
            "FIN" to numReceipts,
        ).entries.joinToString(" ⇒ ") { (metricName, metricValue) ->
            val paddedMetric =
                metricValue.toString().padStart(numPayments.toString().length, '0')
            "$paddedMetric $metricName"
        }.let { "[$it]➣" }

        /*
         * Finally, the following two Kotlin StateFlows are used to communicate with
         * the actor. The inFlow is used to send messages to the actor, and the outFlow
         * is used to receive the results of processing messages.
         */
        private fun rebuild() {
            reset()
            paymentReceiptMap.values.groupBy { it.merchantId }
                .mapValues { (_, receipts) ->
                    receipts.sorted().map { receipt ->
                        NodeTally(1, receipt.onMember)
                    }.combine({ tally1, tally2 ->
                        tally1.onMember == tally2.onMember
                    }, { tally1, tally2 ->
                        tally1.copy(numProcessed = tally1.numProcessed + tally2.numProcessed)
                    })
                }.forEach { (merchant, tally) ->
                    receiptSummaryMap[merchant] = tally
                    nodeLastUsedForMerchant[merchant] = tally.last().onMember
                    numReceipts += tally.sumOf { it.numProcessed }
                }
        }

        /*
         * Hazelcast EntryAddedListener (which derives MapListener) object. Hazelcast
         * will call into the entryAdded callback method, and that method therefore
         * needs to completely very quickly and reliably, and cannot suspend. There will
         * also be multiple simulaneous calls into entryAdded(), so we use a Channel
         * which is thread-safe.
         */
        private val listenerUuid = paymentReceiptMap.addEntryListener(object :
            EntryAddedListener<Int, PaymentReceipt> {
            override fun entryAdded(event: EntryEvent<Int, PaymentReceipt>) {
                inFlow.trySend(ActorMessage.Update(event.value))
            }
        }, true)

        private val inFlow = Channel<ActorMessage>(Channel.UNLIMITED)
        val outFlow = Channel<Summary>(Channel.CONFLATED)

        init {
            scope.launch {
                for (message in inFlow) {
                    when (message) {
                        is ActorMessage.Update -> mergeNewReceipt(message.receipt)
                        is ActorMessage.Summarize -> outFlow.send(summarize())
                        is ActorMessage.Rebuild -> rebuild()
                    }
                }
            }
        }

        suspend fun send(message: ActorMessage) = inFlow.send(message)

        override fun close() {
            paymentReceiptMap.removeEntryListener(listenerUuid)
            inFlow.close()
            outFlow.close()
            scope.cancel()
        }
    } // end of class ReceiptsActor()

    class Heartbeat {
        private var lastHeartbeat = TimeSource.Monotonic.markNow()
        fun beat() { lastHeartbeat = TimeSource.Monotonic.markNow() }
        fun hasElapsed(duration: Duration) = lastHeartbeat.elapsedNow() >= duration
    }

    val heartbeat = Heartbeat()

    suspend fun startReceiptsLog() = coroutineScope {
        val heartbeatJob = launch {
            while (true) {
                if (heartbeat.hasElapsed(20.seconds)) {
                    throw IllegalStateException("Heartbeat has expired!")
                }
                delay(5.seconds)
            }
        }

        ReceiptsActor().use { receiptsActor ->
            val summarizeJob = launch {
                while (true) {
                    delay(AppConfig.reportFrequency)
                    receiptsActor.send(ActorMessage.Summarize())
                }
            }

            /* Keep track of the 2nd-latest and latest reports of the number of receipts
             * in the payments summary map, as well as the latest receiptSummary string,
             * using a scan() function that traverses the output from the receiptsActor.
             */
            receiptsActor.outFlow.consumeAsFlow().scan(
                Triple(0, 0, "")
            ) { (_, current, _), (numReceipts, receiptSummary) ->
                Triple(current, numReceipts, receiptSummary)
            }.drop(1).onEach { (lastNumReceipts, numReceipts, receiptSummary) ->
                if (lastNumReceipts != numReceipts) {
                    logger.log(receiptSummary)
                    heartbeat.beat()
                }
            }.takeWhile { (_, numReceipts, _) -> numReceipts < numPayments }
                .onCompletion {
                    summarizeJob.cancel()
                    logger.log("summarizeJob cancelled")
                }.filter { (lastNumReceipts, numReceipts, _) ->
                    lastNumReceipts == numReceipts // no change in reported value
                }.collect {
                    /* If we've gotten here, then the receipt count isn't advancing,
                     * which has happened because we've missed some MapListener events
                     * (they're not guaranteed during cluster topology changes). If all
                     * payments have been issued, indicating the end of the run, force a
                     * rebuild of our data from the source map.
                     */
                    if (getNumIssued() >= numPayments) {
                        logger.log("Rebuilding receipts summary map")
                        receiptsActor.run {
                            send(ActorMessage.Rebuild())
                            send(ActorMessage.Summarize())
                        }
                    }
                }
        }

        heartbeatJob.cancel()

        logger.log("Receipts log job concluded")
    }
}
