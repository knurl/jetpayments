package org.hazelcast.jetpayments

import com.hazelcast.core.EntryEvent
import com.hazelcast.map.IMap
import com.hazelcast.map.listener.EntryAddedListener
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.RENDEZVOUS
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.*
import kotlin.time.Duration

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
    merchantMap: Map<String, Merchant>,
    private val reportFrequency: Duration = AppConfig.reportFrequency,
    private val getNumIssued: () -> Int,
) : AutoCloseable {
    private val logger = ElapsedTimeLogger("Watcher")
    private val paymentReceiptMap =
        client.getMap<Int, PaymentReceipt>(AppConfig.paymentReceiptMapName)

    // Public state that reveals which nodes are currently processing receipts.
    val nodesInUse: MutableStateFlow<Set<Int>> = MutableStateFlow(emptySet())

    /*
     * Use an actor approach for processing messages and modifying state. Only the
     * actor gets to modify the state encapsulated within this inner class, which
     * avoids mutable shared state. Since the event listener on the PaymentReceipt
     * map has to tell us about new receipts, it communicates this to the actor via
     * the ReceiptMessage sealed class.
     */
    private class ReceiptsActor(
        private val numPayments: Int, private val merchantMap: Map<String, Merchant>,
    ): Actor<ReceiptsActor.Message>() {
        private val receiptSummary = ReceiptSummary()
        private var attemptingRebuild = false

        // Used when we summarize the state of the ReceiptSummary map to callers
        data class Summary(
            val numReceipts: Int = 0,
            val nodesInUse: Set<Int> = emptySet(),
            val summaryString: String = "",
        )

        /*
         * Data structures needed to communicate with the ReceiptsActor.
         */
        sealed class Message { // input
            class Update(val receipt: PaymentReceipt) : Message()

            class Summarize(
                val numIssued: Int, val replyFlow: MutableSharedFlow<Summary>,
            ) : Message()

            class Rebuild(
                val paymentReceiptMap: IMap<Int, PaymentReceipt>,
                val replyChannel: SendChannel<Int>,
            ) : Message()
        }

        /*
         * Methods to perform input/output to Actor from outside
         */
        override suspend fun processMessage(message: Message) {
            when (message) {
                is Message.Update -> {
                    if (!attemptingRebuild) {
                        receiptSummary.addReceipt(message.receipt)
                    }
                }

                is Message.Summarize -> {
                    val summary = summarize(message.numIssued)
                    message.replyFlow.emit(summary)
                }

                is Message.Rebuild -> {
                    attemptingRebuild = true
                    val response =
                        receiptSummary.rebuildFromMap(message.paymentReceiptMap)
                    message.replyChannel.send(response)
                }
            }
        }

        private fun summarize(numIssued: Int): Summary {
            val numReceipts = receiptSummary.numReceipts
            val widest = numPayments.toString().length // numPayments is biggest
            val metrics = mapOf(
                "QUD" to numPayments - numIssued,
                "WKG" to numIssued - numReceipts,
                "FIN" to numReceipts,
            ).entries.joinToString(" ⇒ ") { (name, value) ->
                "${value.toString().padStart(widest, '0')} $name"
            }
            val metricsSummary = "[$metrics]➣"
            fun generateLogLine(fields: Iterable<String>): String =
                "$metricsSummary⟦ ${fields.joinToString(" | ")} ⟧"

            val numMerchants = merchantMap.size
            val minimal = generateLogLine(List(numMerchants) { "" })
            val availableSpace = AppConfig.displayWidth - minimal.numCodepoints()
            val fieldWidth = availableSpace / numMerchants

            /*
             * Now show, for each merchant, a list of all the payments grouped
             * chronologically by the node on which payments were processed, and how
             * many were processed on that node before another node was selected.
             */
            val merchantFields = merchantMap.values.map { merchant ->
                val tally = receiptSummary.getTallyForMerchant(merchant.id)
                TallyField(fieldWidth, merchant.shortName, tally).generate()
            }

            return Summary(
                receiptSummary.numReceipts,
                receiptSummary.nodesInUse,
                generateLogLine(merchantFields)
            )
        }
    } // end of class ReceiptsActor

    private val receiptsActor = ReceiptsActor(numPayments, merchantMap)

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob()).apply {
        receiptsActor.start(this)
    }

    /*
     * Hazelcast EntryAddedListener (which derives MapListener) object. Hazelcast
     * will call into the entryAdded callback method, and that method therefore
     * needs to completely very quickly and reliably, and cannot suspend. There will
     * also be multiple simulaneous calls into entryAdded(), so we use a Channel
     * which is thread-safe.
     */
    private val listenerUuid = paymentReceiptMap.addEntryListener(
        object : EntryAddedListener<Int, PaymentReceipt> {
            override fun entryAdded(event: EntryEvent<Int, PaymentReceipt>) {
                receiptsActor.trySend(ReceiptsActor.Message.Update(event.value))
            }
        }, true
    )

    override fun close() {
        paymentReceiptMap.removeEntryListener(listenerUuid)
        scope.cancel()
    }

    private suspend fun rebuildFromReceiptMap(numReceipts: Int) {
        val numLost = numPayments - numReceipts
        logger.log(italic("$numLost EntryAdded events lost during cluster topology change -> rebuilding from paymentReceiptMap"))
        Channel<Int>(RENDEZVOUS).let { reply ->
            receiptsActor.send(
                ReceiptsActor.Message.Rebuild(paymentReceiptMap, reply)
            )
            val numReceiptsFoundInMap = reply.receive()
            check(numReceiptsFoundInMap == numPayments) { "After rebuild, expected $numPayments receipts, found $numReceiptsFoundInMap" }
            reply.close()
        }
    }

    /*
     * The outFlow we pass into the ReceiptsActor to receive Summary replies. This must
     * be a MutableSharedFlow, rather than a MutableStateFlow, as we need our listeners
     * to be notified even if someone sends the _same_ state to the flow.
     * MutableStateFlow only notifies listeners when the state changes.
     */
    private val outFlow = MutableSharedFlow<ReceiptsActor.Summary>(
        replay = 0, // Only one listener
        extraBufferCapacity = 1, // We only need latest version
        onBufferOverflow = BufferOverflow.DROP_OLDEST, // Sender should never block
    )

    /*
     * Used below to keep track of the most-recent, and 2nd-most-recent
     * captures of the number of receipts; we want to compare these to
     * see if they're still changing. For convenience we also track:
     * nodesInUse, so we can update this value in the surrounding class;
     * and summaryString, for periodic display to the ReceiptWatcher log
     */
    private data class SummaryTracker(
        val lastNumReceipts: Int = 0,
        val numReceipts: Int = 0,
        val nodesInUse: Set<Int> = emptySet(),
        val summaryString: String = "",
    ) {
        fun noProgress(): Boolean {
            return lastNumReceipts != 0 && lastNumReceipts == numReceipts
        }
    }

    suspend fun startReceiptsLog(): Unit = coroutineScope {
        /* The drumbeat that drives the activity. Send a Summarize request to the actor
         * on a periodic basis, providing our output flow for replies.
         */
        launch {
            while (isActive) {
                val msg = ReceiptsActor.Message.Summarize(getNumIssued(), outFlow)
                receiptsActor.send(msg)
                delay(reportFrequency)
            }
        }

        /* Keep track of the 2nd-latest and latest reports of the number of receipts in
         * the payments summary map, using a scan() function that traverses the output
         * from the receiptsActor.
         */
        outFlow.scan(SummaryTracker()) { tracker, next ->
            SummaryTracker(
                lastNumReceipts = tracker.numReceipts, // old current value -> previous
                numReceipts = next.numReceipts, // set the current value
                nodesInUse = next.nodesInUse,
                summaryString = next.summaryString,
            ) // NOTE: Always drop() 1st value from scan(), which is the initial value.
        }.drop(1).onEach { tracker ->
            nodesInUse.value = tracker.nodesInUse
            logger.log(tracker.summaryString)
        }.takeWhile { tracker ->
            tracker.numReceipts < numPayments
        }.onCompletion {
            this@coroutineScope.cancel()
        }.filter { it.noProgress() }.collect { tracker ->
            /* The receipt count isn't advancing, which has likely happened because
             * we've missed some MapListener events (they're not guaranteed during
             * cluster topology changes). If all payments have been now been issued,
             * force a rebuild of our data from the source map.
             */
            if (getNumIssued() >= numPayments) {
                rebuildFromReceiptMap(tracker.numReceipts)
            }
        }
    }
}