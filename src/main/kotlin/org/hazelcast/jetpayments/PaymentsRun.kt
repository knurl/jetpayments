package org.hazelcast.jetpayments

import com.hazelcast.collection.IList
import com.hazelcast.map.IMap
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.take
import kotlin.time.Duration.Companion.milliseconds

/*
 * This class represents the heart of the operation. It handles the execution of a
 * single payments run, of a certain number of payments. It orchestrates the
 * creation of resources, the execution of the payments run including the lifecycle
 * of the FailureSimulator and the ReceiptWatcher, the setup of Kafka and the
 * execution of the various Jet jobs, the verification of the results, and
 * ultimately the rendering of the results to ASCII-art via the Canvas class.
 */
class PaymentsRun() : AutoCloseable {
    private val logger = ElapsedTimeLogger("PaymentsRun")
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    // Cleanup for above
    override fun close() {
        scope.cancel()
    }

    /*
     * Get the Hazelcast cluster and client started up. We'll await() the creation
     * of the cluster and client later in the code, when we need them.
     */
    private val clusterSize = AppConfig.clusterSize
    private val hzClientDeferred = scope.async {
        HzCluster("dev", clusterSize).getClient()
    }

    // Spin up Kafka
    private val kafkaTopicName = AppConfig.kafkaTopicName.uniqify()
    private val kafka = KafkaCluster<Int, String>(kafkaTopicName, 0)
    private val kafkaProcessCG = AppConfig.kafkaProcessPaymentsCG.uniqify()
    private val kafkaVerifyCG = AppConfig.kafkaVerifyPaymentsCG.uniqify()

    // Create the list of merchants we'll use throughout the payment runs.
    private val merchantMap =
        MerchantGenerator(AppConfig.numMerchants, seededRandom).merchantMap

    // This creates a flow with a series of randomly-generated payments.
    private var paymentRequestFlow =
        PaymentGenerator(seededRandom, merchantMap).newPaymentRequestFlow()

    /*
     * PaymentsFlowState encapsulates the objects that we'll need to track the
     * progress of a single payments run. This class needs to be cleared on every
     * payments run.
     */
    private class PaymentRunState(
        client: HzCluster.ClientInstance,
        merchantMap: Map<String, MerchantGenerator.Merchant>
    ) {
        val numPaymentsReceived = MutableStateFlow(0)
        val receiptMap =
            client.getMap<Int, PaymentReceipt>(AppConfig.paymentReceiptMapName)
        val successCheckList =
            client.getList<TimeRange>(AppConfig.paymentProcessedCheckList)
        val paymentOnOneNodeCheckMap =
            client.getMap<String, List<TimeRange>>(AppConfig.paymentOnOneNodeCheckMap)
        val receiptWatcher = ReceiptWatcher(
            receiptMap, merchantMap, AppConfig.screenWidth
        ) { numPaymentsReceived.value }
        val failureSimulator: FailureSimulator =
            FailureSimulator(client, receiptWatcher)

        fun clear(numPayments: Int) {
            numPaymentsReceived.value = 0
            receiptMap.clear()
            successCheckList.clear()
            paymentOnOneNodeCheckMap.clear()
            receiptWatcher.clear(numPayments)
            failureSimulator.clear()
        }
    }

    private val paymentRunStateDeferred = scope.async {
        PaymentRunState(hzClientDeferred.await(), merchantMap)
    }

    // Take the payments from our payments generator, and publish them to Kafka.
    private suspend fun issuePaymentsToKafka(
        numPayments: Int, numPaymentsReceived: MutableStateFlow<Int>
    ) {
        paymentRequestFlow.take(numPayments).collect { paymentReq ->
            // Timestamp and publish each payment, and delay afterward
            val stamped = paymentReq.copy(timeIssued = Epoch.elapsed())
            kafka.publish(stamped.toJsonString())
            // Simulate the real-time delays you'd have between payments IRL.
            delay(paymentRequestDelayNext())
            numPaymentsReceived.value++
        }
        paymentRequestFlow = paymentRequestFlow.drop(numPayments)
    }

    // Show that everything got paid correctly (and some stats).
    private suspend fun verifyPayments(
        paymentRunState: PaymentRunState, numPayments: Int
    ) {
        // Validate that every payment was processed correctly.
        val paid = showAllPaymentRequestsPaid(
            paymentRunState.successCheckList, numPayments
        )

        // Show that only one node processed a given merchant at one time
        val byMerchant = showPaymentsDistributedByMerchant(
            paymentRunState.paymentOnOneNodeCheckMap
        )

        // Show when we had nodes down or up
        val uptime = paymentRunState.failureSimulator.getTimeSpans()

        // Draw all the timespans
        Canvas(paid + byMerchant + uptime).draw().log(logger)
        // Show some useful statistics
        paymentRunState.failureSimulator.getFailureStats()
            .logBoxed(logger, center = true)
    }

    /* Show that each and every payment request resulted in a receipt, meaning that
     * it was paid by payment processing service.
     */
    private suspend fun showAllPaymentRequestsPaid(
        paymentProcessedCheckList: IList<TimeRange>, numPayments: Int
    ): List<TimeSpan> {

        /* Step I: Run a streaming Jet job to replay everything from Kafka, and
         * check we have a receipt. For each receipt, put a TimeRange into our
         * IList.
         */
        val jobName = AppConfig.paymentProcessedCheckJetJobName.uniqify()
        val source = kafka.consumerJetSource(kafkaVerifyCG)
        PaymentProcessedCheckPipeline(
            hzClientDeferred.await(), jobName, source, numPayments
        ).use { it.runUntilCompleted() }

        /* Step II: Sort and fold the contiguous TimeRanges, coalescing into longer
         * ranges except where they differ in terms of payment success. Partition
         * into "succeeded" and "failed" lists. If things worked as planned, we'll
         * only have the former, with a single TimeRange inside.
         */
        val sortedTimeRanges = paymentProcessedCheckList.sorted()
        val (succeeded, failed) = foldTimeRanges(sortedTimeRanges).partition { timeRange ->
            timeRange.marker == "✓"
        }
        assert(AppConfig.simulatePaymentProcessorFailure || (succeeded.size == 1 && failed.isEmpty())) {
            "Expected 1 successful range, got success=$succeeded, failed=$failed"
        }

        /* Step III: Convert our successful and failed TimeRange groups into
         * TimeSpans, for later conversion into a Canvas for display.
         */
        return buildList {
            add("✓ PAID" to succeeded)
            add("× FAIL" to failed)
        }.filter { it.second.isNotEmpty() }.map { (prefix, timeRanges) ->
            TimeSpan(prefix, timeRanges)
        }
    }

    /* Show that each merchant was always assigned a particular node any given moment--and that
     * in failure/recovery situations, that node would automatically be reassigned.
     */
    private suspend fun showPaymentsDistributedByMerchant(
        paymentMemberDistributionCheckMap: IMap<String, List<TimeRange>>
    ): List<TimeSpan> {

        /* Step I: Run a Jet job to show, for each merchant, on which members
         * their payments were paid at any given moment. The resulting map that
         * is created is keyed off the merchantId, and the values are lists of
         * 0-width TimeRanges with the marker being the node on which the payment
         * was processed.
         */
        PaymentMemberCheckPipeline(
            hzClientDeferred.await(),
            AppConfig.paymentDistCheckJetJobName.uniqify(),
            AppConfig.paymentReceiptMapName,
        ).use { it.run() /* Batch; will run until completion. */ }

        /* Step II: Collect the TimeRanges for each merchant back from the jet job
         * and fold the contiguous TimeRanges together, separating by the node on
         * which the payments were made. For each merchant, we should see that none
         * of the TimeRanges should overlap, because that would indicate a case
         * where the same merchant was being serviced from two different nodes. */
        val timeRangesByMerchant =
            paymentMemberDistributionCheckMap.mapValues { (merchant, timeRanges) ->
                foldTimeRanges(timeRanges).also {
                    assert(!doRangesOverlap(it)) {
                        "Expected TimeRanges for $merchant to be non-overlapping, got $it"
                    }
                }
            }

        /* Step III: Convert the list of TimeRanges for each merchant to a TimeSpan,
         * and return them so they can be represented on a Canvas.
         */
        return timeRangesByMerchant.map { (merchantName, timeRanges) ->
            val prefix = merchantMap[merchantName]!!.name
            TimeSpan(prefix, timeRanges)
        }.sorted() // sort TimeSpans by start time
    }

    // Display our key parameters at the start of this payments run.
    private fun showPaymentRunParameters(
        numFailureCycles: Int, numPayments: Int
    ) {
        // allow time for the receipts to catch up at the end and be processed
        val meanIssueDelay = AppConfig.paymentRequestDelayRand.mean.milliseconds
        val paymentIssuePeriod = meanIssueDelay * numPayments
        val clusterMembers = (0 until clusterSize).joinToString { fontEtched(it) }

        logBoxed(logger, boxStyle = Logger.BoxStyle.THICK) {
            add("Hazelcast cluster nodes -> $clusterMembers")
            add("Payment issue -> $numPayments payments in ~$paymentIssuePeriod")
            mapOf(
                "request" to AppConfig.paymentRequestDelayRand,
                "processing" to AppConfig.paymentProcessingDelayRand,
            ).map { (prefix, rand) ->
                "$prefix delay -> µ=${rand.mean.milliseconds} σ=${rand.stddev.milliseconds}"
            }.also { addAll(it) }
            add("")

            add("FAILURE SIMULATOR ACTIONS:")
            buildList {
                add("${AppConfig.warmupTime} ")
                repeat(numFailureCycles) {
                    add("${(AppConfig.failureCycleTime)} ↓↑ ")
                }
                add("${AppConfig.cooldownTime} ")
            }.joinToString(" -> ").also { add(it) }
            add("")

            add("OUR MERCHANTS:")
            val maxWidth = merchantMap.values.maxOf { it.name.length }
            merchantMap.values.map { merchant ->
                "${merchant.name.padEnd(maxWidth)} \"${merchant.shortName}\": ${merchant.id}"
            }.also { addAll(it) }
        }
    }

    /* PaymentsRun entry point. Do the payments run for the given number of
     * payments, and afterwards, verify the payments were processed correctly.
     */
    internal suspend fun run(numFailureCycles: Int) =
        withContext(Dispatchers.Default) {
            val numPayments = calculateNumPayments(numFailureCycles)
            showPaymentRunParameters(numFailureCycles, numPayments)
            val paymentRunState = paymentRunStateDeferred.await()
            paymentRunState.clear(numPayments) // always clear state after every run

            launch {
                issuePaymentsToKafka(
                    numPayments, paymentRunState.numPaymentsReceived
                )
            }

            /* This Jet pipeline will take payments from Kafka, distribute them
             * across nodes in the Hazelcast cluster according to their merchant,
             * and then each of those nodes will process the payments.
             */
            PaymentsJetPipeline(
                hzClientDeferred.await(), // use the client to create Jet job
                AppConfig.paymentProcessingJetJobName.uniqify(),
                kafka.consumerJetSource(kafkaProcessCG),
                numPayments
            ).use { pipeline -> // like Java's try-with-resources
                launch { pipeline.run() } // Start the Jet streaming pipeline.

                with(paymentRunState) {
                    coroutineScope {
                        launch { failureSimulator.runSimulations(pipeline) }
                        launch { receiptWatcher.logReceiptSummary(numPayments) }
                    } // Blocks here until launched coroutines in scope are done.
                }
            } // use() ends -> calls close() on pipeline, which terminates pipeline job

            // Verify that all payments were processed correctly.
            verifyPayments(paymentRunState, numPayments)
        }
}
