import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.zip
import org.hazelcast.jetpayments.*
import kotlin.math.absoluteValue
import kotlin.test.Test
import kotlin.test.fail
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class PaymentsTest {
    private val logger = ElapsedTimeLogger("PaymentsTest")
    private val context = Dispatchers.Default + SupervisorJob()

    @Test
    fun `test runPaymentsFlow with different test periods`() = runBlocking(context) {
        val numTests = 100
        val maxCycles = 3
        // val numFailureCyclesList = List(numTests) { 0 }
        val numFailureCyclesList = List(numTests) { index ->
            index.mod(maxCycles) + 1
        }

        fun showNthTest(testNum: Int) = TextBox(List(numTests) { index ->
            val testStr = "TEST $index: ${numFailureCyclesList[index]} failure cycles"
            if (index == testNum) "run-> $testStr <-now"
            else testStr
        }).center().addBorder().log(logger)

        PaymentsRun().use { paymentsRun ->
            repeat(numTests) { testNum ->
                Epoch.reset()
                showNthTest(testNum)
                paymentsRun.run(numFailureCyclesList[testNum])
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `test KafkaCluster to ensure re-readability`() = runBlocking(context) {
        val numPayments = 2000
        val consumerGroup = AppConfig.kafkaVerifyPaymentsCG.uniqify()

        KafkaCluster<Int, String>(
            AppConfig.kafkaTopicName.uniqify(), 0
        ).use { kafka ->
            val checkFlow = produce(
                context = Dispatchers.IO, capacity = Channel.UNLIMITED
            ) {
                PaymentGenerator(seededRandom).newPaymentRequestSeq().take(numPayments)
                    .forEach { pmtreq ->
                        kafka.publish(pmtreq.toJsonString())
                        send(pmtreq)
                    }
                logger.log("Finished publishing $numPayments payments to Kafka topic ${AppConfig.kafkaTopicName}")
            }.consumeAsFlow()

            logger.log("Consuming $numPayments payments from Kafka topic ${AppConfig.kafkaTopicName}")

            var numChecked = 0
            kafka.consume(consumerGroup) { it.toPaymentRequest() }.take(numPayments)
                .zip(checkFlow) { pmtReq, checkPmtReq ->
                    if (++numChecked % 250 == 0) {
                        logger.log("Checked $numChecked/$numPayments payments")
                    }
                    pmtReq to checkPmtReq
                }.filter { (pmtReq, checkPmtReq) -> pmtReq != checkPmtReq }
                .collect { (pmtReq, checkPmtReq) ->
                    fail("Payment mismatch: $pmtReq $checkPmtReq")
                }
            logger.log("Finished consuming $numPayments payments from Kafka topic ${AppConfig.kafkaTopicName}")
        }

        logger.log("Validated all payments")
    }

    @Test
    fun `test creation of Canvas Timescales`() = runBlocking(context) {
        val canvasSizeSeq = generateSequence(54) { it + 30 }.takeWhile { it <= 144 }
        val merchantMap =
            MerchantGenerator(AppConfig.numMerchants * 2, seededRandom).merchantMap

        /*
         * Outer loop: Try out different canvas sizes, from
         * smaller to larger, up to the maximum of 196...
         */
        canvasSizeSeq.forEach { canvasSize ->
            /*
             * Inner loop: ...for each canvas size, loop through a set of
             * timescales that we can easily reasonably fit to that canvas
             * size. For each time scale, generate a Canvas and render it.
             */
            generateSequence(1.0) { it + 0.5 }.takeWhile { it <= 2.0 }
                .forEach { scaleFactor ->
                    val totalTime = (canvasSize * scaleFactor).seconds
                    val unit = DurationUnit.MILLISECONDS
                    val minTime = 0.seconds
                    val maxTime = minTime + totalTime
                    val maxRanges = canvasSize / 16
                    fun randomTime() = seededRandom.nextLong(
                        minTime.toLong(unit), maxTime.toLong(unit)
                    ).toDuration(unit)

                    buildList {
                        add("canvasSize: $canvasSize")
                        add("maxRanges: $maxRanges")
                        add("maxTime: ${maxTime.toWholeSecStr()}")
                    }.let { lines ->
                        TextBox(lines).addBorder().log(logger)
                    }

                    /*
                     * Generate a TimeSpan (set of TimeRanges) for each Merchant.
                     * These will be used collectively to create a Canvas.
                     */
                    val timeSpans = merchantMap.values.map { merchant ->
                        val marker = merchant.name.first().toString()
                        // Choose a random number of ranges.
                        val numRanges = seededRandom.nextInt(2, maxRanges + 1)

                        /*
                         * For each range, we need two numbers--the boundaries of the
                         * range. Generate numRanges * 2 random numbers, sort them,
                         * and we'll select the ranges pairwise from the sorted list.
                         */
                        val boundaries = List(numRanges * 2) { randomTime() }

                        val timeRanges = boundaries.sorted().chunked(2)
                            .map { (start, endInclusive) ->
                                TimeRange(marker, start, endInclusive)
                            }

                        val prefix = "${merchant.name}: ${timeRanges.size} ranges"
                        TimeSpan(prefix, timeRanges)
                    }

                    val canvas = Canvas(timeSpans.sorted(), canvasSize)
                    val canvasLines = canvas.draw()

                    /*
                     * Render the Canvas, and check that all lines are the
                     * same length.
                     */
                    assert(canvasLines.map { it.length }
                        .all { it == canvasLines.first().length }) { "Canvas row lengths differ" }
                    canvasLines.log(logger)
                }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `test merchant distribution verification logic`() = runBlocking(context) {
        val numPayments = 100
        val clusterSize = AppConfig.clusterSize
        val runLength = numPayments / clusterSize / 2

        val rotationSeq = sequence {
            while (true) {
                repeat(clusterSize) { i ->
                    repeat(runLength) { yield(i) }
                }
            }
        }

        val client = HzCluster("dev", AppConfig.clusterSize).getClient()
        val merchantMap =
            MerchantGenerator(AppConfig.numMerchants, seededRandom).merchantMap
        TextBox(listOf("MERCHANTS:") + merchantMap.keys).center().addBorder()
            .log(logger)
        val paymentSeq =
            PaymentGenerator(seededRandom, merchantMap).newPaymentRequestSeq()
        val paymentReceiptMap =
            client.getMap<Int, PaymentReceipt>(AppConfig.paymentReceiptMapName)

        logger.log("Populating receipt map")
        val requestChannel = produce(
            context = Dispatchers.IO, capacity = Channel.UNLIMITED
        ) {
            (paymentSeq zip rotationSeq).take(
                numPayments
            ).forEach { (payreq, rotate) ->
                val paidOnMember =
                    (payreq.merchantId.hashCode() + rotate).absoluteValue % clusterSize
                val receipt = PaymentReceipt(payreq, true, paidOnMember)
                paymentReceiptMap.put(receipt.paymentId, receipt)
                send(payreq)
            }
        }

        while (paymentReceiptMap.size < numPayments) {
            delay(250.microseconds)
        }

        /*
         * Now run our Jet job to process down the payment receipts.
         */
        logger.log("Running PaymentMemberCheckPipeline")
        PaymentMemberCheckPipeline(client).use { it.run() }

        val timeRangeIteratorByMerchant =
            client.getMap<String, List<TimeRange>>(AppConfig.paymentOnOneNodeCheckMapName)
                .mapValues { (_, timeRanges) ->
                    timeRanges.iterator()
                }

        logger.log("Checking that all timeRanges match")
        var numChecked = 0
        for (payreq in requestChannel) {
            if (numChecked % 10 == 0) logger.log("Checked $numChecked/$numPayments payments")
            assert(timeRangeIteratorByMerchant.containsKey(payreq.merchantId)) { "merchantId ${payreq.merchantId} not found in map" }
            val iter = timeRangeIteratorByMerchant[payreq.merchantId]!!
            assert(iter.hasNext()) { "no more time ranges for merchant ${payreq.merchantId}" }
            val timeRange = iter.next()
            val receipt = paymentReceiptMap[payreq.paymentId]!!
            assert(receipt.onMember.toString() == timeRange.marker) {
                "paidOnMember mismatch ${receipt.onMember} != ${timeRange.marker} for $payreq"
            }
            assert(timeRange.start == timeRange.endInclusive) { "start/endInclusive mismatch $timeRange for $payreq" }
            assert(timeRange.start == receipt.timePaid) { "timePaid mismatch $timeRange for $receipt" }
            numChecked++
        }

        logger.log("Validated all payments")
    }

    @Test
    fun `test TallyField generate function`() = runBlocking(context) {
        val merchant = MerchantGenerator(
            1, seededRandom
        ).merchantMap.values.first().shortName
        val tallySeq = generateSequence(0) { i -> i + 1 }.map { i ->
            ReceiptWatcher.NodeTally(11 + i * 10, i)
        }
        val metricsSummary = "[9999 QUD ⇒ 9999 WKG ⇒ 9999 FIN]➣"
        fun generateLogLine(fields: Iterable<String>): String =
            "$metricsSummary⟦ ${fields.joinToString(" | ")} ⟧"

        val numFields = AppConfig.numMerchants
        val prefixLen = 20
        val emptyFieldLine = generateLogLine(List(numFields) { "" })
        val fieldWidth =
            (AppConfig.screenWidth - emptyFieldLine.numCodepoints() - prefixLen) / numFields
        TextBox(
            listOf("width=$fieldWidth"), borderStyle = TextBox.BorderStyle.DOUBLE
        ).log(logger)
        for (numNodes in 1..9) {
            val tally = tallySeq.take(numNodes).toList()
            val fields = List(numFields) {
                ReceiptWatcher.TallyField(fieldWidth, merchant, tally).generate()
            }
            logger.log(generateLogLine(fields))
        }
    }
}
