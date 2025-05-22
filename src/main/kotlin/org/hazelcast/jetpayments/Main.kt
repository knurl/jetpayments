@file:OptIn(ExperimentalCoroutinesApi::class)

package org.hazelcast.jetpayments

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.hazelcast.jetpayments.AppConfig.paymentAmountRand
import org.hazelcast.jetpayments.AppConfig.paymentProcessingDelayRand
import org.hazelcast.jetpayments.AppConfig.paymentRequestDelayRand
import java.util.logging.Level
import kotlin.math.absoluteValue
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit
import kotlin.time.TimeSource

/*
 * Main entry point for the jetpayments. This is where the PaymentsRun is started.
 */
fun main(): Unit = runBlocking {
    val logger = ElapsedTimeLogger("Main")
    explainer(logger)
    PaymentsRun().run(2)
}

@Suppress("MayBeConstant")
internal object AppConfig {
    // Feel free to modify this section as you please.
    val clusterSize = 3
    val screenWidth = 180 // How wide should we draw in the log output
    val numMerchants = 4
    val kafkaBootstrap: String = "localhost:9092"
    val jetScaleUpDelayMillis = 5000L
    val simulatePaymentProcessorFailure = false
    val reportFrequency = 5.seconds
    val seed = 9L
    val paymentRequestDelayRand = RandomNormalDist(100.0, 25.0, seed)
    val paymentProcessingDelayRand = RandomNormalDist(750.0, 250.0, seed)
    val paymentAmountRand = RandomNormalDist(75.0, 37.5, seed)
    val enableMemberLogs = false
    val logLevel: Level = Level.SEVERE
    val merchantIdWidth = 15

    // Following items generally needn't be changed.
    val steadyStateTime = 30.seconds
    val jetMeanRescheduleTime = 10.seconds
    val warmupTime = 10.seconds // time before failures
    val failureCycleTime = (steadyStateTime + jetMeanRescheduleTime) * 2
    val cooldownTime = 15.seconds // time after failures
    val kafkaPollTimeout = 10.seconds
    val jetStateHistorySize = 4
    val screenWidthWithoutPrefix = screenWidth - 54
    val kafkaTopicName = "payment-requests"
    val kafkaProcessPaymentsCG = "initial-consumer-grp"
    val kafkaVerifyPaymentsCG = "verify-consumer-grp"
    val paymentProcessingJetJobName = "payment-processing-job"
    val paymentProcessedCheckJetJobName = "payment-processed-check-job"
    val paymentDistCheckJetJobName = "payment-member-distribution-check-job"
    val uuidToMemberIndexMapName = "uuid-to-member-index-map"
    val paymentReceiptMapName = "payment-receipt-map"
    val paymentProcessedCheckList = "payment-processed-check-list"
    val paymentOnOneNodeCheckMap = "payment-on-one-node-check-map"
}

internal fun explainer(logger: Logger) {
    """ ++ EXPLAINER ++ => As payments start flowing in from Kafka, you'll see a
        stream of updates from the Watcher. Each line will show you a series of
        metrics on these payments: QUD means how many queued, awaiting processing;
        WKG means the number in "working" (being processed), and FIN means how many
        finished ("processed"). After the metrics, you'll see a field for each of
        the various merchants (using the 3-letter short name for the merchant; see
        table immediately below). At the right-hand end of that field, you'll see
        something like "10 𝘰𝘯 𝟙", which means that node 𝟙 is currently
        responsible for processing that merchant's payments, and there are 10
        payments processed so far. A failure simulator runs in the background to
        brings nodes down and up. As it does this, and Jet reschedules jobs to
        available nodes, you'll see this reflected in each merchant field. The
        existing payments on previously-scheduled nodes will move to the left, as
        the newly-scheduled node appears on the right.
    """.trimIndent().wrapText(AppConfig.screenWidthWithoutPrefix).lines()
        .logBoxed(logger, boxStyle = Logger.BoxStyle.DOUBLE)
}

internal fun paymentRequestDelayNext() = paymentRequestDelayRand.getValue().milliseconds
internal fun paymentProcessingDelayNext() =
    paymentProcessingDelayRand.getValue().milliseconds

internal fun paymentAmountNext() = paymentAmountRand.getValue().absoluteValue

internal fun calculateNumPayments(numFailureCycles: Int) =
    ((AppConfig.warmupTime + AppConfig.failureCycleTime * numFailureCycles + AppConfig.cooldownTime) / paymentRequestDelayRand.mean.milliseconds).toInt()

/*
 * Randomness -> We want a single seed that we choose once, for all randomness
 */
internal val seededRandom = Random(AppConfig.seed)

/*
 * We want to measure time from a single baseline at the beginning of the test.
 * Encapsulate that here. Allow it to be reset in the case of multiple payments
 * runs in sequence.
 */
internal object Epoch {
    private var theEpoch = TimeSource.Monotonic.markNow()

    fun reset() {
        theEpoch = TimeSource.Monotonic.markNow()
    }

    fun elapsed() = theEpoch.elapsedNow()
}

/*
 * Miscellaneous helper functions.
 */

internal fun String.uniqify() =
    (1..4).map { (('a'..'z') + ('0'..'9')).random(seededRandom) }.joinToString("")
        .let { suffix ->
            "$this-$suffix"
        }

internal fun Duration.toRoundedSeconds() =
    ((this * 10).toInt(DurationUnit.SECONDS) / 10.0).seconds

internal fun Duration.toWholeSecStr() = "${this.toInt(DurationUnit.SECONDS)}s"

// Write this digit using an etched font.
internal fun fontEtched(digit: Int): String {
    require(digit in 0..9)
    val etched =
        listOf("𝟘", "𝟙", "𝟚", "𝟛", "𝟜", "𝟝", "𝟞", "𝟟", "𝟠", "𝟡").toTypedArray()
    return etched[digit % etched.size]
}

// Write this character using a bold font.
internal fun fontBold(char: Char): String {
    require(char in 'A'..'Z')
    val bold = mapOf(
        'A' to "𝗔",
        'B' to "𝗕",
        'C' to "𝗖",
        'D' to "𝗗",
        'E' to "𝗘",
        'F' to "𝗙",
        'G' to "𝗚",
        'H' to "𝗛",
        'I' to "𝗜",
        'J' to "𝗝",
        'K' to "𝗞",
        'L' to "𝗟",
        'M' to "𝗠",
        'N' to "𝗡",
        'O' to "𝗢",
        'P' to "𝗣",
        'Q' to "𝗤",
        'R' to "𝗥",
        'S' to "𝗦",
        'T' to "𝗧",
        'U' to "𝗨",
        'V' to "𝗩",
        'W' to "𝗪",
        'X' to "𝗫",
        'Y' to "𝗬",
        'Z' to "𝗭"
    )
    return bold[char]!!
}

/*
 * Extension function on String to count the number of Unicode codepoints. We'll use
 * a lot for Unicode strings in place of the length property of the String.
 */
internal fun String.numCodepoints() = this.codePointCount(0, this.length)

/*
 * Create our own versions of pad, padStart and padEnd. These already exist in
 * Kotlin's standard libraries, but they use *length*, and as many of the strings
 * we're logging are Unicode, we want to use numCodepoints() instead of length.
 */
internal fun String.pad(length: Int) = " ".repeat(length - this.numCodepoints())
internal fun String.padStart(length: Int) = pad(length) + this
internal fun String.padEnd(length: Int): String = this + pad(length)

/*
 * Base class for ClosedRange<T> that implements Comparable<ClosedRange<T>>. This is
 * needed for the doRangesOverlap() function below. Note that ClosedRange<T> is a
 * Kotlin standard library interface, but it doesn't implement
 * Comparable<ClosedRange<T>>. So we create our own.
 */
internal abstract class ComparableClosedRange<T : Comparable<T>> : ClosedRange<T>,
    Comparable<ClosedRange<T>> {
    override fun compareTo(other: ClosedRange<T>) =
        this.start.compareTo(other.start)
}

// Does this ClosedRange overlap with the other?
internal fun <T : Comparable<T>> ClosedRange<T>.overlaps(other: ClosedRange<T>) =
    this.start <= other.endInclusive && this.endInclusive >= other.start

// Do any of the ClosedRanges in the list overlap?
internal fun <T : Comparable<T>> doRangesOverlap(ranges: Iterable<ComparableClosedRange<T>>) =
    ranges.sorted().zipWithNext().any { (first, second) ->
        first.overlaps(second)
    }

/*
 * Use Java's Random class to create a normal distribution.
 */
internal class RandomNormalDist(
    val mean: Double,
    val stddev: Double,
    seed: Long,
) {
    private val javaRandom = java.util.Random(seed)
    fun getValue() = javaRandom.nextGaussian() * stddev + mean
}

/*
 * Take a string and wrap it to the given width.
 */
internal fun String.wrapText(maxLength: Int): String {
    class Line(initialWords: List<String> = emptyList()) {
        private val words = mutableListOf<String>()
        private var lineLength = 0

        init {
            if (initialWords.isNotEmpty()) {
                initialWords.forEach { word ->
                    if (!tryToAdd(word)) throw IllegalArgumentException("Word $word too big for line of size $maxLength")
                }
            }
        }

        fun tryToAdd(word: String): Boolean {
            require(word.isNotEmpty()) { "Word cannot be empty" }
            require(!word.contains("\n")) { "Line cannot contain newlines" }
            require(!word.contains(" ")) { "Line cannot contain spaces" }
            require(word.numCodepoints() <= maxLength) { "Word $word too long for line of length $maxLength" }
            return if (lineLength + 1 + word.numCodepoints() <= maxLength) {
                words.add(word)
                lineLength += 1 + word.numCodepoints()
                true
            } else false
        }

        override fun toString() = words.joinToString(" ")
    }

    // First replace any newlines with spaces and normalize multiple spaces
    val flatText = this.replace("\n", " ").replace(Regex("\\s+"), " ").trim()

    /*
     * Split the string into Lines (class above), each of which fits within the
     * text width provided.
     */
    return flatText.split(" ").fold(mutableListOf<Line>(Line())) { lines, word ->
        val lastLine = lines.last()
        if (lastLine.tryToAdd(word)) { // See if it fits on current line
            lines
        } else { // Start a new line, starting with this word
            lines.apply {
                add(Line(listOf(word)))
            }
        }
    }.joinToString("\n") { it.toString() }
}

internal fun String.stripTabs() = this.replace("\t", " ".repeat(4))
