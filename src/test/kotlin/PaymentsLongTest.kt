import io.kotest.core.spec.style.FunSpec
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.hazelcast.jetpayments.*

@OptIn(ExperimentalCoroutinesApi::class)
class PaymentsLongTest : FunSpec({
    val logger = ElapsedTimeLogger("PaymentsLongTest")

    test("Run many payment runs in series") {
        val numTests = 96
        val maxCycles = 3

        PaymentsRun().use { paymentsRun ->
            repeat(numTests) { testNum ->
                val numFailureCycles = testNum.mod(maxCycles) + 1
                TextBox(
                    "test number: ${testNum + 1} ${italic("of")} $numTests",
                    "failure cycles: $numFailureCycles"
                ).addBorder(TextBox.BorderStyle.DOUBLE).log(logger)
                Epoch.reset()
                paymentsRun.run(numFailureCycles)
            }
        }
    }
})
