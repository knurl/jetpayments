package org.hazelcast.jetpayments

import kotlinx.coroutines.flow.flow
import org.hazelcast.jetpayments.MerchantGenerator.Merchant

/*
 * Class that generates a Kotlin Flow of simulated payments for us to process.
 */
class PaymentGenerator(
    private val seededRandom: kotlin.random.Random,
    private val merchantMap: Map<String, Merchant> = MerchantGenerator(
        AppConfig.numMerchants, seededRandom
    ).merchantMap,
) {
    fun newPaymentRequestFlow() = flow {
        generateSequence(1) { it + 1 }.forEach { paymentId ->
            fun to2Digits(x: Double) = ((x * 100).toInt() / 100.0)
            val merchant = merchantMap.values.random(seededRandom)
            val payment = PaymentRequest(
                paymentId,
                to2Digits(paymentAmountNext()),
                Epoch.elapsed(), // will be updated to the issue time later
                merchant.id,
                merchant.name,
            )
            emit(payment) // send to flow
        }
    }
}
