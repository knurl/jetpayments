package org.hazelcast.jetpayments

import kotlin.time.DurationUnit

/* Type of Logger that prints elapsed time since Epoch (see Main.kt), rather than
 * a standard date/timestamp.
 */
class ElapsedTimeLogger(
    label: String,
) : Logger(label) {
    override fun getFormattedTime(): String {
        return "%5.1fs".format(Epoch.elapsed().toDouble(DurationUnit.SECONDS))
    }
}
