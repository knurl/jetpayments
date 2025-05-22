package org.hazelcast.jetpayments

import org.hazelcast.jetpayments.ReceiptWatcher.NodeTally

/*
 * This is a specialized class used to render the per-merchant fields by the
 * ReceiptWatcher. For each merchant, we want to show the number of _consecutive_
 * payments processed for that merchant on a specific node. The TallyField class
 * takes care of rendering this information in a compact way, so that the user can
 * see the number of payments processed by for a merchant on each node in time
 * order.
 *
 * The class is initialized with the per-merchant value from the payment receipt summary map
 * tracked by the ReceiptWatcher.
 */
class TallyField(
    private val width: Int,
    private val merchant: String,
    private val tally: List<NodeTally>) {

    init {
        require(width >= merchant.numCodepoints() + 2)
    }

    private val open = "▹"
    private val ellipsis = "⋯"
    private val arrow = " → "
    private val on = "𝘰𝘯"

    private fun nodeTallyRep(numProcessed: Int, nodes: String) =
        "$numProcessed $on $nodes"

    /*
     * This is the private method that does the hard work of rendering the
     * per-merchant field according to the available space for the field.
     */
    private fun tallyRep(tally: List<NodeTally>, width: Int): String? {
        fun nodeTallyRep(nodeTally: NodeTally) =
            nodeTallyRep(nodeTally.numProcessed, fontEtched(nodeTally.onMember))

        fun allMembers(tally: List<NodeTally>) =
            tally.joinToString(",") { fontEtched(it.onMember) }

        fun fistAndLast(tally: List<NodeTally>) =
            fontEtched(tally.first().onMember) + ellipsis + fontEtched(tally.last().onMember)

        val mostRecent = tally.lastOrNull() ?: return ""
        val lessRecent = tally.dropLast(1)

        /* We only have the most recent element (no older elements). This is the
         * termination condition for this recursive function.
         */
        if (lessRecent.isEmpty()) {
            val rep = nodeTallyRep(mostRecent)
            return if (rep.numCodepoints() <= width) rep else null
        }

        /* We have older elements to integrate. Start by seeing if we can integrate
         * the string representation for the most recent element, along with the
         * older elements.
         */
        val mostRecentRep = arrow + nodeTallyRep(mostRecent)

        // recursive call
        val u = tallyRep(lessRecent, width - mostRecentRep.numCodepoints())

        // Ok, it fit. We can display all the elements.
        if (u != null) {
            val rep = u + mostRecentRep
            check(rep.numCodepoints() <= width)
            return rep
        }

        /* It didn't fit. We couldn't fit the older elements in the remaining space.
         * Now try to condense the representation of all of the nodeTally elements,
         * including the most recent element.
         */
        val sumProcessed = tally.sumOf { it.numProcessed }

        val small = nodeTallyRep(sumProcessed, allMembers(tally))
        if (small.numCodepoints() <= width) {
            return small
        }

        val smaller = nodeTallyRep(sumProcessed, fistAndLast(tally))
        if (smaller.numCodepoints() <= width) {
            return smaller
        }

        return null // Failure condition. Nothing fits.
    }

    /*
     * This is the public point of access for the class. It generates the string
     * representation for the per-merchant field.
     */
    fun generate(): String {
        val merchant = "$merchant$open "
        val widthLeft = width - merchant.numCodepoints()
        val rep = tallyRep(tally, widthLeft) ?: "CANTFIT"
        return merchant + rep.padStart(widthLeft)
    }
}
