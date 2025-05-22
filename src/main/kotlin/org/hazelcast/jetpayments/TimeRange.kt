package org.hazelcast.jetpayments

import java.io.Serializable
import kotlin.time.Duration

/*
 * A range of time, with a marker string that can be used to label the range. The
 * marker is used to generate the string representation of the TimeRange, and is
 * also used to determine whether two TimeRanges are mergeable.
 */
internal class TimeRange(
    val marker: String,
    override val start: Duration,
    override val endInclusive: Duration,
) : ComparableClosedRange<Duration>(), Serializable {

    // for singletons (points in time of zero width)
    constructor(
        marker: String,
        timestamp: Duration,
    ) : this(marker, timestamp, timestamp)

    init {
        require(start <= endInclusive) { "Invalid range: $start..$endInclusive" }
    }

    fun isMergeable(other: TimeRange): Boolean {
        return this.marker == other.marker && this.endInclusive <= other.start
    }

    fun merge(other: TimeRange): TimeRange {
        require(this.isMergeable(other))
        return TimeRange(marker, start, other.endInclusive)
    }

    override fun toString() = "($marker: $start .. $endInclusive)"
}

/*
 * Generalized function for combining a list of items into a condensed list.
 */
internal fun <T> combine(
    items: List<T>,
    combinable: (T, T) -> Boolean,
    combine: (T, T) -> T,
): List<T> {
    if (items.isEmpty()) return emptyList()
    return items.fold(mutableListOf(items.first())) { acc, curr ->
        if (combinable(acc.last(), curr)) {
            acc[acc.lastIndex] = combine(acc.last(), curr)
        } else {
            acc.add(curr)
        }
        acc
    }
}

/*
 * Fold consecutive TimeRanges into a condensed set.
 */
internal fun foldTimeRanges(
    sortedTimeRanges: List<TimeRange>,
) = combine(sortedTimeRanges, TimeRange::isMergeable, TimeRange::merge)
