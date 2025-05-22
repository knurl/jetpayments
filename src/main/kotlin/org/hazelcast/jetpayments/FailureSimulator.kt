package org.hazelcast.jetpayments

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/*
 * Simulate the failure and recovery of nodes in the Hazelcast cluster so we can
 * observe how Jet gracefully responds to these. The failure simulator is run on
 * a separate coroutine, asynchronously, using the handed-in HzCluster to bring
 * nodes down and back up. Keeps track of the failure periods so that these can
 * be graphed using the Canvas object later.
 */
internal class FailureSimulator(
    private val client: HzCluster.ClientInstance,
    private val watcher: ReceiptWatcher,
) {
    private val logger = ElapsedTimeLogger("FailSim")

    enum class NodeCycleRequest {
        SHUTDOWN_NODE, RESTORE_NODE
    }

    // Need to be cleared on every run
    private val eventTimeRanges =
        Array(client.originalClusterSize) { mutableListOf<TimeRange>() }

    fun clear() {
        eventTimeRanges.forEach {
            it.clear()
        }
    }

    /*
     * Keep track, across all payment runs, of the average time of failure, and of
     * recovery, measuring both from the point of initiation of the topology change,
     * right up to the point where Jet recognizes this change and reschedules
     * accordingly.
     */
    private val failureStats = object {
        var times =
            NodeCycleRequest.entries.associateWith { mutableListOf<Double>() }

        fun addTime(nodeCycleRequest: NodeCycleRequest, elapsed: Duration) {
            times[nodeCycleRequest]?.add(elapsed.toDouble(DurationUnit.SECONDS))
        }

        fun getSummary() = times.map { (nodeCycleRequest, dataPoints) ->
            "Average time for $nodeCycleRequest: ${
                dataPoints.average().toDuration(DurationUnit.SECONDS)
                    .toRoundedSeconds()
            }"
        }
    }

    fun getFailureStats() = failureStats.getSummary()

    /*
     * Suspending call to run all the failure simulations for this payment run.
     * The flow we're collecting here will stop emitting when we don't have enough
     * time to run any more complete failure simulation cycles (bringing a node
     * down, *and* back up again). At that point, the function will return. Note
     * that we will measure from the point of initiating the node down or up, to
     * the point that Jet has rescheduled work across the members according to the
     * new topology.
     */
    suspend fun runSimulations(pipeline: StreamingJetPipeline) =
        failureFlow(pipeline).collect { (nodeCycleRequest, member) ->
            val marker = when (nodeCycleRequest) {
                NodeCycleRequest.SHUTDOWN_NODE -> "↓"
                NodeCycleRequest.RESTORE_NODE -> "↑"
            }

            val reqName = nodeCycleRequest.name
            val memberName = fontEtched(member)

            logger.logEmphasis("Initiating $reqName $memberName...")

            // Adjust cluster topology and note the elapsed time for it.
            val start = Epoch.elapsed()
            adjustClusterState(nodeCycleRequest, member, pipeline)
            val end = Epoch.elapsed()

            eventTimeRanges[member].add(TimeRange(marker, start, end))

            val elapsed = end - start
            failureStats.addTime(nodeCycleRequest, elapsed)

            logger.logEmphasis("Completed $reqName $memberName in ${elapsed.toRoundedSeconds()}")
        }

    /*
     * We call this after all the failure simulations are complete, in order to get
     * the measured time of the failure cycles so we can graph these.
     */
    fun getTimeSpans() =
        eventTimeRanges.mapIndexed { member, timeRanges -> member to timeRanges }
            .filter { it.second.isNotEmpty() }.map { (member, timeRanges) ->
                TimeSpan("NODE ${fontEtched(member)}", timeRanges)
            }

    /*
     * Bring a given node down, or back up. See comments for JetScheduleState. We
     * track four states for Jet, in terms of whether or not is has rescheduled
     * subsequent to a node entering or leaving the cluster. Here we only care about
     * two of those states, which both relate to the cases where Jet has already
     * noticed any recent topology changes and has rescheduled jobs accordingly.
     */
    private suspend fun adjustClusterState(
        nodeCycleRequest: NodeCycleRequest,
        member: Int,
        pipeline: JetPipeline,
    ) {
        when (nodeCycleRequest) {
            NodeCycleRequest.SHUTDOWN_NODE -> {
                pipeline.jetSchedulerStateFlow.first { it is JetPipeline.JetSchedulerState.Running }
                client.shutdownMember(member)
                pipeline.jetSchedulerStateFlow.first { it is JetPipeline.JetSchedulerState.RunningWithNodeDown }
            }

            NodeCycleRequest.RESTORE_NODE -> {
                pipeline.jetSchedulerStateFlow.first { it is JetPipeline.JetSchedulerState.RunningWithNodeDown }
                client.restartMember(member)
                pipeline.jetSchedulerStateFlow.first { it is JetPipeline.JetSchedulerState.Running }
            }
        }
    }

    /*
     * This function creates a Kotlin floW that contains both the uptime/downtime
     * events themselves, along with a period of delay between each. These are
     * consumed by the runSimulations method above, which does a collect on the
     * flow, and responds to the events by calling adjustClusterState to bring nodes
     * down or back up again. Thus it is this function, failureFlow, that provides
     * the overall timing and rhythm for the failure simulator.
     */
    private fun failureFlow(pipeline: StreamingJetPipeline) = flow {
        delay(AppConfig.warmupTime)

        /* Always do the DOWN and UP as a pair. If we don't have enough time
         * left to do the full cycle, then don't start one.
         */
        while (pipeline.hasTimeLeft(AppConfig.failureCycleTime)) {
            delay(AppConfig.steadyStateTime) // steadyStateTime
            val memberToKill = watcher.nodesInUse.value.let {
                if (it.isNotEmpty()) it.random(seededRandom)
                else (0 until client.originalClusterSize).random(seededRandom)
            }
            emit(NodeCycleRequest.SHUTDOWN_NODE to memberToKill) // 4 * snapshot
            delay(AppConfig.steadyStateTime) // steadyStateTime
            emit(NodeCycleRequest.RESTORE_NODE to memberToKill) // 4 * snapshot
        }
    }
}
