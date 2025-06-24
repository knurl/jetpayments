package org.hazelcast.jetpayments

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

internal abstract class Actor<T> : Channel<T> by Channel(Channel.UNLIMITED) {
    abstract suspend fun processMessage(message: T)

    fun start(scope: CoroutineScope) {
        scope.launch {
            for (msg in this@Actor) processMessage(msg)
        }
    }
}