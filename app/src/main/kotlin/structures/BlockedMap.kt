import java.time.Instant

import java.util.PriorityQueue

const val NO_TIMEOUT = 0.0

data class TimeoutEntry(
    val clientId: String,
    val deadline: Instant,
)

data class BlockedClient(
    val clientId: String,
    val command: RedisCommand
)

class BlockedMap {
    // Key → clients blocked on that key (FIFO)
    private val entries: HashMap<RespValue, ArrayDeque<BlockedClient>> = HashMap()

    // Client → full client info (for lookup and cleanup)
    private val clientToInfo: HashMap<String, BlockedClient> = HashMap()

    // Client → keys it's waiting on (for cleanup)
    private val clientToKeys: HashMap<String, MutableList<RespValue>> = HashMap()
    private val timeoutQueue: PriorityQueue<TimeoutEntry> = PriorityQueue(compareBy { it.deadline })

    fun getEarliestTimeout(): TimeoutEntry? = timeoutQueue.peek()

    fun blockClient(clientId: String, keys: List<RespValue>, command: RedisCommand, timeoutSec: Double) {
        val client = BlockedClient(clientId, command)
        clientToInfo[clientId] = client

        if (timeoutSec != 0.0) {
            val deadline = Instant.now().plusMillis((timeoutSec * 1000).toLong())
            timeoutQueue.add(TimeoutEntry(clientId, deadline))
        }

        val ctk = clientToKeys.getOrPut(clientId) { mutableListOf() }
        keys.forEach { key ->
            val en = entries.getOrPut(key) { ArrayDeque() }
            ctk.add(key)
            en.addLast(client)
        }
    }

    fun unblockClient(clientId: String) {
        clientToInfo.remove(clientId)
        val keys = clientToKeys.remove(clientId) ?: return
        keys.forEach { key ->
            entries[key]?.removeAll { it.clientId == clientId }
            if (entries[key]?.isEmpty() == true) {
                entries.remove(key)
            }
        }
    }

    fun getNextClientForKey(key: RespValue): BlockedClient? {
        val client = entries[key]?.removeFirstOrNull() ?: return null
        unblockClient(client.clientId)
        return client
    }

    fun getClientsTimingOutBefore(deadline: Instant): List<BlockedClient> {
        val expired = mutableListOf<BlockedClient>()
        while (timeoutQueue.isNotEmpty() && timeoutQueue.peek().deadline <= deadline) {
            val entry = timeoutQueue.poll()

            val client = clientToInfo[entry.clientId]
            if (client != null) {
                expired.add(client)
                unblockClient(entry.clientId)
            }
        }
        return expired
    }


}