import java.time.Instant
import java.util.PriorityQueue

data class TimeoutEntry(
    val clientId: String,
    val deadline: Instant,
)

class BlockedMap {
    // Key → clients blocked on that key (FIFO)
    private val entries: HashMap<RespValue, ArrayDeque<String>> = HashMap()

    // Client → keys it's waiting on (for cleanup)
    private val clientToKeys: HashMap<String, MutableList<RespValue>> = HashMap()
    private val timeoutQueue: PriorityQueue<TimeoutEntry> = PriorityQueue(compareBy { it.deadline })

    fun getEarliestTimeout(): TimeoutEntry? {
        return timeoutQueue.peek()
    }

    fun blockClient(clientId: String, keys: List<RespValue>, timeoutSec: Double) {
        if (timeoutSec != 0.0) {
            val deadline = Instant.now().plusMillis((timeoutSec * 1000).toLong())
            timeoutQueue.add(TimeoutEntry(clientId, deadline))
        }
        val ctk = clientToKeys.getOrPut(clientId) { mutableListOf() }
        keys.forEach {
            val en = entries.getOrPut(it) { ArrayDeque() }
            ctk.add(it)
            en.addLast(clientId)
        }
    }

    fun unblockClient(clientId: String) {
        val keys = clientToKeys.remove(clientId) ?: return
        keys.forEach {
            entries[it]?.remove(clientId)
            if (entries[it]?.isEmpty() == true) {
                entries.remove(it)
            }
        }
    }

    fun getNextClientForKey(key: RespValue): String? {
        val client = entries[key]?.removeFirstOrNull() ?: return null
        unblockClient(client)
        return client
    }

    fun getClientsTimingOutBefore(deadline: Instant): List<String> {
        val expired = mutableListOf<String>()
        while (timeoutQueue.isNotEmpty() && timeoutQueue.peek().deadline <= deadline) {
            val entry = timeoutQueue.poll()

            // Skip if already unblocked (stale entry)
            if (clientToKeys.containsKey(entry.clientId)) {
                expired.add(entry.clientId)
                unblockClient(entry.clientId)
            }
        }
        return expired
    }
}