import java.nio.ByteBuffer


class RedisStream(
    private val trie: StreamTrie = StreamTrie()
) {
    private var lastInserted = StreamId(0uL, 0uL)

    fun add(id: StreamId, fields: Map<String, ByteArray>): Result<StreamId> {
        if (id.timestampMs == 0uL && id.sequence == 0uL) return Result.failure(IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0"))
        if (id.timestampMs < lastInserted.timestampMs || (id.timestampMs == lastInserted.timestampMs && id.sequence <= lastInserted.sequence)) return Result.failure(
            IllegalArgumentException("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        )
        lastInserted = id

        val entry = StreamEntry(id, fields)
        trie.insert(entry)
        return Result.success(id)
    }

    fun len(): Int = trie.size()

}

data class StreamEntry(
    val id: StreamId,
    val fields: Map<String, ByteArray>
)


data class StreamId(
    val timestampMs: ULong,
    val sequence: ULong,
) : Comparable<StreamId> {
    fun toBytes(): ByteArray {
        val buf = ByteBuffer.allocate(16)
        buf.putLong(timestampMs.toLong())
        buf.putLong(sequence.toLong())
        return buf.array()
    }

    override fun compareTo(other: StreamId): Int {
        val cmp = timestampMs.compareTo(other.timestampMs)
        if (cmp != 0) return cmp
        return sequence.compareTo(other.sequence)
    }

    companion object {
        fun fromBytes(bytes: ByteArray): StreamId {
            require(bytes.size == 16) { "StreamId requires 16 bytes" }
            val buf = ByteBuffer.wrap(bytes)
            return StreamId(
                timestampMs = buf.getLong().toULong(),
                sequence = buf.getLong().toULong()
            )
        }

        fun parse(idString: String): StreamId {
            val parts = idString.split("-")
            require(parts.size == 2) { "Invalid StreamId format" }
            return StreamId(
                timestampMs = parts[0].toULong(),
                sequence = parts[1].toULong()
            )
        }
    }
}
