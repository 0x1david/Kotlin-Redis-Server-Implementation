import java.nio.ByteBuffer


class RedisStream(
    private val trie: StreamTrie = StreamTrie()
) {
    private var lastInserted = StreamId(0uL, 0uL)


    fun add(id: String, fields: Map<String, ByteArray>): Result<StreamId> {
        val parsedId = parseOrGenerateId(id)
            ?: return Result.failure(IllegalArgumentException("ERR The stream id is in corrupted format"))
        if (parsedId.timestampMs == 0uL && parsedId.sequence == 0uL) return Result.failure(IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0"))
        if (parsedId.timestampMs < lastInserted.timestampMs || (parsedId.timestampMs == lastInserted.timestampMs && parsedId.sequence <= lastInserted.sequence)) return Result.failure(
            IllegalArgumentException("ERR The ID specified in XADD is equal or smaller than the target stream top item")
        )
        lastInserted = parsedId

        val entry = StreamEntry(parsedId, fields)
        trie.insert(entry)
        return Result.success(parsedId)
    }

    fun len(): Int = trie.size()

    fun parseOrGenerateId(idString: String): StreamId? {
        val parts = idString.split("-")
        require(parts.size == 2) { "Invalid StreamId format" }

        val timestampMs = when (parts[0]) {
            "*" -> throw NotImplementedError("Not yet")
            else -> parts[0].toULongOrNull() ?: return null
        }
        val sequence = when (parts[1]) {
            "*" -> generateSequence(timestampMs)
            else -> parts[1].toULongOrNull() ?: return null
        }
        return StreamId(timestampMs, sequence)

    }

    fun generateSequence(timestamp: ULong): ULong =
        trie.rangeQuery(
            StreamId(timestamp, 0u),
            StreamId(timestamp, ULong.MAX_VALUE)
        ).lastOrNull()
            ?.let { it.id.sequence + 1u }
            ?: if (timestamp == 0uL) 1uL else 0uL
}

data class StreamEntry(
    val id: StreamId,
    val fields: Map<String, ByteArray>
)


data class StreamId(
    val timestampMs: ULong,
    val sequence: ULong,
) : Comparable<StreamId> {

    override fun toString(): String = "${timestampMs}-${sequence}"

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
    }
}
