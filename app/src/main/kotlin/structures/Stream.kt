import java.nio.ByteBuffer

class RedisStream(
    private val trie: StreamTrie = StreamTrie()
) {

    fun add(
        id: StreamId,
        fields: Map<String, ByteArray>
    ): StreamId {
        val entry = StreamEntry(id, fields)
        trie.insert(entry)
        return id
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
