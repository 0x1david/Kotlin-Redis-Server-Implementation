// Currently for beginning of implementation we use a simple True
// Later on a switch to Radix Tree with Packed Leafs
class TrieNode(
    // Map Representation over linked list due to sparse data
    val children: MutableMap<Byte, TrieNode> = mutableMapOf(),
    var value: StreamEntry? = null
)

class StreamTrie {
    private val root = TrieNode()
    private var size = 0

    companion object {
        private val MIN_ID = ByteArray(16) { 0 }
        private val MAX_ID = ByteArray(16) { 0xFF.toByte() }
        val MIN_STREAM_ID = StreamId(0u, 0u)
        val MAX_STREAM_ID = StreamId(ULong.MAX_VALUE, ULong.MAX_VALUE)
    }

    fun insert(entry: StreamEntry) {
        val isNew = insertRecursive(root, entry.id.toBytes(), 0, entry)
        if (isNew) size++
    }

    fun getStreamMaxId(): StreamId? {
        var current = root
        val path = ByteArray(16)

        for (i in 0 until 16) {
            if (current.children.isEmpty()) return null

            val maxByte = current.children.keys.max()
            path[i] = maxByte
            current = current.children[maxByte]!!
        }

        return current.value?.id
    }

    fun search(id: StreamId): StreamEntry? = searchRecursive(root, id.toBytes(), 0)

    fun delete(id: StreamId): Boolean {
        val existed = deleteRecursive(root, id.toBytes(), 0).existed
        if (existed) size--
        return existed
    }

    fun rangeQuery(start: StreamId = MIN_STREAM_ID, end: StreamId = MAX_STREAM_ID): Sequence<StreamEntry> =
        sequence { collectRange(root, ByteArray(0), start.toBytes(), end.toBytes()) }

    fun rangeQuery(start: StreamId = MIN_STREAM_ID, end: StreamId = MAX_STREAM_ID, count: Int): List<StreamEntry> =
        rangeQuery(start, end).take(count).toList()


    suspend fun SequenceScope<StreamEntry>.collectRange(
        node: TrieNode,
        currentPath: ByteArray,
        start: ByteArray,
        end: ByteArray
    ) {
        if (currentPath.size == 16) {
            if (node.value != null && currentPath.isInRange(start, end)) yield(node.value!!)
            return
        }

        for ((byte, child) in node.children.entries.sortedBy { it.key.toInt() and 0xFF }) {
            val childPath = currentPath + byte
            val depth = childPath.size

            val endPrefix = end.sliceArray(0 until minOf(depth, end.size))
            val startPrefix = start.sliceArray(0 until minOf(depth, start.size))

            if (childPath > endPrefix) break
            if (childPath < startPrefix) continue

            collectRange(child, childPath, start, end)
        }
    }

    fun contains(id: StreamId): Boolean = search(id) != null
    fun isEmpty(): Boolean = root.children.isEmpty()
    fun size(): Int = size

    fun iterator(): Iterator<StreamEntry> =
        sequence { collectRange(root, ByteArray(0), MIN_ID, MAX_ID) }.iterator()

    fun iteratorFrom(id: StreamId): Iterator<StreamEntry> =
        sequence { collectRange(root, ByteArray(0), id.toBytes(), MAX_ID) }.iterator()

    fun trimBefore(id: StreamId): Int {
        var deleted = 0
        val exclusiveEnd = StreamId(id.timestampMs, id.sequence - 1u)
        val entriesToDelete = rangeQuery(MIN_STREAM_ID, exclusiveEnd)
            .map { it.id }
            .toList()

        for (entryId in entriesToDelete) {
            if (delete(entryId)) deleted++
        }

        return deleted
    }

    fun trimToMaxLength(maxLen: Int): Int {
        if (size <= maxLen) return 0

        var deleted = 0
        val toDelete = size - maxLen

        val entriesToDelete = iterator().asSequence()
            .take(toDelete)
            .map { it.id }
            .toList()

        for (id in entriesToDelete) {
            if (delete(id)) deleted++
        }

        return deleted
    }

    private fun insertRecursive(node: TrieNode, key: ByteArray, index: Int, entry: StreamEntry): Boolean {
        if (index == key.size) {
            val isNewEntry = node.value == null
            node.value = entry
            return isNewEntry
        }

        val byte = key[index]
        val child = node.children.getOrPut(byte) { TrieNode() }
        return insertRecursive(child, key, index + 1, entry)
    }

    private fun searchRecursive(node: TrieNode, key: ByteArray, index: Int): StreamEntry? {
        if (index == key.size) return node.value

        val byte = key[index]
        return node.children[byte]?.let { searchRecursive(it, key, index + 1) }
    }

    private fun deleteRecursive(node: TrieNode, key: ByteArray, index: Int): DeleteResult {
        if (index == key.size) {
            val existed = node.value != null
            node.value = null
            return DeleteResult(existed = existed, canRemove = node.children.isEmpty())
        }

        val child = node.children[key[index]]
            ?: return DeleteResult(existed = false, canRemove = false)

        val result = deleteRecursive(child, key, index + 1)
        if (result.canRemove) node.children.remove(key[index])

        return DeleteResult(
            existed = result.existed,
            canRemove = node.value == null && node.children.isEmpty()
        )
    }

    private data class DeleteResult(val existed: Boolean, val canRemove: Boolean)

    fun debugPrint() {
        println("StreamTrie Debug (size: $size)")
        println("=".repeat(50))
        debugPrintRecursive(root, "", 0)
    }

    private fun debugPrintRecursive(node: TrieNode, prefix: String, depth: Int) {
        if (node.value != null) {
            println("$prefix└─ [LEAF] ${node.value!!.id} -> ${node.value!!.fields}")
        }

        val sortedChildren = node.children.entries.sortedBy { it.key.toInt() and 0xFF }
        sortedChildren.forEachIndexed { index, (byte, child) ->
            val isLast = index == sortedChildren.size - 1
            val connector = if (isLast) "└─" else "├─"
            val childPrefix = if (isLast) "   " else "│  "

            val byteHex = String.format("%02X", byte.toInt() and 0xFF)
            val pathBytes = depth + 1
            val status = if (child.value != null) "[VALUE]" else "[NODE]"

            println("$prefix$connector $status byte[$pathBytes]: 0x$byteHex")
            debugPrintRecursive(child, prefix + childPrefix, depth + 1)
        }
    }

    fun debugPrintCompact() {
        println("StreamTrie Contents (size: $size)")
        iterator().asSequence().forEachIndexed { index, entry ->
            println("[$index] ${entry.id} -> ${entry.fields}")
        }
    }

    fun debugPrintPaths() {
        println("StreamTrie Paths (size: $size)")
        debugPrintPathsRecursive(root, ByteArray(0))
    }

    private fun debugPrintPathsRecursive(node: TrieNode, currentPath: ByteArray) {
        if (node.value != null) {
            val hexPath = currentPath.joinToString("") { String.format("%02X", it.toInt() and 0xFF) }
            println("$hexPath -> ${node.value!!.id} (${node.value!!.fields})")
        }

        node.children.entries.sortedBy { it.key.toInt() and 0xFF }.forEach { (byte, child) ->
            debugPrintPathsRecursive(child, currentPath + byte)
        }
    }
}

