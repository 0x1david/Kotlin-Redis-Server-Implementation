class SkipListNode(
    val member: RespBulkString,
    val score: Double,
    val level: Byte
) {
    var backward: SkipListNode? = null
    val forward: MutableList<SkipListNode?> = MutableList(level.toInt()) { null }
    val span: MutableList<Long> = MutableList(level.toInt()) { 0L }
}

class SkipList(
    private val maxLevel: Byte = 32,
    private val probability: Double = 0.25
) {
    private var size: Long = 0
    private var level: Byte = 1
    private val header: SkipListNode = SkipListNode(
        RespBulkString(""),
        Double.NEGATIVE_INFINITY, maxLevel
    )
    private var tail: SkipListNode? = null

    fun add(member: RespBulkString, score: Double): Boolean {
        TODO("Not yet implemented")
    }

    fun remove(member: RespBulkString, score: Double): Boolean {
        TODO("Not yet implemented")
    }

    fun getRank(member: RespBulkString, score: Double): Long? {
        TODO("Not yet implemented")
    }

    fun getByRank(rank: Long): SkipListNode? {
        TODO("Not yet implemented")
    }

    suspend fun SequenceScope<SkipListNode>.collectRange(
        minScore: Double = Double.NEGATIVE_INFINITY,
        maxScore: Double = Double.POSITIVE_INFINITY,
        reverse: Boolean = false
    ) {
    }

    private fun findLastInRange(maxScore: Double): SkipListNode? {
        TODO("Not yet implemented")
    }

    private fun randomLevel(): Byte {
        var lvl: Byte = 1
        while (Math.random() < probability && lvl < maxLevel) lvl++

        return lvl
    }

    private fun compareMembers(a: RespBulkString, b: RespBulkString): Int {
        return a.value!!.compareTo(b.value!!)
    }

    fun getSize(): Long = size
    fun isEmpty(): Boolean = size == 0L

    fun debugPrint() {
        println("Size: $size, Level: $level")
        var current = header.forward[0]
        while (current != null) {
            println("  ${current.member.value}: ${current.score}")
            current = current.forward[0]
        }
    }
}