operator fun ByteArray.compareTo(other: ByteArray): Int {
    val minLen = minOf(this.size, other.size)
    for (i in 0 until minLen) {
        val cmp = this[i].toUByte().compareTo(other[i].toUByte())
        if (cmp != 0) return cmp
    }
    return this.size.compareTo(other.size)
}

fun ByteArray.isInRange(start: ByteArray, end: ByteArray): Boolean =
    this >= start && this <= end

fun ByteArray.startsWith(prefix: ByteArray): Boolean {
    if (prefix.size > this.size) return false
    for (i in prefix.indices) {
        if (this[i] != prefix[i]) return false
    }
    return true
}

