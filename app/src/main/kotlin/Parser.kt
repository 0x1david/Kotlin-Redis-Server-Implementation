import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readFully
import io.ktor.utils.io.readUTF8Line
import java.net.ProtocolException

private const val VERBATIM_FORMAT_LENGTH = 3 // Per RESP3 spec

sealed interface RespValue

// https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
data class RespSimpleString(val value: String) : RespValue
data class RespSimpleError(val message: String) : RespValue
data class RespInteger(val value: Long) : RespValue
data class RespBulkString(val value: String?) : RespValue
class RespArray(var elements: MutableList<RespValue>) : RespValue
data class RespBool(val value: Boolean) : RespValue
data class RespDouble(val value: Double) : RespValue
data class RespBigNumber(val value: String) : RespValue
data class RespBulkError(val message: String) : RespValue
data class RespVerbatimString(val format: String, val value: String) : RespValue
data class RespMap(val entries: Map<RespValue, RespValue>) : RespValue
data class RespAttributes(val entries: Map<RespValue, RespValue>) : RespValue
data class RespSet(val entries: Set<RespValue>) : RespValue
data class RespPush(val entries: List<RespValue>) : RespValue
data object RespNull : RespValue

suspend fun ByteReadChannel.readRespValue(
    maxDepth: Int = 1000,
    maxCollectionSize: Int = 1_000_000,
    maxStringLength: Int = 512_000_000
): RespValue {
    return RespParser(this, maxDepth, maxCollectionSize, maxStringLength)
        .readRespPayload()
}

class RespParser(
    private val channel: ByteReadChannel,
    private val maxDepth: Int = 1000,
    private val maxCollectionSize: Int = 1_000_000,
    private val maxStringLength: Int = 512_000_000 // 512MB, Redis default
) {
    private var currentDepth = 0

    suspend fun readRespPayload(): RespValue {
        checkDepth()
        currentDepth++
        return try {
            when (val type = channel.readByte().toInt().toChar()) {
                '*' -> readRespArray()
                '$' -> readRespBulkString()
                '!' -> readRespBulkError()
                '=' -> readRespVerbatimString()
                '%' -> readRespMap()
                '|' -> readRespAttributes()
                '~' -> readRespSet()
                '>' -> readRespPush()
                '#' -> readRespBool()
                '+' -> {
                    val line = readLineUntilCRLF()
                    RespSimpleString(line)
                }

                '-' -> {
                    val line = readLineUntilCRLF()
                    RespSimpleError(line)
                }

                ':' -> {
                    val line = readLineUntilCRLF()
                    RespInteger(line.toLongOrNull() ?: throw ProtocolException("Invalid integer"))
                }

                ',' -> {
                    val line = readLineUntilCRLF()
                    RespDouble(line.toDoubleOrNull() ?: throw ProtocolException("Invalid double"))
                }

                '(' -> {
                    val line = readLineUntilCRLF()
                    RespBigNumber(line)
                }

                '_' -> {
                    consumeCRLF()
                    RespNull
                }

                else -> throw ProtocolException("Unsupported Resp type: $type")
            }
        } finally {
            currentDepth--
        }
    }

    private suspend fun readRespBool(): RespValue = when (channel.readUTF8Line()) {
        "t" -> RespBool(true)
        "f" -> RespBool(false)
        else -> throw ProtocolException("Invalid boolean")
    }


    private suspend fun readRespArray(): RespValue {
        val count = channel.readUTF8Line()?.toIntOrNull() ?: throw ProtocolException("Invalid array length")
        checkCollectionSize(count)

        val arr = (0 until count).mapTo(mutableListOf()) {
            readRespPayload()
        }
        return RespArray(arr)
    }

    private suspend fun readRespPush(): RespValue {
        val count = channel.readUTF8Line()?.toIntOrNull() ?: throw ProtocolException("Invalid push length")
        checkCollectionSize(count)

        val arr = (0 until count).map {
            readRespPayload()
        }
        return RespPush(arr)
    }

    private suspend fun readRespSet(): RespValue {
        val count = channel.readUTF8Line()?.toIntOrNull() ?: throw ProtocolException("Invalid set length")
        checkCollectionSize(count)

        val set = buildSet(count) {
            repeat(count) {
                add(readRespPayload())
            }
        }
        return RespSet(set)
    }

    private suspend fun readRespBulkString(): RespValue {
        val length = channel.readUTF8Line()?.toInt() ?: throw ProtocolException("Missing length in bulk string")
        if (length == -1) return RespNull
        checkStringLength(length)

        val bytes = ByteArray(length)
        channel.readFully(bytes, 0, length)
        consumeCRLF()

        return RespBulkString(bytes.decodeToString())
    }

    private suspend fun readRespBulkError(): RespValue {
        val length = channel.readUTF8Line()?.toInt() ?: throw ProtocolException("Missing length in bulk error")
        if (length == -1) return RespNull
        checkStringLength(length)

        val bytes = ByteArray(length)
        channel.readFully(bytes, 0, length)
        consumeCRLF()

        return RespBulkError(bytes.decodeToString())
    }

    private suspend fun readRespVerbatimString(): RespValue {
        val length = channel.readUTF8Line()?.toInt() ?: throw ProtocolException("Missing length in verbatim string")
        val dataLength = length - VERBATIM_FORMAT_LENGTH - 1
        checkStringLength(dataLength)

        val formatBytes = ByteArray(VERBATIM_FORMAT_LENGTH)
        channel.readFully(formatBytes, 0, VERBATIM_FORMAT_LENGTH)
        require(channel.readByte() == ':'.code.toByte()) { "Expected `:`" }

        val bytes = ByteArray(dataLength)
        channel.readFully(bytes, 0, dataLength)
        consumeCRLF()

        return RespVerbatimString(formatBytes.decodeToString(), bytes.decodeToString())
    }

    private suspend fun readRespMap(): RespValue {
        val count = channel.readUTF8Line()?.toIntOrNull() ?: throw ProtocolException("Invalid map length")
        checkCollectionSize(count)

        val entries = buildMap(count) {
            repeat(count) {
                put(readRespPayload(), readRespPayload())
            }
        }
        return RespMap(entries)
    }

    private suspend fun readRespAttributes(): RespValue {
        val count = channel.readUTF8Line()?.toIntOrNull() ?: throw ProtocolException("Invalid attributes length")
        checkCollectionSize(count)

        val entries = buildMap(count) {
            repeat(count) {
                put(readRespPayload(), readRespPayload())
            }
        }
        return RespAttributes(entries)
    }


    private suspend fun consumeCRLF() {
        require(
            channel.readByte() == '\r'.code.toByte() &&
                    channel.readByte() == '\n'.code.toByte()
        ) {
            "Expected CRLF sequence"
        }
    }

    private fun checkDepth() {
        if (currentDepth >= maxDepth) {
            throw ProtocolException("Maximum nesting depth exceeded: $maxDepth")
        }
    }

    private fun checkCollectionSize(size: Int) {
        if (size !in 0..maxCollectionSize) {
            throw ProtocolException("Collection size $size exceeds maximum: $maxCollectionSize")
        }
    }

    private fun checkStringLength(length: Int) {
        if (length !in -1..maxStringLength) {
            throw ProtocolException("String length $length exceeds maximum: $maxStringLength")
        }
    }

    private suspend fun readLineUntilCRLF(): String {
        val bytes = mutableListOf<Byte>()

        while (true) {
            val byte = channel.readByte()
            if (byte == '\r'.code.toByte()) {
                val next = channel.readByte()
                if (next == '\n'.code.toByte()) {
                    return bytes.toByteArray().decodeToString()
                }
                throw ProtocolException("Expected LF after CR")
            }
            bytes.add(byte)
        }
    }
}


