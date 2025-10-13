import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.writeByte
import io.ktor.utils.io.writeFully
import io.ktor.utils.io.writeStringUtf8

suspend fun ByteWriteChannel.writeRespValue(value: RespValue) {
    when (value) {
        is RespSimpleString -> writeSimpleString(value.value)
        is RespSimpleError -> writeSimpleError(value.message)
        is RespInteger -> writeInteger(value.value)
        is RespBulkString -> writeBulkString(value.value)
        is RespArray -> writeArray(value.elements)
        is RespBool -> writeBool(value.value)
        is RespDouble -> writeDouble(value.value)
        is RespBigNumber -> writeBigNumber(value.value)
        is RespBulkError -> writeBulkError(value.message)
        is RespVerbatimString -> writeVerbatimString(value.format, value.value)
        is RespMap -> writeMap(value.entries)
        is RespAttributes -> writeAttributes(value.entries)
        is RespSet -> writeSet(value.entries)
        is RespPush -> writePush(value.entries)
        is RespNull -> writeNull()
    }
}

private suspend fun ByteWriteChannel.writeSimpleString(value: String) {
    writeStringUtf8("+$value")
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeSimpleError(message: String) {
    writeStringUtf8("-$message")
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeInteger(value: Long) {
    writeStringUtf8(":$value")
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeBulkString(value: String?) {
    if (value == null) {
        writeStringUtf8("$-1")
        writeCRLF()
        return
    }

    val bytes = value.encodeToByteArray()
    writeStringUtf8("$${bytes.size}")
    writeCRLF()
    writeFully(bytes)
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeArray(elements: List<RespValue>) {
    writeStringUtf8("*${elements.size}")
    writeCRLF()
    elements.forEach { writeRespValue(it) }
}

private suspend fun ByteWriteChannel.writeBool(value: Boolean) {
    writeStringUtf8(if (value) "#t" else "#f")
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeDouble(value: Double) {
    require(value.isFinite()) { "RESP3 double must be finite" }
    writeStringUtf8(",$value")
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeBigNumber(value: String) {
    writeStringUtf8("($value")
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeBulkError(message: String) {
    val bytes = message.encodeToByteArray()
    writeStringUtf8("!${bytes.size}")
    writeCRLF()
    writeFully(bytes)
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeVerbatimString(format: String, value: String) {
    require(format.length == 3) { "Format must be exactly 3 characters" }
    val valueBytes = value.encodeToByteArray()
    val totalLength = 3 + 1 + valueBytes.size // format + ':' + value
    writeStringUtf8("=$totalLength")
    writeCRLF()
    writeStringUtf8("$format:")
    writeFully(valueBytes)
    writeCRLF()
}

private suspend fun ByteWriteChannel.writeMap(entries: Map<RespValue, RespValue>) {
    writeStringUtf8("%${entries.size}")
    writeCRLF()
    entries.forEach { (key, value) ->
        writeRespValue(key)
        writeRespValue(value)
    }
}

private suspend fun ByteWriteChannel.writeAttributes(entries: Map<RespValue, RespValue>) {
    writeStringUtf8("|${entries.size}")
    writeCRLF()
    entries.forEach { (key, value) ->
        writeRespValue(key)
        writeRespValue(value)
    }
}

private suspend fun ByteWriteChannel.writeSet(entries: Set<RespValue>) {
    writeStringUtf8("~${entries.size}")
    writeCRLF()
    entries.forEach { writeRespValue(it) }
}

private suspend fun ByteWriteChannel.writePush(entries: List<RespValue>) {
    writeStringUtf8(">${entries.size}")
    writeCRLF()
    entries.forEach { writeRespValue(it) }
}

private suspend fun ByteWriteChannel.writeNull() {
    writeRespValue(RespBulkString(null))
}

private suspend fun ByteWriteChannel.writeCRLF() {
    writeByte('\r'.code.toByte())
    writeByte('\n'.code.toByte())
}