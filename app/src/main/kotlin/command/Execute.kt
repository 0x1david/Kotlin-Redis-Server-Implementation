import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentHashMap

data class ExecutionContext(
    val dataStore: RedisDataStore,
    val blockedMap: BlockedMap,
    val responseChannels: ConcurrentHashMap<String, Channel<WritableRespValue>>,
    val clientId: String,
    val checkTimeouts: suspend () -> Unit
)

suspend fun executeRedisCommand(command: RedisCommand, context: ExecutionContext): RespValue {
    return when (command) {
        is RedisCommand.Ping -> executePing()
        is RedisCommand.Echo -> executeEcho(command)
        is RedisCommand.Get -> executeGet(command, context)
        is RedisCommand.Set -> executeSet(command, context)
        is RedisCommand.RPush -> executeRPush(command, context)
        is RedisCommand.LPush -> executeLPush(command, context)
        is RedisCommand.RPop -> executeRPop(command, context)
        is RedisCommand.LPop -> executeLPop(command, context)
        is RedisCommand.BLPop -> executeBLPop(command, context)
        is RedisCommand.LLen -> executeLLen(command, context)
        is RedisCommand.LRange -> executeLRange(command, context)
        is RedisCommand.Type -> executeType(command, context)
        is RedisCommand.XAdd -> executeXAdd(command, context)
        is RedisCommand.XRange -> executeXRange(command, context)
        is RedisCommand.XRead -> executeXRead(command, context)
    }
}


fun executePing(): RespValue = RespSimpleString("PONG")

fun executeEcho(command: RedisCommand.Echo): RespValue = command.message

fun executeGet(command: RedisCommand.Get, context: ExecutionContext): RespValue =
    context.dataStore.get(command.key)

fun executeSet(command: RedisCommand.Set, context: ExecutionContext): RespValue {
    context.dataStore.set(command.key, command.value, command.params)
    return RespSimpleString("OK")
}


suspend fun executeRPush(command: RedisCommand.RPush, context: ExecutionContext): RespValue {
    context.checkTimeouts()

    val key = command.key as WritableRespValue
    var lst = context.dataStore.get(key)
    if (lst is RespNull) {
        lst = RespArray(mutableListOf())
        context.dataStore.set(key, lst)
    }
    if (lst !is RespArray) return RespSimpleError("Provided key doesn't correspond to an array.")

    for (el in command.values) {
        lst.elements.add(el as WritableRespValue)
    }

    val finalSize = lst.elements.size.toLong()

    while (lst.elements.isNotEmpty()) {
        val blockedClient = context.blockedMap.getNextClientForKey(key) ?: break
        val poppedValue = lst.elements.removeFirst()
        context.responseChannels[blockedClient.clientId]?.send(RespArray(mutableListOf(key, poppedValue)))
    }

    return RespInteger(finalSize)
}

suspend fun executeLPush(command: RedisCommand.LPush, context: ExecutionContext): RespValue {
    context.checkTimeouts()

    val key = command.key as WritableRespValue
    var lst = context.dataStore.get(key)
    if (lst is RespNull) {
        lst = RespArray(mutableListOf())
        context.dataStore.set(key, lst)
    }
    if (lst !is RespArray) return RespSimpleError("Provided key doesn't correspond to an array.")

    for (el in command.values) {
        lst.elements.addFirst(el as WritableRespValue)
    }

    val finalSize = lst.elements.size.toLong()

    while (lst.elements.isNotEmpty()) {
        val blockedClient = context.blockedMap.getNextClientForKey(key) ?: break
        val poppedValue = lst.elements.removeFirst()
        context.responseChannels[blockedClient.clientId]?.send(RespArray(mutableListOf(key, poppedValue)))
    }

    return RespInteger(finalSize)
}

fun executeRPop(command: RedisCommand.RPop, context: ExecutionContext): RespValue {
    val lst = when (val item = context.dataStore.get(command.key)) {
        is RespNull -> return RespNull
        is RespArray -> item
        else -> return RespSimpleError("Provided key doesn't correspond to an array")
    }

    if (command.count <= 0 || command.count > lst.elements.size) return RespNull

    val pop = { lst.elements.removeLast() }

    return if (command.count == 1) pop()
    else RespArray(MutableList(command.count) { pop() })
}

fun executeLPop(command: RedisCommand.LPop, context: ExecutionContext): RespValue {
    val lst = when (val item = context.dataStore.get(command.key)) {
        is RespNull -> return RespNull
        is RespArray -> item
        else -> return RespSimpleError("Provided key doesn't correspond to an array")
    }

    if (command.count <= 0 || command.count > lst.elements.size) return RespNull

    val pop = { lst.elements.removeFirst() }

    return if (command.count == 1) pop()
    else RespArray(MutableList(command.count) { pop() })
}

fun executeBLPop(command: RedisCommand.BLPop, context: ExecutionContext): RespValue {
    val lst = when (val item = context.dataStore.get(command.key)) {
        is RespNull -> null
        is RespArray -> item
        else -> return RespSimpleError("Underlying datastore element is not a list")
    }

    if (lst != null && lst.elements.isNotEmpty()) {
        val poppedValue = lst.elements.removeFirst()
        return RespArray(mutableListOf(command.key as WritableRespValue, poppedValue))
    }

    context.blockedMap.blockClient(
        context.clientId,
        listOf(command.key),
        command = command,
        timeoutSec = command.timeout
    )
    return NoResponse
}

fun executeLLen(command: RedisCommand.LLen, context: ExecutionContext): RespValue {
    return when (val item = context.dataStore.get(command.key)) {
        is RespArray -> RespInteger(item.elements.size.toLong())
        else -> RespInteger(0)
    }
}

fun executeType(command: RedisCommand.Type, context: ExecutionContext): RespValue {
    return when (context.dataStore.get(command.key)) {
        is RespArray -> RespSimpleString("array")
        is RespSet -> RespSimpleString("set")
        is RespSimpleString, is RespBulkString -> RespSimpleString("string")
        is RespNull -> RespSimpleString("none")
        is RespStream -> RespSimpleString("stream")
        else -> RespSimpleString("undefined")
    }

}

fun executeLRange(command: RedisCommand.LRange, context: ExecutionContext): RespValue {
    val lst = context.dataStore.get(command.key)

    if (lst is RespNull) return RespArray(mutableListOf())
    if (lst !is RespArray) return RespSimpleError("Provided key doesn't correspond to an array.")

    val lstSize = lst.elements.size
    val normalizedStart = if (command.start < 0) {
        (lstSize + command.start).coerceAtLeast(0)
    } else {
        command.start.coerceAtMost(lstSize)
    }
    val normalizedEnd = if (command.end < 0) {
        (lstSize + command.end).coerceAtLeast(0)
    } else {
        command.end.coerceAtMost(lstSize - 1)
    }

    return when {
        normalizedStart > normalizedEnd -> RespArray(mutableListOf())
        normalizedStart >= lstSize -> RespArray(mutableListOf())
        else -> RespArray(lst.elements.subList(normalizedStart, normalizedEnd + 1))
    }
}

suspend fun executeXAdd(command: RedisCommand.XAdd, context: ExecutionContext): RespValue {
    context.checkTimeouts()

    val stream = context.dataStore.getOrPut(command.key) { RespStream() } as? RespStream
        ?: return RespSimpleError("Provided key doesn't correspond to a stream.")

    val result = stream.stream
        .add(command.id!!, command.args.toMap())
        .map { RespBulkString(it.toString()) }
        .getOrElse { return RespSimpleError(it.message ?: "System Err") }

    val blockedClient = context.blockedMap.getNextClientForKey(command.key) ?: return result

    when (val cmd = blockedClient.command) {
        is RedisCommand.XRead -> {
            val responses = cmd.keysToStarts.mapNotNull { (key, start) ->
                val streamData = context.dataStore.get(key) as? RespStream ?: return@mapNotNull null
                val entries = streamData.stream.range(start, startExcl = true).getOrThrow()
                if (entries.elements.isEmpty()) null
                else RespArray(mutableListOf(key as WritableRespValue, entries))
            }
            if (responses.isNotEmpty()) {
                context.responseChannels[blockedClient.clientId]?.send(
                    RespArray(responses.toMutableList())
                )
            }
        }

        else -> {}
    }

    return result
}

fun executeXRange(command: RedisCommand.XRange, context: ExecutionContext): RespValue {
    val stream = context.dataStore.get(command.key) as? RespStream
        ?: return RespSimpleError("Provided key doesn't correspond to a stream.")
    return stream.stream.range(command.start, command.end).getOrElse {
        return RespSimpleError(it.message!!)
    }
}

suspend fun executeXRead(command: RedisCommand.XRead, context: ExecutionContext): RespValue {
    context.checkTimeouts()

    for ((key, _) in command.keysToStarts) {
        val value = context.dataStore.get(key)
        if (value !is RespNull && value !is RespStream) {
            return RespSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
    }

    val results = command.keysToStarts.mapNotNull { (key, start) ->
        val stream = context.dataStore.get(key) as? RespStream ?: return@mapNotNull null
        val entries = stream.stream.range(start, startExcl = true).getOrThrow()
        if (entries.elements.isEmpty()) null
        else RespArray(mutableListOf(key as WritableRespValue, entries))
    }

    if (results.isNotEmpty() || command.timeout == null) return RespArray(results.toMutableList())

    val keys = command.keysToStarts.map { it.first }
    context.blockedMap.blockClient(context.clientId, keys, timeoutSec = command.timeout / 1000.0, command = command)
    return NoResponse
}