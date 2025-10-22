class ParseException(message: String) : Exception(message)

fun parseCommand(resp: RespValue): Result<RedisCommand> {
    if (resp !is RespArray) {
        return Result.failure(ParseException("ERR invalid command format"))
    }

    val commandName = (resp.elements.firstOrNull() as? RespBulkString)?.value?.uppercase()
        ?: return Result.failure(ParseException("ERR invalid command format"))

    return when (commandName) {
        "PING" -> Result.success(RedisCommand.Ping)
        "MULTI" -> Result.success(RedisCommand.Multi)
        "EXEC" -> Result.success(RedisCommand.Exec)
        "DISCARD" -> Result.success(RedisCommand.Discard)
        "ECHO" -> parseSingleCommandArg(resp, "echo", RedisCommand::Echo)
        "GET" -> parseSingleCommandArg(resp, "get", RedisCommand::Get)
        "LLEN" -> parseSingleCommandArg(resp, "llen", RedisCommand::LLen)
        "TYPE" -> parseSingleCommandArg(resp, "type", RedisCommand::Type)
        "INCR" -> parseSingleCommandArg(resp, "incr", RedisCommand::Incr)
        "SET" -> parseSet(resp)
        "RPUSH" -> parseRPush(resp)
        "LPUSH" -> parseLPush(resp)
        "RPOP" -> parseRPop(resp)
        "LPOP" -> parseLPop(resp)
        "BLPOP" -> parseBLPop(resp)
        "LRANGE" -> parseLRange(resp)
        "XADD" -> parseXAdd(resp)
        "XRANGE" -> parseXRange(resp)
        "XREAD" -> parseXRead(resp)
        else -> Result.failure(ParseException("ERR unknown command '$commandName'"))
    }
}

fun <T : RedisCommand> parseSingleCommandArg(
    resp: RespArray,
    commandName: String,
    constructor: (RespValue) -> T
): Result<T> =
    if (resp.elements.size != 2) {
        Result.failure(ParseException("ERR wrong number of arguments for '$commandName' command: ${resp.elements.size}"))
    } else {
        Result.success(constructor(resp.elements[1]))
    }


fun parseXAdd(resp: RespArray): Result<RedisCommand.XAdd> {
    val size = resp.elements.size
    if (size < 5) return Result.failure(ParseException("ERR wrong number of arguments for 'xadd' command: $size"))
    if (size % 2 == 0) return Result.failure(ParseException("ERR wrong number of arguments for 'xadd' command: $size"))
    val id = resp.elements[2] as? RespBulkString
        ?: return Result.failure(ParseException("ERR expected bulk string id as second argument for 'xadd' command: $size"))

    val params = (3 until size step 2).map {
        val field = resp.elements[it] as? RespBulkString
            ?: return Result.failure(ParseException("ERR unexpected argument to 'xadd' command: ${resp.elements[it]}"))
        val value = resp.elements[it + 1] as? RespBulkString
            ?: return Result.failure(ParseException("ERR unexpected argument to 'xadd' command: ${resp.elements[it + 1]}"))
        (field.value!! to value.value!!.toByteArray())
    }

    return Result.success(RedisCommand.XAdd(resp.elements[1], id.value, params))
}

fun parseXRange(resp: RespArray): Result<RedisCommand.XRange> {
    val size = resp.elements.size
    if (size != 4) return Result.failure(ParseException("ERR wrong number of arguments for 'xadd' command: $size"))
    val key = resp.elements[1] as? RespBulkString
        ?: return Result.failure(ParseException("ERR expected bulk string id as second argument for 'xadd' command: $size"))

    val start = resp.elements[2] as? RespBulkString
        ?: return Result.failure(ParseException("ERR expected bulk string start id as second argument for 'xadd' command: $size"))

    val end = resp.elements[3] as? RespBulkString
        ?: return Result.failure(ParseException("ERR expected bulk string end idas second argument for 'xadd' command: $size"))

    return Result.success(RedisCommand.XRange(key, start.value!!, end.value!!))
}

fun parseXRead(resp: RespArray): Result<RedisCommand.XRead> {
    fun <T> Iterator<T>.nextOrNull(): T? = if (hasNext()) next() else null
    val tokens = resp.elements.iterator()
    tokens.next()

    var blockTimeout: Double? = null

    var next = (tokens.nextOrNull() as? RespBulkString)?.value
        ?: return Result.failure(ParseException("ERR syntax error"))

    if (next.uppercase() == "BLOCK") {
        blockTimeout = (tokens.nextOrNull() as? RespBulkString)?.value?.toDoubleOrNull()
            ?: return Result.failure(ParseException("ERR timeout is not an integer or out of range"))
        next = (tokens.nextOrNull() as? RespBulkString)?.value
            ?: return Result.failure(ParseException("ERR syntax error"))
    }

    if (next.uppercase() != "STREAMS") return Result.failure(ParseException("ERR syntax error, STREAMS required"))

    val remaining = tokens.asSequence().toList()
    if (remaining.size < 2 || remaining.size % 2 != 0) return Result.failure(ParseException("ERR unbalanced streams"))


    val keysToStarts = remaining.chunked(remaining.size / 2).let { (keys, ids) ->
        keys.zip(ids).map { (key, id) ->
            key to ((id as? RespBulkString)?.value
                ?: return Result.failure(ParseException("ERR invalid stream ID")))
        }
    }

    return Result.success(RedisCommand.XRead(keysToStarts, blockTimeout))
}


fun parseSet(resp: RespArray): Result<RedisCommand.Set> {
    val size = resp.elements.size
    if (size < 3) return Result.failure(ParseException("ERR wrong number of arguments for 'set' command: $size"))
    if (size % 2 == 0) return Result.failure(ParseException("ERR wrong number of arguments for 'set' command: $size"))

    val params = DataStoreParams()
    for (i in 3 until size step 2) {
        params.parseParameter(resp.elements[i], resp.elements[i + 1])
    }

    return Result.success(RedisCommand.Set(resp.elements[1], resp.elements[2], params))
}

fun parseRPush(resp: RespArray): Result<RedisCommand.RPush> =
    if (resp.elements.size < 3) {
        Result.failure(ParseException("ERR wrong number of arguments for 'rpush' command: ${resp.elements.size}"))
    } else {
        Result.success(RedisCommand.RPush(resp.elements[1], resp.elements.drop(2)))
    }

fun parseLPush(resp: RespArray): Result<RedisCommand.LPush> =
    if (resp.elements.size < 3) {
        Result.failure(ParseException("ERR wrong number of arguments for 'lpush' command: ${resp.elements.size}"))
    } else {
        Result.success(RedisCommand.LPush(resp.elements[1], resp.elements.drop(2)))
    }

fun parseRPop(resp: RespArray): Result<RedisCommand.RPop> {
    val argSize = resp.elements.size
    if (argSize < 2) return Result.failure(ParseException("ERR wrong number of arguments for 'rpop' command: ${resp.elements.size}"))

    val key = resp.elements[1]
    val count = if (argSize == 2) {
        1
    } else {
        val countArg = resp.elements[2] as? RespBulkString
            ?: return Result.failure(ParseException("ERR count must be a string"))

        countArg.value?.toIntOrNull()
            ?: return Result.failure(ParseException("ERR value is not an integer or out of range"))
    }

    return Result.success(RedisCommand.RPop(key, count))
}

fun parseLPop(resp: RespArray): Result<RedisCommand.LPop> {
    val argSize = resp.elements.size
    if (argSize < 2) return Result.failure(ParseException("ERR wrong number of arguments for 'lpop' command: ${resp.elements.size}"))


    val key = resp.elements[1]
    val count = if (argSize == 2) {
        1
    } else {
        val countArg = resp.elements[2] as? RespBulkString
            ?: return Result.failure(ParseException("ERR count must be a string"))

        countArg.value?.toIntOrNull()
            ?: return Result.failure(ParseException("ERR value is not an integer or out of range"))
    }

    return Result.success(RedisCommand.LPop(key, count))
}

fun parseBLPop(resp: RespArray): Result<RedisCommand.BLPop> {
    if (resp.elements.size != 3) return Result.failure(ParseException("Expected 3 arguments for 'blpop' command, got: ${resp.elements.size}"))

    val key = resp.elements[1]
    val timeout = resp.elements[2]

    if (timeout !is RespBulkString) return Result.failure(ParseException("Expected second argument to be bulk string for blpop"))
    val timeoutValue = timeout.value?.toDoubleOrNull()
        ?: return Result.failure(ParseException("Expected second argument to be double for blpop"))

    return Result.success(RedisCommand.BLPop(key, timeoutValue))
}


fun parseLRange(resp: RespArray): Result<RedisCommand.LRange> {
    if (resp.elements.size != 4) {
        return Result.failure(ParseException("ERR wrong number of arguments for 'lrange' command: ${resp.elements.size}"))
    }

    val key = resp.elements[1]
    val startVal = resp.elements[2]
    val endVal = resp.elements[3]

    if (startVal !is RespBulkString) return Result.failure(ParseException("Provided start index is not a bulk string."))
    if (endVal !is RespBulkString) return Result.failure(ParseException("Provided end index is not a bulk string."))
    if (startVal.value == null) return Result.failure(ParseException("Provided start index is null."))
    if (endVal.value == null) return Result.failure(ParseException("Provided end index is null."))


    val start =
        startVal.value.toIntOrNull() ?: return Result.failure(ParseException("Start index is not a valid integer."))
    val end = endVal.value.toIntOrNull() ?: return Result.failure(ParseException("End index is not a valid integer."))

    return Result.success(RedisCommand.LRange(key, start, end))
}