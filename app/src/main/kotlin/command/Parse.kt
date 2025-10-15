class ParseException(message: String) : Exception(message)

fun parseCommand(resp: RespValue): Result<RedisCommand> {
    if (resp !is RespArray) {
        return Result.failure(ParseException("ERR invalid command format"))
    }

    val commandName = (resp.elements.firstOrNull() as? RespBulkString)?.value?.uppercase()
        ?: return Result.failure(ParseException("ERR invalid command format"))

    return when (commandName) {
        "PING" -> Result.success(RedisCommand.Ping)
        "ECHO" -> parseEcho(resp)
        "GET" -> parseGet(resp)
        "SET" -> parseSet(resp)
        "RPUSH" -> parseRPush(resp)
        "LPUSH" -> parseLPush(resp)
        "RPOP" -> parseRPop(resp)
        "LPOP" -> parseLPop(resp)
        "BLPOP" -> parseBLPop(resp)
        "LLEN" -> parseLLen(resp)
        "LRANGE" -> parseLRange(resp)
        else -> Result.failure(ParseException("ERR unknown command '$commandName'"))
    }
}


fun parseEcho(resp: RespArray): Result<RedisCommand.Echo> =
    if (resp.elements.size != 2) {
        Result.failure(ParseException("ERR wrong number of arguments for 'echo' command: ${resp.elements.size}"))
    } else {
        Result.success(RedisCommand.Echo(resp.elements[1]))
    }

fun parseGet(resp: RespArray): Result<RedisCommand.Get> =
    if (resp.elements.size != 2) {
        Result.failure(ParseException("ERR wrong number of arguments for 'get' command: ${resp.elements.size}"))
    } else {
        Result.success(RedisCommand.Get(resp.elements[1]))
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

fun parseLLen(resp: RespArray): Result<RedisCommand.LLen> =
    if (resp.elements.size != 2) {
        Result.failure(ParseException("ERR wrong number of arguments for 'llen' command: ${resp.elements.size}"))
    } else {
        Result.success(RedisCommand.LLen(resp.elements[1]))
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