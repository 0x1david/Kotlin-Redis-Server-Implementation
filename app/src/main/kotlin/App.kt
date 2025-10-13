import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.io.EOFException

class RedisServer(
    private val host: String = "0.0.0.0",
    private val port: Int = 6379
) {
    private val dataStore = RedisDataStore()
    private val commandChannel = Channel<CommandRequest>(Channel.UNLIMITED)

    suspend fun start() {
        val selectorManager = SelectorManager(Dispatchers.IO)
        val serverSocket = aSocket(selectorManager).tcp().bind(host, port)

        println("Server listening on port $port")

        coroutineScope {
            launch { processCommands() }

            while (true) {
                val socket = serverSocket.accept()
                println("Accepted new connection")
                launch {
                    handleClient(socket)
                }
            }
        }
    }

    private suspend fun processCommands() {
        for (request in commandChannel) {
            val response = executeCommand(request.command)
            request.responseChannel.send(response)
        }
    }

    private suspend fun handleClient(socket: Socket) {
        socket.use {
            val input = it.openReadChannel()
            val output = it.openWriteChannel(autoFlush = true)

            while (!input.isClosedForRead) {
                try {
                    val data = input.readRespValue()
                    println("Received: $data")

                    val responseChannel = Channel<RespValue>(1)
                    commandChannel.send(CommandRequest(data, responseChannel))
                    val response = responseChannel.receive()
                    output.writeRespValue(response)
                } catch (e: EOFException) {
                    println("Client disconnected")
                    break
                } catch (e: Exception) {
                    println("Error: ${e.message}")
                    break
                }
            }
        }
    }

    private fun executeCommand(data: RespValue): RespValue {
        if (data !is RespArray) return RespSimpleString("PONG")

        val command = (data.elements.firstOrNull() as? RespBulkString)?.value?.uppercase()
            ?: return RespSimpleError("ERR invalid command format")

        return when (command) {
            "PING" -> RespSimpleString("PONG")
            "ECHO" -> data.validateAndExecute(2, "echo") { data.elements[1] }
            "GET" -> data.validateAndExecute(2, "get") { dataStore.get(data.elements[1]) }
            "SET" -> executeSet(data)
            "RPUSH" -> executePush(data)
            "LPUSH" -> executePush(data, true)
            "LLEN" -> data.validateAndExecute(2, "llen") {
                when (val item = dataStore.get(data.elements[1])) {
                    is RespArray -> RespInteger(item.elements.size.toLong())
                    else -> RespInteger(0)
                }
            }

            "LRANGE" -> lrange(data)

            else -> RespSimpleError("ERR unknown command '$command'")
        }
    }

    private inline fun RespArray.validateAndExecute(
        expectedSize: Int,
        command: String,
        block: () -> RespValue
    ): RespValue =
        if (elements.size != expectedSize)
            RespSimpleError("ERR wrong number of arguments for '$command' command: ${elements.size}")
        else block()


    private fun executeSet(data: RespArray): RespValue {
        val size = data.elements.size
        if (size % 2 == 0) return RespSimpleError("ERR wrong number of arguments for 'set' command: $size")

        val params = DataStoreParams()
        for (i in 3 until size step 2) {
            params.parseParameter(data.elements[i], data.elements[i + 1])
        }

        dataStore.set(data.elements[1], data.elements[2], params)
        return RespSimpleString("OK")
    }

    private fun executePush(data: RespArray, left: Boolean = false): RespValue {
        if (data.elements.size < 3) {
            return RespSimpleError("ERR wrong number of arguments for '${if (left) 'l' else 'r'}push' command: ${data.elements.size}")
        }
        var lst = dataStore.get(data.elements[1])
        if (lst is RespNull) {
            lst = RespArray(mutableListOf())
            dataStore.set(data.elements[1], lst)
        }
        if (lst !is RespArray) return RespSimpleError("Provided key doesn't correspond to an array.")

        for (el in data.elements.drop(2)) {
            if (left) lst.elements.addFirst(el) else lst.elements.add(el)
        }
        return RespInteger(lst.elements.size.toLong())
    }

    private fun lrange(data: RespArray): RespValue {
        if (data.elements.size != 4) {
            return RespSimpleError("ERR wrong number of arguments for 'rpush' command: ${data.elements.size}")
        }
        val startVal = data.elements[2]
        val endVal = data.elements[3]
        val lst = dataStore.get(data.elements[1])

        if (lst is RespNull) return RespArray(mutableListOf())
        if (lst !is RespArray) return RespSimpleError("Provided key doesn't correspond to an array.")
        val lstSize = lst.elements.size
        if (startVal !is RespBulkString) return RespSimpleError("Provided start index is not a bulk string.")
        if (endVal !is RespBulkString) return RespSimpleError("Provided end index is not a bulk string.")
        if (startVal.value == null) return RespSimpleError("Provided start index is null.")
        if (endVal.value == null) return RespSimpleError("Provided end index is null.")

        val start =
            startVal.value.toIntOrNull() ?: return RespSimpleError("Start index is not a valid integer.")
        val end = endVal.value.toIntOrNull() ?: return RespSimpleError("End index is not a valid integer.")

        val normalizedStart = if (start < 0) (lstSize + start).coerceAtLeast(0) else start.coerceAtMost(lstSize)
        val normalizedEnd = if (end < 0) (lstSize + end).coerceAtLeast(0) else end.coerceAtMost(lstSize - 1)

        return when {
            normalizedStart > normalizedEnd -> RespArray(mutableListOf())
            normalizedStart >= lstSize -> RespArray(mutableListOf())
            else -> RespArray(lst.elements.subList(normalizedStart, (normalizedEnd + 1)))
        }
    }

    private data class CommandRequest(
        val command: RespValue,
        val responseChannel: Channel<RespValue>
    )
}


fun main() = runBlocking {
    RedisServer().start()
}