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
            "ECHO" -> {
                if (data.elements.size != 2) {
                    return RespSimpleError("ERR wrong number of arguments for 'echo' command: ${data.elements.size}")
                }
                data.elements[1]
            }

            "GET" -> {
                if (data.elements.size != 2) {
                    return RespSimpleError("ERR wrong number of arguments for 'get' command: ${data.elements.size}")
                }
                dataStore.get(data.elements[1])
            }

            "SET" -> {
                val size = data.elements.size
                if (size % 2 == 0) {
                    return RespSimpleError("ERR wrong number of arguments for 'set' command: $size")
                }
                val params = DataStoreParams()
                for (i in 3 until size step 2) {
                    params.parseParameter(data.elements[i], data.elements[i + 1])
                }

                dataStore.set(data.elements[1], data.elements[2], params)
                RespSimpleString("OK")
            }

            "RPUSH" -> {
                if (data.elements.size < 3) {
                    return RespSimpleError("ERR wrong number of arguments for 'rpush' command: ${data.elements.size}")
                }
                var lst = dataStore.get(data.elements[1])
                if (lst is RespNull) {
                    lst = RespArray(mutableListOf())
                    dataStore.set(data.elements[1], lst)
                }
                if (lst !is RespArray) {
                    return RespSimpleError("Provided key doesn't correspond to an array.")
                }
                for (el in data.elements.drop(2)) {
                    lst.elements.add(el)
                }
                RespInteger(lst.elements.size.toLong())
            }

            "LRANGE" -> {
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

                val normalizedStart =
                    if (start < 0) (lstSize + start).coerceAtLeast(0) else start.coerceAtMost(lstSize)
                val normalizedEnd =
                    if (end < 0) (lstSize + end).coerceAtLeast(0) else end.coerceAtMost(lstSize - 1)

                return when {
                    normalizedStart > normalizedEnd -> RespArray(mutableListOf())
                    normalizedStart >= lstSize -> RespArray(mutableListOf())
                    else -> RespArray(
                        lst.elements.subList(normalizedStart, (normalizedEnd + 1).coerceAtMost(lstSize))
                            .toMutableList()
                    )
                }
            }

            else -> RespSimpleError("ERR unknown command '$command'")
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