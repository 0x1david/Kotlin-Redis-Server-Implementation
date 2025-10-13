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