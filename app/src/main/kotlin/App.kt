import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.io.EOFException
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

enum class ConnectionState {
    Standard,
    Multi,
}

class ClientConnection(
    val channel: Channel<WritableRespValue>,
    val state: ConnectionState,
    val commandQueue: ArrayDeque<RedisCommand>
)

class RedisServer(
    private val host: String = "0.0.0.0",
    private val port: Int = 6379
) {
    private val dataStore = RedisDataStore()
    private val commandChannel = Channel<CommandRequest>(Channel.UNLIMITED)
    private val blockedMap = BlockedMap()
    private val connectionMap = ConcurrentHashMap<String, ClientConnection>()
    private val clientIdCounter = AtomicLong(0)

    suspend fun start() {
        val selectorManager = SelectorManager(Dispatchers.IO)
        val serverSocket = aSocket(selectorManager).tcp().bind(host, port)

        println("Server listening on port $port")

        coroutineScope {
            launch { runEventLoop() }

            while (true) {
                val socket = serverSocket.accept()
                println("Accepted new connection")
                launch { handleClient(socket) }
            }
        }
    }

    private suspend fun checkAndHandleTimeouts() {
        val expired = blockedMap.getClientsTimingOutBefore(Instant.now())
        expired.forEach {
            when (it.command) {
                is RedisCommand.XRead, is RedisCommand.BLPop -> connectionMap[it.clientId]?.channel?.send(RespNullArray)
                else -> connectionMap[it.clientId]?.channel?.send(RespNull)

            }
        }
    }


    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun runEventLoop() {
        while (true) {
            checkAndHandleTimeouts()
            val timeout = blockedMap.getEarliestTimeout()?.deadline ?: Instant.now().plusMillis(100)
            val timeoutMs = (timeout.toEpochMilli() - Instant.now().toEpochMilli())

            val request = select {
                commandChannel.onReceive { it }
                onTimeout(timeoutMs) { null }
            }

            if (request != null) {
                val response = executeCommand(request)
                if (response is WritableRespValue) request.responseChannel.send(response)
            }
        }
    }

    private suspend fun handleClient(socket: Socket) {
        val clientId = clientIdCounter.getAndIncrement().toString()
        val responseChannel = Channel<WritableRespValue>(Channel.UNLIMITED)
        connectionMap[clientId] = ClientConnection(responseChannel, ConnectionState.Standard, ArrayDeque())

        socket.use {
            val input = it.openReadChannel()
            val output = it.openWriteChannel(autoFlush = true)

            while (!input.isClosedForRead) {
                try {
                    val data = input.readRespValue()
                    println("Received: $data")

                    commandChannel.send(CommandRequest(data, responseChannel, clientId))
                    val response = responseChannel.receive()

                    if (response != NoResponse) {
                        output.writeRespValue(response)
                    }
                } catch (e: EOFException) {
                    println("Client disconnected")
                    break
                } catch (e: Exception) {
                    println("Error: ${e.message}")
                    break
                }
            }
        }

        connectionMap.remove(clientId)
        blockedMap.unblockClient(clientId)
    }

    private suspend fun executeCommand(request: CommandRequest): RespValue {
        val parseResult = parseCommand(request.command)

        if (parseResult.isFailure) {
            return RespSimpleError(parseResult.exceptionOrNull()?.message ?: "ERR parse failed")
        }

        val command = parseResult.getOrThrow()
        val context = ExecutionContext(
            dataStore = dataStore,
            blockedMap = blockedMap,
            responseChannels = connectionMap,
            clientId = request.clientId,
            checkTimeouts = ::checkAndHandleTimeouts
        )

        return executeRedisCommand(command, context)
    }

    private data class CommandRequest(
        val command: RespValue,
        val responseChannel: Channel<WritableRespValue>,
        val clientId: String
    )
}


fun main() = runBlocking {
    RedisServer().start()
}