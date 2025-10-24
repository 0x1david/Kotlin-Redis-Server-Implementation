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
    Subscribed,
}

typealias UserId = String


class ClientConnection(
    var state: ConnectionState,
    val commandQueue: ArrayDeque<RedisCommand>,
    var subCount: Long = 0
) {
    companion object : () -> ClientConnection {
        override operator fun invoke(): ClientConnection = ClientConnection(
            ConnectionState.Standard,
            ArrayDeque()
        )
    }
}


class RedisServer(
    private val host: String = "0.0.0.0",
    private val port: Int = 6379
) {
    private val dataStore = RedisDataStore()
    private val commandChannel = Channel<CommandRequest>(Channel.UNLIMITED)
    private val blockedMap = BlockedMap()
    private val clientToConnectionInfo = ConcurrentHashMap<UserId, ClientConnection>()
    private val clientToChannel = ConcurrentHashMap<UserId, Channel<WritableRespValue>>()
    private val publisherToSubscriber = ConcurrentHashMap<String, MutableSet<UserId>>()
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
                is RedisCommand.XRead, is RedisCommand.BLPop -> clientToChannel[it.clientId]?.send(RespNullArray)
                else -> clientToChannel[it.clientId]?.send(RespNull)

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
        clientToConnectionInfo[clientId] = ClientConnection(ConnectionState.Standard, ArrayDeque())
        clientToChannel[clientId] = responseChannel

        socket.use {
            val input = it.openReadChannel()
            val output = it.openWriteChannel(autoFlush = true)

            coroutineScope {
                val responseJob = launch {
                    for (response in responseChannel) {
                        if (response != NoResponse) {
                            output.writeRespValue(response)
                        }
                    }
                }

                try {
                    while (!input.isClosedForRead) {
                        val data = input.readRespValue()
                        println("Received: $data")
                        commandChannel.send(CommandRequest(data, responseChannel, clientId))
                    }
                } catch (e: EOFException) {
                    println("Client disconnected")
                } catch (e: Exception) {
                    println("Error: ${e.message}")
                } finally {
                    responseJob.cancel()
                }
            }
        }

        clientToConnectionInfo.remove(clientId)
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
            responseChannels = clientToChannel,
            connection = clientToConnectionInfo.getOrPut(request.clientId, ClientConnection),
            pubSubMap = publisherToSubscriber,
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