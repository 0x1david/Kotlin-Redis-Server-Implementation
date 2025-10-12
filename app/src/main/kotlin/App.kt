import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readUTF8Line
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.*

fun main() = runBlocking {
    val selectorManager = SelectorManager(Dispatchers.IO)
    val serverSocket = aSocket(selectorManager).tcp().bind("0.0.0.0", 6379)

    println("Server listening on port 6379")

    while (true) {
        val socket = serverSocket.accept()
        println("Accepted new connection")

        launch {
            handleClient(socket)
        }
    }
}

suspend fun handleClient(socket: Socket) {
    socket.use {
        val input = it.openReadChannel()
        val output = it.openWriteChannel(autoFlush = true)

        while (!input.isClosedForRead) {
            val data = input.readRespValue()

            println("Received: $data")
            when (data) {
                is RespArray -> {
                    val values = data.elements
                    val first = values.first()
                    if (first is RespBulkString && first.value == "ECHO") {
                        require(values.size == 2) { "Variable length RespArray not yet implemented" }
                        output.writeRespValue(values[1])
                    } else {
                        output.writeFully("+PONG\r\n".toByteArray())
                    }
                }

                else -> {
                    output.writeFully("+PONG\r\n".toByteArray())
                }

            }

        }
    }
}