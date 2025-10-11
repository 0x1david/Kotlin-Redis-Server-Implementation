import kotlinx.coroutines.*
import kotlinx.coroutines.Dispatchers.IO
import java.net.*

fun main() = runBlocking {
    val serverSocket = ServerSocket(6379).apply {
        reuseAddress = true
    }

    println("Server listening on port 6379")

    while (true) {
        val clientSocket = withContext(IO) {
            serverSocket.accept()
        }

        println("Accepted new connection")

        launch(IO) {
            handleClient(clientSocket)
        }
    }
}

suspend fun handleClient(socket: Socket) {
    socket.use {
        val reader = it.getInputStream().bufferedReader()
        val writer = it.getOutputStream().bufferedWriter()

        while (isActive && !socket.isClosed) {
            val line = reader.readLine() ?: break
            println("Received: $line")

            writer.write("+PONG\r\n")
            writer.flush()
        }
    }
}