import java.net.ServerSocket

fun main(args: Array<String>) {
    System.err.println("Logs from your program will appear here!")

     var serverSocket = ServerSocket(6379)

     serverSocket.reuseAddress = true

     serverSocket.accept()
     println("accepted new connection")
}
