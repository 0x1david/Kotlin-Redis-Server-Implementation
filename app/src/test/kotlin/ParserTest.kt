import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.ktor.utils.io.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.io.EOFException
import java.net.ProtocolException

class RespParserTest {

    @Test
    fun `simple string parses correctly`() = runTest {
        val channel = ByteReadChannel("+OK\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespSimpleString("OK")
    }

    @Test
    fun `simple string throws on missing newline`() = runTest {
        val channel = ByteReadChannel("+OK")
        val parser = RespParser(channel)

        shouldThrow<EOFException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `simple error parses correctly`() = runTest {
        val channel = ByteReadChannel("-ERR unknown command\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespSimpleError("ERR unknown command")
    }

    @Test
    fun `simple error throws on missing newline`() = runTest {
        val channel = ByteReadChannel("-ERR")
        val parser = RespParser(channel)

        shouldThrow<EOFException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `integer parses correctly`() = runTest {
        val channel = ByteReadChannel(":1000\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespInteger(1000)
    }

    @Test
    fun `integer throws on invalid format`() = runTest {
        val channel = ByteReadChannel(":notanumber\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `bulk string parses correctly`() = runTest {
        val channel = ByteReadChannel("$5\r\nhello\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespBulkString("hello")
    }

    @Test
    fun `bulk string returns null for -1 length`() = runTest {
        val channel = ByteReadChannel("$-1\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespNull
    }

    @Test
    fun `array parses correctly`() = runTest {
        val channel = ByteReadChannel("*2\r\n+hello\r\n+world\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()
        val arr = result as RespArray
        arr.elements.shouldContainExactly(
            listOf(
                RespSimpleString("hello"),
                RespSimpleString("world")
            )
        )
    }

    @Test
    fun `array throws on invalid length`() = runTest {
        val channel = ByteReadChannel("*invalid\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `bool parses true correctly`() = runTest {
        val channel = ByteReadChannel("#t\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespBool(true)
    }

    @Test
    fun `bool throws on invalid value`() = runTest {
        val channel = ByteReadChannel("#x\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `double parses correctly`() = runTest {
        val channel = ByteReadChannel(",3.14\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespDouble(3.14)
    }

    @Test
    fun `double throws on invalid format`() = runTest {
        val channel = ByteReadChannel(",notadouble\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `big number parses correctly`() = runTest {
        val channel = ByteReadChannel("(3492890328409238509324850943850943825024385\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespBigNumber("3492890328409238509324850943850943825024385")
    }

    @Test
    fun `big number throws on missing newline`() = runTest {
        val channel = ByteReadChannel("(12345")
        val parser = RespParser(channel)

        shouldThrow<EOFException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `bulk error parses correctly`() = runTest {
        val channel = ByteReadChannel("!21\r\nSYNTAX invalid syntax\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespBulkError("SYNTAX invalid syntax")
    }

    @Test
    fun `bulk error returns null for -1 length`() = runTest {
        val channel = ByteReadChannel("!-1\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespNull
    }

    @Test
    fun `verbatim string parses correctly`() = runTest {
        val channel = ByteReadChannel("=15\r\ntxt:Some string\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespVerbatimString("txt", "Some string")
    }

    @Test
    fun `verbatim string throws on missing colon`() = runTest {
        val channel = ByteReadChannel("=15\r\ntxtXSome string\r\n")
        val parser = RespParser(channel)

        shouldThrow<IllegalArgumentException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `map parses correctly`() = runTest {
        val channel = ByteReadChannel("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespMap(
            mapOf(
                RespSimpleString("first") to RespInteger(1),
                RespSimpleString("second") to RespInteger(2)
            )
        )
    }

    @Test
    fun `map throws on invalid length`() = runTest {
        val channel = ByteReadChannel("%bad\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `attributes parse correctly`() = runTest {
        val channel = ByteReadChannel("|1\r\n+key\r\n+value\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespAttributes(
            mapOf(
                RespSimpleString("key") to RespSimpleString("value")
            )
        )
    }

    @Test
    fun `attributes throw on invalid length`() = runTest {
        val channel = ByteReadChannel("|invalid\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `set parses correctly`() = runTest {
        val channel = ByteReadChannel("~3\r\n+apple\r\n+banana\r\n+cherry\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespSet(
            setOf(
                RespSimpleString("apple"),
                RespSimpleString("banana"),
                RespSimpleString("cherry")
            )
        )
    }

    @Test
    fun `set throws on invalid length`() = runTest {
        val channel = ByteReadChannel("~bad\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `push parses correctly`() = runTest {
        val channel = ByteReadChannel(">2\r\n+message\r\n+data\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespPush(
            listOf(
                RespSimpleString("message"),
                RespSimpleString("data")
            )
        )
    }

    @Test
    fun `push throws on invalid length`() = runTest {
        val channel = ByteReadChannel(">invalid\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }

    @Test
    fun `null parses correctly`() = runTest {
        val channel = ByteReadChannel("_\r\n")
        val parser = RespParser(channel)

        val result = parser.readRespPayload()

        result shouldBe RespNull
    }

    @Test
    fun `unsupported type throws exception`() = runTest {
        val channel = ByteReadChannel("@unsupported\r\n")
        val parser = RespParser(channel)

        shouldThrow<ProtocolException> {
            parser.readRespPayload()
        }
    }
}