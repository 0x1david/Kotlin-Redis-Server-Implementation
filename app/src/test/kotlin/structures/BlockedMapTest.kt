import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Assertions.*
import java.time.Instant


class BlockedMapTest {
    private lateinit var blockedMap: BlockedMap
    private val key1 = RespBulkString("key1")
    private val key2 = RespBulkString("key2")
    private val key3 = RespBulkString("key3")

    @BeforeEach
    fun setup() {
        blockedMap = BlockedMap()
    }

    @Test
    fun `blocking single client on single key returns that client`() {
        val command = RedisCommand.BLPop(key1, 0.0)
        blockedMap.blockClient("client1", listOf(key1), command, NO_TIMEOUT)

        val result = blockedMap.getNextClientForKey(key1)
        assertEquals("client1", result?.clientId)
        assertNull(blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `blocking multiple clients on same key returns FIFO order`() {
        val command = RedisCommand.BLPop(key1, 0.0)
        blockedMap.blockClient("client1", listOf(key1), command, NO_TIMEOUT)
        blockedMap.blockClient("client2", listOf(key1), command, NO_TIMEOUT)
        blockedMap.blockClient("client3", listOf(key1), command, NO_TIMEOUT)

        assertEquals("client1", blockedMap.getNextClientForKey(key1)?.clientId)
        assertEquals("client2", blockedMap.getNextClientForKey(key1)?.clientId)
        assertEquals("client3", blockedMap.getNextClientForKey(key1)?.clientId)
        assertNull(blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `blocking client on multiple keys removes from all when unblocked`() {
        val command = RedisCommand.XRead(listOf(key1 to "0-0", key2 to "0-0", key3 to "0-0"), null)
        blockedMap.blockClient("client1", listOf(key1, key2, key3), command, NO_TIMEOUT)

        assertEquals("client1", blockedMap.getNextClientForKey(key1)?.clientId)
        assertNull(blockedMap.getNextClientForKey(key2))
        assertNull(blockedMap.getNextClientForKey(key3))
    }

    @Test
    fun `multiple clients on different keys are independent`() {
        val command1 = RedisCommand.BLPop(key1, 0.0)
        val command2 = RedisCommand.BLPop(key2, 0.0)
        blockedMap.blockClient("client1", listOf(key1), command1, NO_TIMEOUT)
        blockedMap.blockClient("client2", listOf(key2), command2, NO_TIMEOUT)

        assertEquals("client2", blockedMap.getNextClientForKey(key2)?.clientId)
        assertEquals("client1", blockedMap.getNextClientForKey(key1)?.clientId)
    }

    @Test
    fun `timeout returns expired clients and removes them`() {
        val now = Instant.now()
        val command1 = RedisCommand.BLPop(key1, 1.0)
        val command2 = RedisCommand.BLPop(key2, 5.0)
        blockedMap.blockClient("client1", listOf(key1), command1, timeoutSec = 1.0)
        blockedMap.blockClient("client2", listOf(key2), command2, timeoutSec = 5.0)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(3))

        assertEquals(1, expired.size)
        assertEquals("client1", expired[0].clientId)
        assertNull(blockedMap.getNextClientForKey(key1))
        assertEquals("client2", blockedMap.getNextClientForKey(key2)?.clientId)
    }

    @Test
    fun `timeout returns multiple expired clients in deadline order`() {
        val now = Instant.now()
        val command1 = RedisCommand.BLPop(key1, 2.0)
        val command2 = RedisCommand.BLPop(key2, 1.0)
        val command3 = RedisCommand.BLPop(key3, 3.0)
        blockedMap.blockClient("client1", listOf(key1), command1, timeoutSec = 2.0)
        blockedMap.blockClient("client2", listOf(key2), command2, timeoutSec = 1.0)
        blockedMap.blockClient("client3", listOf(key3), command3, timeoutSec = 3.0)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(5))

        assertEquals(3, expired.size)
        assertEquals("client2", expired[0].clientId)
    }

    @Test
    fun `timeout skips already unblocked clients`() {
        val now = Instant.now()
        val command = RedisCommand.BLPop(key1, 5.0)
        blockedMap.blockClient("client1", listOf(key1), command, timeoutSec = 5.0)

        blockedMap.getNextClientForKey(key1)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(10))
        assertTrue(expired.isEmpty())
    }

    @Test
    fun `no timeout when null timeoutSec`() {
        val now = Instant.now()
        val command = RedisCommand.BLPop(key1, 0.0)
        blockedMap.blockClient("client1", listOf(key1), command, NO_TIMEOUT)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(1000))
        assertTrue(expired.isEmpty())
        assertEquals("client1", blockedMap.getNextClientForKey(key1)?.clientId)
    }

    @Test
    fun `client blocked on multiple keys with timeout removed from all on timeout`() {
        val now = Instant.now()
        val command = RedisCommand.XRead(listOf(key1 to "0-0", key2 to "0-0"), 1.0)
        blockedMap.blockClient("client1", listOf(key1, key2), command, timeoutSec = 1.0)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(2))

        assertEquals(1, expired.size)
        assertEquals("client1", expired[0].clientId)
        assertNull(blockedMap.getNextClientForKey(key1))
        assertNull(blockedMap.getNextClientForKey(key2))
    }

    @Test
    fun `mixed timeout and non-timeout clients`() {
        val now = Instant.now()
        val command1 = RedisCommand.BLPop(key1, 0.0)
        val command2 = RedisCommand.BLPop(key1, 1.0)
        blockedMap.blockClient("client1", listOf(key1), command1, NO_TIMEOUT)
        blockedMap.blockClient("client2", listOf(key1), command2, timeoutSec = 1.0)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(2))

        assertEquals(1, expired.size)
        assertEquals("client2", expired[0].clientId)
        assertEquals("client1", blockedMap.getNextClientForKey(key1)?.clientId)
    }

    @Test
    fun `getNextClientForKey on non-existent key returns null`() {
        assertNull(blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `same client blocked multiple times on different keys`() {
        val command1 = RedisCommand.BLPop(key1, 0.0)
        val command2 = RedisCommand.BLPop(key2, 0.0)
        blockedMap.blockClient("client1", listOf(key1), command1, NO_TIMEOUT)
        blockedMap.blockClient("client1", listOf(key2), command2, NO_TIMEOUT)

        assertEquals("client1", blockedMap.getNextClientForKey(key1)?.clientId)

        assertNull(blockedMap.getNextClientForKey(key2))
    }
}