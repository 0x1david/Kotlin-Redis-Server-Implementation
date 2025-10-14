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
        blockedMap.blockClient("client1", listOf(key1))

        assertEquals("client1", blockedMap.getNextClientForKey(key1))
        assertNull(blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `blocking multiple clients on same key returns FIFO order`() {
        blockedMap.blockClient("client1", listOf(key1))
        blockedMap.blockClient("client2", listOf(key1))
        blockedMap.blockClient("client3", listOf(key1))

        assertEquals("client1", blockedMap.getNextClientForKey(key1))
        assertEquals("client2", blockedMap.getNextClientForKey(key1))
        assertEquals("client3", blockedMap.getNextClientForKey(key1))
        assertNull(blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `blocking client on multiple keys removes from all when unblocked`() {
        blockedMap.blockClient("client1", listOf(key1, key2, key3))

        assertEquals("client1", blockedMap.getNextClientForKey(key1))
        // Should be removed from all other keys
        assertNull(blockedMap.getNextClientForKey(key2))
        assertNull(blockedMap.getNextClientForKey(key3))
    }

    @Test
    fun `multiple clients on different keys are independent`() {
        blockedMap.blockClient("client1", listOf(key1))
        blockedMap.blockClient("client2", listOf(key2))

        assertEquals("client2", blockedMap.getNextClientForKey(key2))
        assertEquals("client1", blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `timeout returns expired clients and removes them`() {
        val now = Instant.now()
        blockedMap.blockClient("client1", listOf(key1), timeoutSec = 1)
        blockedMap.blockClient("client2", listOf(key2), timeoutSec = 5)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(3))

        assertEquals(listOf("client1"), expired)
        assertNull(blockedMap.getNextClientForKey(key1))
        assertEquals("client2", blockedMap.getNextClientForKey(key2))
    }

    @Test
    fun `timeout returns multiple expired clients in deadline order`() {
        val now = Instant.now()
        blockedMap.blockClient("client1", listOf(key1), timeoutSec = 2)
        blockedMap.blockClient("client2", listOf(key2), timeoutSec = 1)
        blockedMap.blockClient("client3", listOf(key3), timeoutSec = 3)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(5))

        assertEquals(3, expired.size)
        assertEquals("client2", expired[0]) // Shortest timeout first
    }

    @Test
    fun `timeout skips already unblocked clients`() {
        val now = Instant.now()
        blockedMap.blockClient("client1", listOf(key1), timeoutSec = 5)

        // Unblock by servicing the key
        blockedMap.getNextClientForKey(key1)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(10))
        assertTrue(expired.isEmpty())
    }

    @Test
    fun `no timeout when null timeoutSec`() {
        val now = Instant.now()
        blockedMap.blockClient("client1", listOf(key1), timeoutSec = null)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(1000))
        assertTrue(expired.isEmpty())
        assertEquals("client1", blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `client blocked on multiple keys with timeout removed from all on timeout`() {
        val now = Instant.now()
        blockedMap.blockClient("client1", listOf(key1, key2), timeoutSec = 1)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(2))

        assertEquals(listOf("client1"), expired)
        assertNull(blockedMap.getNextClientForKey(key1))
        assertNull(blockedMap.getNextClientForKey(key2))
    }

    @Test
    fun `mixed timeout and non-timeout clients`() {
        val now = Instant.now()
        blockedMap.blockClient("client1", listOf(key1), timeoutSec = null)
        blockedMap.blockClient("client2", listOf(key1), timeoutSec = 1)

        val expired = blockedMap.getClientsTimingOutBefore(now.plusSeconds(2))

        assertEquals(listOf("client2"), expired)
        assertEquals("client1", blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `getNextClientForKey on non-existent key returns null`() {
        assertNull(blockedMap.getNextClientForKey(key1))
    }

    @Test
    fun `same client blocked multiple times on different keys`() {
        blockedMap.blockClient("client1", listOf(key1))
        blockedMap.blockClient("client1", listOf(key2))

        assertEquals("client1", blockedMap.getNextClientForKey(key1))
        // Client already fully unblocked
        assertNull(blockedMap.getNextClientForKey(key2))
    }
}