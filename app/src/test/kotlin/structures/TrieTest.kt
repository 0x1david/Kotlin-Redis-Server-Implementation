import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach

class StreamTrieTest {
    private lateinit var trie: StreamTrie

    @BeforeEach
    fun setup() {
        trie = StreamTrie()
    }

    @Test
    fun `insert and search`() {
        val entry = createEntry(100u, 1u, "key1" to "value1")
        trie.insert(entry)

        val found = trie.search(StreamId(100u, 1u))
        assertNotNull(found)
        assertEquals(entry.id, found?.id)
        assertEquals(1, trie.size())
    }

    @Test
    fun `insert duplicate updates value`() {
        val entry1 = createEntry(100u, 1u, "key" to "old")
        val entry2 = createEntry(100u, 1u, "key" to "new")

        trie.insert(entry1)
        trie.insert(entry2)

        val found = trie.search(StreamId(100u, 1u))
        assertArrayEquals("new".toByteArray(), found?.fields?.get("key"))
        assertEquals(1, trie.size()) // Size shouldn't increase
    }

    @Test
    fun `delete existing entry`() {
        val entry = createEntry(100u, 1u)
        trie.insert(entry)

        val deleted = trie.delete(StreamId(100u, 1u))
        assertTrue(deleted)
        assertNull(trie.search(StreamId(100u, 1u)))
        assertEquals(0, trie.size())
    }

    @Test
    fun `delete non-existent entry`() {
        val deleted = trie.delete(StreamId(100u, 1u))
        assertFalse(deleted)
        assertEquals(0, trie.size())
    }

    @Test
    fun `rangeQuery returns entries in order`() {
        trie.insert(createEntry(100u, 1u))
        trie.insert(createEntry(100u, 5u))
        trie.insert(createEntry(100u, 3u))
        trie.insert(createEntry(200u, 1u))

        val results = trie.rangeQuery(
            StreamId(100u, 2u),
            StreamId(100u, 10u)
        ).toList()

        assertEquals(2, results.size)
        assertEquals(StreamId(100u, 3u), results[0].id)
        assertEquals(StreamId(100u, 5u), results[1].id)
    }

    @Test
    fun `rangeQuery with count limit`() {
        trie.insert(createEntry(100u, 1u))
        trie.insert(createEntry(100u, 2u))
        trie.insert(createEntry(100u, 3u))

        val results = trie.rangeQuery(
            StreamId(0u, 0u),
            StreamId(ULong.MAX_VALUE, ULong.MAX_VALUE),
            count = 2
        )

        assertEquals(2, results.size)
    }

    @Test
    fun `iterator returns all entries in order`() {
        trie.insert(createEntry(300u, 1u))
        trie.insert(createEntry(100u, 1u))
        trie.insert(createEntry(200u, 1u))

        val ids = trie.iterator().asSequence().map { it.id }.toList()

        assertEquals(3, ids.size)
        assertEquals(StreamId(100u, 1u), ids[0])
        assertEquals(StreamId(200u, 1u), ids[1])
        assertEquals(StreamId(300u, 1u), ids[2])
    }

    @Test
    fun `iteratorFrom starts at specified ID`() {
        trie.insert(createEntry(100u, 1u))
        trie.insert(createEntry(200u, 1u))
        trie.insert(createEntry(300u, 1u))

        val ids = trie.iteratorFrom(StreamId(200u, 1u))
            .asSequence()
            .map { it.id }
            .toList()

        assertEquals(2, ids.size)
        assertEquals(StreamId(200u, 1u), ids[0])
        assertEquals(StreamId(300u, 1u), ids[1])
    }

    @Test
    fun `trimBefore removes older entries`() {
        trie.insert(createEntry(100u, 1u))
        trie.insert(createEntry(200u, 1u))
        trie.insert(createEntry(300u, 1u))

        val deleted = trie.trimBefore(StreamId(250u, 0u))

        assertEquals(2, deleted)
        assertEquals(1, trie.size())
        assertNotNull(trie.search(StreamId(300u, 1u)))
    }

    @Test
    fun `trimToMaxLength removes oldest entries`() {
        trie.insert(createEntry(100u, 1u))
        trie.insert(createEntry(200u, 1u))
        trie.insert(createEntry(300u, 1u))
        trie.insert(createEntry(400u, 1u))

        val deleted = trie.trimToMaxLength(2)

        assertEquals(2, deleted)
        assertEquals(2, trie.size())
        assertNull(trie.search(StreamId(100u, 1u)))
        assertNull(trie.search(StreamId(200u, 1u)))
        assertNotNull(trie.search(StreamId(300u, 1u)))
    }

    @Test
    fun `StreamId comparison and serialization`() {
        val id1 = StreamId(100u, 5u)
        val id2 = StreamId(100u, 10u)
        val id3 = StreamId(200u, 1u)

        assertTrue(id1 < id2)
        assertTrue(id2 < id3)

        val bytes = id1.toBytes()
        val reconstructed = StreamId.fromBytes(bytes)
        assertEquals(id1, reconstructed)
    }

    @Test
    fun `StreamId parse from string`() {
        val id = StreamId.parse("1609459200000-5")
        assertEquals(1609459200000u, id.timestampMs)
        assertEquals(5uL, id.sequence)
    }

    private fun createEntry(
        timestamp: ULong,
        sequence: ULong,
        vararg fields: Pair<String, String>
    ): StreamEntry {
        return StreamEntry(
            id = StreamId(timestamp, sequence),
            fields = fields.associate { it.first to it.second.toByteArray() }
        )
    }
}