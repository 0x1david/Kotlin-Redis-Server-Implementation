import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*

class SkipListTest {

    private lateinit var skipList: SkipList

    @BeforeEach
    fun setup() {
        skipList = SkipList()
    }

    @Nested
    inner class AddTests {

        @Test
        fun `add single element`() {
            val result = skipList.add(RespBulkString("a"), 1.0)

            assertTrue(result)
            assertEquals(1L, skipList.getSize())
            assertFalse(skipList.isEmpty())
        }

        @Test
        fun `add multiple elements in order`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            assertEquals(3L, skipList.getSize())
        }

        @Test
        fun `add multiple elements out of order`() {
            skipList.add(RespBulkString("c"), 3.0)
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)

            assertEquals(3L, skipList.getSize())
        }

        @Test
        fun `add duplicate returns false and does not increase size`() {
            skipList.add(RespBulkString("a"), 1.0)
            val result = skipList.add(RespBulkString("a"), 1.0)

            assertFalse(result)
            assertEquals(1L, skipList.getSize())
        }

        @Test
        fun `add elements with same score but different members`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 1.0)
            skipList.add(RespBulkString("c"), 1.0)

            assertEquals(3L, skipList.getSize())
        }

        @Test
        fun `add elements with same member but different scores`() {
            skipList.add(RespBulkString("a"), 1.0)
            val result = skipList.add(RespBulkString("a"), 2.0)

            assertTrue(result)
            assertEquals(2L, skipList.getSize())
        }

        @Test
        fun `add many elements`() {
            repeat(1000) { i ->
                skipList.add(RespBulkString("member$i"), i.toDouble())
            }

            assertEquals(1000L, skipList.getSize())
        }
    }

    @Nested
    inner class GetRankTests {

        @Test
        fun `getRank of single element`() {
            skipList.add(RespBulkString("a"), 1.0)

            assertEquals(0L, skipList.getRank(RespBulkString("a"), 1.0))
        }

        @Test
        fun `getRank returns correct positions`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            assertEquals(0L, skipList.getRank(RespBulkString("a"), 1.0))
            assertEquals(1L, skipList.getRank(RespBulkString("b"), 2.0))
            assertEquals(2L, skipList.getRank(RespBulkString("c"), 3.0))
        }

        @Test
        fun `getRank with elements added out of order`() {
            skipList.add(RespBulkString("c"), 3.0)
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)

            assertEquals(0L, skipList.getRank(RespBulkString("a"), 1.0))
            assertEquals(1L, skipList.getRank(RespBulkString("b"), 2.0))
            assertEquals(2L, skipList.getRank(RespBulkString("c"), 3.0))
        }

        @Test
        fun `getRank returns null for non-existent element`() {
            skipList.add(RespBulkString("a"), 1.0)

            assertNull(skipList.getRank(RespBulkString("b"), 2.0))
            assertNull(skipList.getRank(RespBulkString("a"), 2.0))
        }

        @Test
        fun `getRank with same score orders by member lexicographically`() {
            skipList.add(RespBulkString("b"), 1.0)
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("c"), 1.0)

            assertEquals(0L, skipList.getRank(RespBulkString("a"), 1.0))
            assertEquals(1L, skipList.getRank(RespBulkString("b"), 1.0))
            assertEquals(2L, skipList.getRank(RespBulkString("c"), 1.0))
        }

        @Test
        fun `getRank in large list`() {
            repeat(1000) { i ->
                skipList.add(RespBulkString("member$i"), i.toDouble())
            }

            assertEquals(0L, skipList.getRank(RespBulkString("member0"), 0.0))
            assertEquals(500L, skipList.getRank(RespBulkString("member500"), 500.0))
            assertEquals(999L, skipList.getRank(RespBulkString("member999"), 999.0))
        }
    }

    @Nested
    inner class GetByRankTests {

        @Test
        fun `getByRank of single element`() {
            skipList.add(RespBulkString("a"), 1.0)

            val node = skipList.getByRank(0)
            assertNotNull(node)
            assertEquals("a", node!!.member.value)
            assertEquals(1.0, node.score)
        }

        @Test
        fun `getByRank returns correct elements`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            assertEquals("a", skipList.getByRank(0)?.member?.value)
            assertEquals("b", skipList.getByRank(1)?.member?.value)
            assertEquals("c", skipList.getByRank(2)?.member?.value)
        }

        @Test
        fun `getByRank returns null for negative index`() {
            skipList.add(RespBulkString("a"), 1.0)

            assertNull(skipList.getByRank(-1))
        }

        @Test
        fun `getByRank returns null for index greater than size`() {
            skipList.add(RespBulkString("a"), 1.0)

            assertNull(skipList.getByRank(1))
            assertNull(skipList.getByRank(100))
        }

        @Test
        fun `getByRank returns null for empty list`() {
            assertNull(skipList.getByRank(0))
        }

        @Test
        fun `getByRank in large list`() {
            repeat(1000) { i ->
                skipList.add(RespBulkString("member$i"), i.toDouble())
            }

            assertEquals("member0", skipList.getByRank(0)?.member?.value)
            assertEquals("member500", skipList.getByRank(500)?.member?.value)
            assertEquals("member999", skipList.getByRank(999)?.member?.value)
        }
    }

    @Nested
    inner class RemoveTests {

        @Test
        fun `remove single element`() {
            skipList.add(RespBulkString("a"), 1.0)
            val result = skipList.remove(RespBulkString("a"), 1.0)

            assertTrue(result)
            assertEquals(0L, skipList.getSize())
            assertTrue(skipList.isEmpty())
        }

        @Test
        fun `remove from middle`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            skipList.remove(RespBulkString("b"), 2.0)

            assertEquals(2L, skipList.getSize())
            assertEquals(0L, skipList.getRank(RespBulkString("a"), 1.0))
            assertEquals(1L, skipList.getRank(RespBulkString("c"), 3.0))
        }

        @Test
        fun `remove from start`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            skipList.remove(RespBulkString("a"), 1.0)

            assertEquals(2L, skipList.getSize())
            assertEquals(0L, skipList.getRank(RespBulkString("b"), 2.0))
            assertEquals(1L, skipList.getRank(RespBulkString("c"), 3.0))
        }

        @Test
        fun `remove from end`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            skipList.remove(RespBulkString("c"), 3.0)

            assertEquals(2L, skipList.getSize())
            assertEquals(0L, skipList.getRank(RespBulkString("a"), 1.0))
            assertEquals(1L, skipList.getRank(RespBulkString("b"), 2.0))
        }

        @Test
        fun `remove non-existent element returns false`() {
            skipList.add(RespBulkString("a"), 1.0)
            val result = skipList.remove(RespBulkString("b"), 2.0)

            assertFalse(result)
            assertEquals(1L, skipList.getSize())
        }

        @Test
        fun `remove with wrong score returns false`() {
            skipList.add(RespBulkString("a"), 1.0)
            val result = skipList.remove(RespBulkString("a"), 2.0)

            assertFalse(result)
            assertEquals(1L, skipList.getSize())
        }

        @Test
        fun `remove all elements`() {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            skipList.remove(RespBulkString("a"), 1.0)
            skipList.remove(RespBulkString("b"), 2.0)
            skipList.remove(RespBulkString("c"), 3.0)

            assertEquals(0L, skipList.getSize())
            assertTrue(skipList.isEmpty())
        }

        @Test
        fun `ranks update correctly after removal`() {
            repeat(10) { i ->
                skipList.add(RespBulkString("member$i"), i.toDouble())
            }

            skipList.remove(RespBulkString("member5"), 5.0)

            assertEquals(9L, skipList.getSize())
            assertEquals(4L, skipList.getRank(RespBulkString("member4"), 4.0))
            assertEquals(5L, skipList.getRank(RespBulkString("member6"), 6.0))
            assertNull(skipList.getRank(RespBulkString("member5"), 5.0))
        }
    }

    @Nested
    inner class CollectRangeTests {

        @Test
        fun `collectRange returns all elements by default`() = runBlocking {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            val nodes = sequence {
                with(skipList) {
                    collectRange()
                }
            }.toList()

            assertEquals(3, nodes.size)
            assertEquals("a", nodes[0].member.value)
            assertEquals("b", nodes[1].member.value)
            assertEquals("c", nodes[2].member.value)
        }

        @Test
        fun `collectRange with min and max score`() = runBlocking {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)
            skipList.add(RespBulkString("d"), 4.0)
            skipList.add(RespBulkString("e"), 5.0)

            val nodes = sequence {
                with(skipList) {
                    collectRange(minScore = 2.0, maxScore = 4.0)
                }
            }.toList()

            assertEquals(3, nodes.size)
            assertEquals("b", nodes[0].member.value)
            assertEquals("c", nodes[1].member.value)
            assertEquals("d", nodes[2].member.value)
        }

        @Test
        fun `collectRange with reverse`() = runBlocking {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)

            val nodes = sequence {
                with(skipList) {
                    collectRange(reverse = true)
                }
            }.toList()

            assertEquals(3, nodes.size)
            assertEquals("c", nodes[0].member.value)
            assertEquals("b", nodes[1].member.value)
            assertEquals("a", nodes[2].member.value)
        }

        @Test
        fun `collectRange reverse with score range`() = runBlocking {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)
            skipList.add(RespBulkString("c"), 3.0)
            skipList.add(RespBulkString("d"), 4.0)
            skipList.add(RespBulkString("e"), 5.0)

            val nodes = sequence {
                with(skipList) {
                    collectRange(minScore = 2.0, maxScore = 4.0, reverse = true)
                }
            }.toList()

            assertEquals(3, nodes.size)
            assertEquals("d", nodes[0].member.value)
            assertEquals("c", nodes[1].member.value)
            assertEquals("b", nodes[2].member.value)
        }

        @Test
        fun `collectRange returns empty for no matches`() = runBlocking {
            skipList.add(RespBulkString("a"), 1.0)
            skipList.add(RespBulkString("b"), 2.0)

            val nodes = sequence {
                with(skipList) {
                    collectRange(minScore = 5.0, maxScore = 10.0)
                }
            }.toList()

            assertEquals(0, nodes.size)
        }

        @Test
        fun `collectRange on empty list`() = runBlocking {
            val nodes = sequence {
                with(skipList) {
                    collectRange()
                }
            }.toList()

            assertEquals(0, nodes.size)
        }
    }

    @Nested
    inner class IntegrationTests {

        @Test
        fun `add, getRank, getByRank consistency`() {
            repeat(100) { i ->
                skipList.add(RespBulkString("member$i"), i.toDouble())
            }

            repeat(100) { i ->
                val rank = skipList.getRank(RespBulkString("member$i"), i.toDouble())
                assertEquals(i.toLong(), rank)

                val node = skipList.getByRank(i.toLong())
                assertNotNull(node)
                assertEquals("member$i", node!!.member.value)
                assertEquals(i.toDouble(), node.score)
            }
        }

        @Test
        fun `add, remove, getRank consistency`() {
            repeat(10) { i ->
                skipList.add(RespBulkString("member$i"), i.toDouble())
            }

            // Remove every other element
            for (i in 0 until 10 step 2) {
                skipList.remove(RespBulkString("member$i"), i.toDouble())
            }

            assertEquals(5L, skipList.getSize())

            // Check remaining elements have correct ranks
            assertEquals(0L, skipList.getRank(RespBulkString("member1"), 1.0))
            assertEquals(1L, skipList.getRank(RespBulkString("member3"), 3.0))
            assertEquals(2L, skipList.getRank(RespBulkString("member5"), 5.0))
            assertEquals(3L, skipList.getRank(RespBulkString("member7"), 7.0))
            assertEquals(4L, skipList.getRank(RespBulkString("member9"), 9.0))
        }

        @Test
        fun `stress test with random operations`() {
            val random = kotlin.random.Random(42)
            val expected = mutableMapOf<Pair<String, Double>, Boolean>()

            // Random adds
            repeat(500) {
                val member = "m${random.nextInt(200)}"
                val score = random.nextDouble(0.0, 100.0)
                val key = member to score

                val wasAdded = skipList.add(RespBulkString(member), score)
                val shouldAdd = !expected.containsKey(key)
                assertEquals(shouldAdd, wasAdded, "Add consistency failed for $key")

                if (wasAdded) {
                    expected[key] = true
                }
            }

            assertEquals(expected.size.toLong(), skipList.getSize())

            // Random removes
            val toRemove = expected.keys.take(expected.size / 2)
            toRemove.forEach { (member, score) ->
                val wasRemoved = skipList.remove(RespBulkString(member), score)
                assertTrue(wasRemoved, "Should have removed $member:$score")
                expected.remove(member to score)
            }

            assertEquals(expected.size.toLong(), skipList.getSize())
        }

        @Test
        fun `collectRange matches getRank ordering`() = runBlocking {
            repeat(50) { i ->
                skipList.add(RespBulkString("member$i"), i.toDouble())
            }

            val nodes = sequence {
                with(skipList) {
                    collectRange()
                }
            }.toList()

            nodes.forEachIndexed { index, node ->
                val rank = skipList.getRank(node.member, node.score)
                assertEquals(index.toLong(), rank, "Rank mismatch for ${node.member.value}")
            }
        }
    }
}