import java.time.Instant
import java.time.Duration

data class DataStoreValue(
    val value: RespValue,
    val expiry: Instant?
)

class RedisDataStore {
    private val data = HashMap<RespValue, DataStoreValue>()

    fun get(k: RespValue): RespValue {
        val value = data[k]
        if (value == null || value.expiry?.isBefore(Instant.now()) == true) {
            // For now removal will be always lazy, might reconsider later and have some periodic deletion
            data.remove(k)
            return RespNull
        }
        return value.value
    }

    fun set(k: RespValue, v: RespValue, p: DataStoreParams? = null) {
        val expiry = p?.expiryMs?.let { Instant.now().plus(Duration.ofMillis(it)) }
        data[k] = DataStoreValue(v, expiry)
    }

}


class DataStoreParams(var expiryMs: Long? = null) {
    fun parseParameter(p: RespValue, v: RespValue) {
        require(p is RespBulkString && v is RespBulkString) { "Parameter name and value must be encoded as RESP bulk string" }

        when (p.value?.uppercase()) {
            "PX" -> {
                expiryMs = v.value?.toLongOrNull() ?: throw IllegalArgumentException("Expected integer got: `${v}`")
            }

            else -> throw IllegalArgumentException("Unknown argument: `${p.value?.uppercase()}`")
        }
    }
}