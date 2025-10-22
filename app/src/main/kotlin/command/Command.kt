import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentHashMap

sealed interface RedisCommand {
    data object Ping : RedisCommand
    data object Multi : RedisCommand
    data object Exec : RedisCommand
    data object Discard : RedisCommand
    data class Echo(val message: RespValue) : RedisCommand
    data class Get(val key: RespValue) : RedisCommand
    data class Set(val key: RespValue, val value: RespValue, val params: DataStoreParams) : RedisCommand
    data class RPush(val key: RespValue, val values: List<RespValue>) : RedisCommand
    data class LPush(val key: RespValue, val values: List<RespValue>) : RedisCommand
    data class RPop(val key: RespValue, val count: Int = 1) : RedisCommand
    data class LPop(val key: RespValue, val count: Int = 1) : RedisCommand
    data class BLPop(val key: RespValue, val timeout: Double) : RedisCommand
    data class LLen(val key: RespValue) : RedisCommand
    data class LRange(val key: RespValue, val start: Int, val end: Int) : RedisCommand
    data class Type(val key: RespValue) : RedisCommand
    data class XAdd(val key: RespValue, val id: String?, val args: List<Pair<String, ByteArray>>) : RedisCommand
    data class XRange(val key: RespValue, val start: String, val end: String) : RedisCommand
    data class XRead(val keysToStarts: List<Pair<RespValue, String>>, val timeout: Double?) : RedisCommand
    data class Incr(val key: RespValue) : RedisCommand
}
