class RedisDataStore {
    private val data = HashMap<RespValue, RespValue>()

    fun get(k: RespValue): RespValue = data[k] ?: RespBulkString(null)
    fun set(k: RespValue, v: RespValue) {
        data[k] = v
    }
}