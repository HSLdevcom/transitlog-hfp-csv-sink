package fi.hsl.transitlog.hfp.utils

/**
 * Helper for consuming values only once.
 *
 * @param keyFunction Function for calculating key for each value. The key should be equal if and only if the values are equal.
 */
class Deduplicator<T, K>(initialSetSize: Int = 10000, private val keyFunction: (T) -> K) {
    private val keys = HashSet<K>(initialSetSize)

    fun consumeOnlyOnce(value: T, consumer: (T) -> Unit) {
        val key = keyFunction(value)
        if (!keys.contains(key)) {
            keys.add(key)
            consumer(value)
        }
    }
}