package fi.hsl.transitlog.hfp.utils

import org.roaringbitmap.longlong.Roaring64Bitmap

/**
 * Helper for consuming values only once.
 *
 * @param keyFunction Function for calculating key for each value. The key should be equal if and only if the values are equal.
 */
class Deduplicator<T>(private val keyFunction: (T) -> Long) {
    private val keys = Roaring64Bitmap()

    fun consumeOnlyOnce(value: T, consumer: (T) -> Unit) {
        val key = keyFunction(value)
        if (!keys.contains(key)) {
            keys.add(key)
            consumer(value)
        }
    }
}