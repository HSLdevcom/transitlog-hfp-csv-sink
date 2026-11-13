package fi.hsl.transitlog.hfp

import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import org.junit.jupiter.api.Timeout

class AwaitAllTest {
    private val executor = Executors.newFixedThreadPool(2)

    @AfterTest
    fun tearDown() {
        executor.shutdownNow()
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `returns immediately when nothing was submitted`() {
        awaitAll(ExecutorCompletionService<Void>(executor), 0)
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `waits for every submitted task`() {
        val completionService = ExecutorCompletionService<Void>(executor)
        val done = AtomicInteger()
        repeat(5) {
            completionService.submit(
                {
                    Thread.sleep(50)
                    done.incrementAndGet()
                },
                null,
            )
        }

        awaitAll(completionService, 5)

        assertEquals(5, done.get())
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    fun `a failing task does not stop waiting for the others`() {
        val completionService = ExecutorCompletionService<Void>(executor)
        val done = AtomicInteger()
        completionService.submit({ throw IllegalStateException("boom") }, null)
        repeat(3) { completionService.submit({ done.incrementAndGet() }, null) }

        awaitAll(completionService, 4)

        assertEquals(3, done.get())
    }
}
