package fi.hsl.transitlog.hfp.utils

import java.util.concurrent.ThreadFactory

object DaemonThreadFactory : ThreadFactory {
    private var threadCounter = 1L

    override fun newThread(runnable: Runnable): Thread {
        val thread = Thread(runnable)
        thread.isDaemon = true
        thread.name = "DaemonThread-${threadCounter++}"
        return thread
    }
}