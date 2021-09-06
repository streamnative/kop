/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;

/**
 * Shutdownable thread.
 */
@Slf4j
public abstract class ShutdownableThread extends Thread {

    private final boolean isInterruptible;
    private final String logIdent;
    private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    public ShutdownableThread(String name) {
        this(name, true);
    }

    public ShutdownableThread(String name,
                              boolean isInterruptible) {
        super(name);
        this.isInterruptible = isInterruptible;
        this.setDaemon(false);
        this.logIdent = "[" + name + "]";
    }

    public boolean isRunning() {
        return shutdownInitiated.getCount() != 0;
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    public boolean isShutdownComplete() {
        return shutdownComplete.getCount() == 0;
    }

    public synchronized boolean initiateShutdown() {
        if (isRunning() && log.isDebugEnabled()) {
            log.debug("{} Shutting down", logIdent);
        }
        shutdownInitiated.countDown();
        if (isInterruptible) {
            interrupt();
            return true;
        } else {
            return false;
        }
    }

    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete.
     */
    public void awaitShutdown() throws InterruptedException {
        shutdownComplete.await();
        if (log.isDebugEnabled()) {
            log.debug("{} Shutdown completed", logIdent);
        }
    }

    /**
     *  Causes the current thread to wait until the shutdown is initiated,
     *  or the specified waiting time elapses.
     *
     * @param timeout
     * @param unit
     */
    public void pause(long timeout, TimeUnit unit) throws InterruptedException {
        if (shutdownInitiated.await(timeout, unit)) {
            if (log.isTraceEnabled()) {
                log.trace("{} shutdownInitiated latch count reached zero. Shutdown called.", logIdent);
            }
        }
    }

    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception.
     */
    protected abstract void doWork();

    @Override
    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("{} Starting", logIdent);
        }
        try {
            while (isRunning()) {
                doWork();
            }
        } catch (FatalExitError e) {
            shutdownInitiated.countDown();
            shutdownComplete.countDown();
            if (log.isDebugEnabled()) {
                log.debug("{} Stopped", logIdent);
            }
            Exit.exit(e.statusCode());
        } catch (Throwable cause) {
            if (isRunning()) {
                log.error("{} Error due to", logIdent, cause);
            }
        } finally {
            shutdownComplete.countDown();
        }
    }

}
