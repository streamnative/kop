package io.streamnative.pulsar.handlers.kop.coordinator.group;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoordinatorEventManager {
    private static final String coordinatorEventThreadName = "coordinator-event-thread";
    private final ReentrantLock putLock = new ReentrantLock();
    private static final LinkedBlockingQueue<GroupCoordinator.CoordinatorEvent> queue =
            new LinkedBlockingQueue<>();
    private final CoordinatorEventThread thread =
            new CoordinatorEventThread(coordinatorEventThreadName);

    public void start() {
        thread.start();
    }

    public void close() {
        thread.shutdown();
    }


    public void put(GroupCoordinator.CoordinatorEvent event) {
        try {
            putLock.lock();
            queue.put(event);
        } catch (InterruptedException e) {
            log.error("Error put event {} to coordinator event queue {}", event, e);
        } finally {
            putLock.unlock();
        }
    }

    public void clearAndPut(GroupCoordinator.CoordinatorEvent event) {
        try {
            putLock.lock();
            queue.clear();
            put(event);
        } finally {
            putLock.unlock();
        }
    }

    static class CoordinatorEventThread extends Thread {
        private final String threadName;
        private final CountDownLatch shutdownInitiated = new CountDownLatch(1);
        private final CountDownLatch shutdownComplete = new CountDownLatch(1);

        public CoordinatorEventThread(String name) {
            this.threadName = name;
        }

        private void doWork() {
            GroupCoordinator.CoordinatorEvent event = null;
            try {
                event = queue.take();
                event.process();
            } catch (InterruptedException e) {
                log.error("Error processing event {}, {}", event, e);
            }

        }

        @Override
        public void run() {
            log.info("Starting");
            try {
                while (isRunning()) {
                    doWork();
                }
            } catch (Exception e) {
                shutdownInitiated.countDown();
                shutdownComplete.countDown();
                log.info("Stopped");
                super.stop();
                return;
            } finally {
                shutdownComplete.countDown();
            }
            log.info("Stopped");
        }

        @Override
        public synchronized void start() {
            setName();
            super.start();
        }

        private void setName() {
            super.setName(threadName);
        }

        public void shutdown() {
            try {
                initiateShutdown();
                awaitShutdown();
            } catch (InterruptedException e) {
                log.error("shut down {} exception {}", threadName, e);
            }

        }

        public boolean isShutdownComplete() {
            return shutdownComplete.getCount() == 0;
        }

        public boolean initiateShutdown() {
            synchronized (this) {
                if (isRunning()) {
                    log.info("Shutting down");
                    shutdownInitiated.countDown();
                    return true;
                }
                return false;
            }
        }

        public void awaitShutdown() throws InterruptedException {
            shutdownComplete.await();
            log.info("Shutdown completed");
        }

        private boolean isRunning() {
            return shutdownInitiated.getCount() != 0;
        }
    }

}
