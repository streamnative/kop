package io.streamnative.pulsar.handlers.kop.coordinator.group;

import io.streamnative.pulsar.handlers.kop.utils.ShutdownableThread;
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
        try {
            thread.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted at shutting down {}", coordinatorEventThreadName);
        }

    }


    public void put(GroupCoordinator.CoordinatorEvent event) {
        try {
            putLock.lock();
            queue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    static class CoordinatorEventThread extends ShutdownableThread {

        public CoordinatorEventThread(String name) {
            super(name);
        }

        @Override
        protected void doWork() {
            GroupCoordinator.CoordinatorEvent event = null;
            try {
                event = queue.take();
                event.process();
            } catch (InterruptedException e) {
                log.error("Error processing event {}, {}", event, e);
            }
        }

    }

}
