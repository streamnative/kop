package io.streamnative.pulsar.handlers.kop.utils;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeCreated;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;

import com.google.api.client.util.Lists;
import kafka.zookeeper.StateChangeHandler;
import kafka.zookeeper.ZNodeChangeHandler;
import kafka.zookeeper.ZNodeChildChangeHandler;
import kafka.zookeeper.ZooKeeperClientAuthFailedException;
import kafka.zookeeper.ZooKeeperClientExpiredException;
import kafka.zookeeper.ZooKeeperClientTimeoutException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@Slf4j
@Getter
public class ZooKeeperClient {
    private String connectString;
    private int sessionTimeoutMs;
    private int connectionTimeoutMs;
    private int maxInFlightRequests;
    private volatile ZooKeeper zooKeeper;

    public ZooKeeperClient(String connectString,
                           int sessionTimeoutMs,
                           int connectionTimeoutMs,
                           int maxInFlightRequests) {
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.maxInFlightRequests = maxInFlightRequests;
    }

    private final ReentrantReadWriteLock initializationLock = new ReentrantReadWriteLock();
    private static final ReentrantLock isConnectedOrExpiredLock = new ReentrantLock();
    private static final Condition isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition();
    private final ConcurrentHashMap<String, ZNodeChangeHandler> zNodeChangeHandlers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ZNodeChildChangeHandler> zNodeChildChangeHandlers =
            new ConcurrentHashMap<>();
    private final Semaphore inFlightRequests = new Semaphore(maxInFlightRequests);
    private static final ConcurrentHashMap<String, StateChangeHandler> stateChangeHandlers =
            new ConcurrentHashMap<>();
    private static final ScheduledExecutorService expiryScheduler =
            new ScheduledThreadPoolExecutor(1);

    public void init() {
        log.info("Initializing a new session to {}.", connectString);
        try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, new ZooKeeperClientWatcher());
        } catch (IOException e) {
            log.error("Initializing a new session failed {}", e.getMessage());
        }
    }

    public void registerZNodeChangeHandler(ZNodeChangeHandler zNodeChangeHandler) {
        zNodeChangeHandlers.put(zNodeChangeHandler.path(), zNodeChangeHandler);
    }

    public void unregisterZNodeChangeHandler(String path) {
        zNodeChangeHandlers.remove(path);
    }

    public void registerZNodeChildChangeHandler(ZNodeChildChangeHandler zNodeChildChangeHandler) {
        zNodeChildChangeHandlers.put(zNodeChildChangeHandler.path(), zNodeChildChangeHandler);
    }

    public void unregisterZNodeChildChangeHandler(String path) {
        zNodeChildChangeHandlers.remove(path);
    }

    public void registerStateChangeHandler(StateChangeHandler stateChangeHandler) {
        try {
            initializationLock.readLock().lock();
            if (stateChangeHandler != null)
                stateChangeHandlers.put(stateChangeHandler.name(), stateChangeHandler);
        } finally {
            initializationLock.readLock().unlock();
        }
    }

    public void unregisterStateChangeHandler(String name) {
        try {
            initializationLock.readLock().lock();
            stateChangeHandlers.remove(name);
        } finally {
            initializationLock.readLock().unlock();
        }
    }

    private boolean shouldWatch(AsyncRequest request) {
        switch (request.getName()) {
            case "GetChildrenRequest":
                return zNodeChildChangeHandlers.contains(request.getPath());
            case "ExistsRequest":
            case "GetDataRequest":
                return zNodeChangeHandlers.contains(request.getPath());
            default:
                throw new IllegalStateException("Unexpected value: " + request.getName());
        }
    }

    public void waitUntilConnected() throws InterruptedException {
        try {
            isConnectedOrExpiredLock.lock();
            waitUntilConnected(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } finally {
            isConnectedOrExpiredLock.unlock();
        }
    }

    private void waitUntilConnected(long timeout, TimeUnit timeUnit) throws InterruptedException {
        log.info("Waiting until connected.");
        long nanos = timeUnit.toNanos(timeout);
        try {
            isConnectedOrExpiredLock.lock();
            ZooKeeper.States connectionState = zooKeeper.getState();
            while (!connectionState.isConnected() && connectionState.isAlive()) {
                if (nanos <= 0) {
                    throw new ZooKeeperClientTimeoutException(
                            "Timed out waiting for connection while in state: " + connectionState);
                }
                nanos = isConnectedOrExpiredCondition.awaitNanos(nanos);
                connectionState = zooKeeper.getState();
            }
            if (connectionState == ZooKeeper.States.AUTH_FAILED) {
                throw new ZooKeeperClientAuthFailedException(
                        "Auth failed either before or while waiting for connection");
            } else if (connectionState == ZooKeeper.States.CLOSED) {
                throw new ZooKeeperClientExpiredException(
                        "Session expired either before or while waiting for connection");
            }
            log.info("Connected.");
        } finally {
            isConnectedOrExpiredLock.unlock();
        }
    }

    public void close() {
        log.info("Closing.");
        try {
            initializationLock.writeLock().lock();
            zNodeChangeHandlers.clear();
            zNodeChildChangeHandlers.clear();
            stateChangeHandlers.clear();
            zooKeeper.close();
        } catch (InterruptedException e) {
            log.error("zookeeper close failed {}", e.getMessage());
        } finally {
            initializationLock.writeLock().unlock();
        }
        expiryScheduler.shutdown();
        log.info("Closed.");
    }

    protected List<AsyncResponse> handleRequests(Set<AsyncRequest> requests)
            throws InterruptedException {
        if (requests.isEmpty()) {
            return Lists.newArrayList();
        } else {
            CountDownLatch countDownLatch = new CountDownLatch(requests.size());
            ArrayBlockingQueue<AsyncResponse> responseQueue =
                    new ArrayBlockingQueue<>(requests.size());

            for (AsyncRequest request : requests) {
                try {
                    inFlightRequests.acquire();
                    initializationLock.readLock().lock();
                    send(request).whenComplete(
                            (response, throwable) -> {
                                responseQueue.add(response);
                                inFlightRequests.release();
                                countDownLatch.countDown();
                            });

                } catch (Exception e) {
                    inFlightRequests.release();
                    throw e;
                } finally {
                    initializationLock.readLock().unlock();
                }
            }
            countDownLatch.await();

            return Lists.newArrayList(responseQueue.iterator());
        }
    }

    private CompletableFuture<AsyncResponse> send(AsyncRequest request) {
        CompletableFuture<AsyncResponse> completableFuture = new CompletableFuture<>();

        long sendTimeMs = System.currentTimeMillis();
        switch (request.getName()) {
            case "ExistsRequest":
                zooKeeper.exists(request.getPath(), shouldWatch(request), new AsyncCallback.StatCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, Stat stat) {
                        completableFuture.complete(new ExistsResponse(
                                KeeperException.Code.get(rc),
                                path,
                                ctx,
                                stat,
                                new ResponseMetadata(sendTimeMs, System.currentTimeMillis())
                        ));

                    }
                }, request.getCtx().orElse(null));
                break;
            case "GetChildrenRequest":
                zooKeeper.getChildren(request.path, shouldWatch(request), new AsyncCallback.Children2Callback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                        completableFuture.complete(new GetChildrenResponse(
                                KeeperException.Code.get(rc),
                                path,
                                ctx,
                                children,
                                stat,
                                new ResponseMetadata(sendTimeMs, System.currentTimeMillis())
                        ));
                    }
                }, request.getCtx().orElse(null));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + request);
        }

        completableFuture.complete(null);
        return completableFuture;
    }

    private void scheduleSessionExpiryHandler() {
        expiryScheduler.schedule(() -> {
            log.info("Session expired.");
            reinitialize();
        }, 0, TimeUnit.MILLISECONDS);
    }

    private void callBeforeInitializingSession(StateChangeHandler handler) {
        try {
            handler.beforeInitializingSession();
        } catch (Throwable t) {
            log.error("Uncaught error in handler {}, throwable {}", handler.name(), t);
        }
    }

    private void callAfterInitializingSession(StateChangeHandler handler) {
        try {
            handler.afterInitializingSession();
        } catch (Throwable t) {
            log.error("Uncaught error in handler {}, throwable {}", handler.name(), t);
        }
    }

    private void reinitialize() {
        // Initialization callbacks are invoked outside of the lock to avoid deadlock potential since their completion
        // may require additional Zookeeper requests, which will block to acquire the initialization lock
        stateChangeHandlers.values().forEach(
                this::callBeforeInitializingSession);

        try {
            initializationLock.writeLock().lock();
            if (!zooKeeper.getState().isAlive()) {
                zooKeeper.close();
                log.info("Initializing a new session to {}.", connectString);
                // retry forever until ZooKeeper can be instantiated
                boolean connected = false;
                while (!connected) {
                    try {
                        zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, new ZooKeeperClientWatcher());
                        connected = true;
                    } catch (Exception e) {
                        log.info("Error when recreating ZooKeeper, retrying after a short sleep", e);
                        Thread.sleep(1000);
                    }
                }
                stateChangeHandlers.values().forEach(this::callAfterInitializingSession);
            }
        } catch (Exception e) {
            log.error("Error before recreating zookeeper when zookeeper close {}", e.getMessage());
        } finally {
            initializationLock.writeLock().unlock();
        }
    }


    // package level visibility for testing only
    private class ZooKeeperClientWatcher implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {
            log.debug("Received event: {}", watchedEvent);
            String path = watchedEvent.getPath();
            if (path == null) {
                Event.KeeperState state = watchedEvent.getState();
                try {
                    isConnectedOrExpiredLock.lock();
                    isConnectedOrExpiredCondition.signalAll();
                } finally {
                    isConnectedOrExpiredLock.unlock();
                }
                if (state == Event.KeeperState.AuthFailed) {
                    log.error("Auth failed.");
                    stateChangeHandlers.values().forEach(StateChangeHandler::onAuthFailure);
                } else if (state == Event.KeeperState.Expired) {
                    scheduleSessionExpiryHandler();
                }
            } else {
                Event.EventType eventType = watchedEvent.getType();
                if (eventType == NodeChildrenChanged) {
                    zNodeChildChangeHandlers.get(path).handleChildChange();
                } else if (eventType == NodeCreated) {
                    zNodeChangeHandlers.get(path).handleCreation();
                } else if (eventType == NodeDeleted) {
                    zNodeChangeHandlers.get(path).handleDeletion();
                } else if (eventType == NodeDataChanged) {
                    zNodeChangeHandlers.get(path).handleDataChange();
                }
            }
        }
    }

    @Getter
    abstract static class AsyncRequest {
        private final String name;
        private final String path;
        private final Optional ctx;

        public AsyncRequest(String path, Optional ctx, String name) {
            this.path = path;
            this.ctx = ctx;
            this.name = name;
        }

    }

    @Getter
    static class ExistsRequest extends AsyncRequest {
        private final String path;
        private final Optional ctx;
        private final static String name = "ExistsRequest";

        public ExistsRequest(String path, Optional ctx) {
            super(path, ctx, name);
            this.path = path;
            this.ctx = ctx;
        }
    }

    @Getter
    static class GetChildrenRequest extends AsyncRequest {
        private final String path;
        private final boolean registerWatch;
        private final Optional ctx;
        private final static String name = "GetChildrenRequest";

        public GetChildrenRequest(String path, boolean registerWatch, Optional ctx) {
            super(path, ctx, name);
            this.path = path;
            this.registerWatch = registerWatch;
            this.ctx = ctx;
        }
    }

    @Getter
    abstract static class AsyncResponse {
        private final KeeperException.Code resultCode;
        private final String path;
        private final Optional ctx;
        private final Stat stat;
        private final ResponseMetadata metadata;

        protected AsyncResponse(KeeperException.Code resultCode,
                                String path,
                                Optional ctx,
                                Stat stat,
                                ResponseMetadata metadata) {
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = ctx;
            this.stat = stat;
            this.metadata = metadata;
        }

        public Optional<KeeperException> resultException() {
            if (resultCode == KeeperException.Code.OK) {
                return Optional.empty();
            }
            return Optional.of(KeeperException.create(resultCode, path));
        }

        public void maybeThrow() throws KeeperException {
            if (resultCode != KeeperException.Code.OK) {
                throw KeeperException.create(resultCode, path);
            }
        }

    }

    @Getter
    static class ResponseMetadata {
        private final long sendTimeMs;
        private final long receivedTimeMs;

        public ResponseMetadata(long sendTimeMs, long receivedTimeMs) {
            this.sendTimeMs = sendTimeMs;
            this.receivedTimeMs = receivedTimeMs;
        }

        private long responseTimeMs() {
            return receivedTimeMs - sendTimeMs;
        }
    }


    @Getter
    static class ExistsResponse extends AsyncResponse {
        private final KeeperException.Code resultCode;
        private final String path;
        private final Optional ctx;
        private final Stat stat;
        private final ResponseMetadata metadata;

        public ExistsResponse(KeeperException.Code code,
                              String path,
                              Object ctx,
                              Stat stat,
                              ResponseMetadata metadata) {
            super(code, path, Optional.of(ctx), stat, metadata);
            this.resultCode = code;
            this.path = path;
            this.ctx = Optional.of(ctx);
            this.stat = stat;
            this.metadata = metadata;
        }
    }

    @Getter
    static class GetChildrenResponse extends AsyncResponse {
        private final KeeperException.Code resultCode;
        private final String path;
        private final Optional ctx;
        private final List<String> children;
        private final Stat stat;
        private final ResponseMetadata metadata;

        GetChildrenResponse(KeeperException.Code resultCode,
                            String path,
                            Object ctx,
                            List<String> children,
                            Stat stat,
                            ResponseMetadata metadata) {
            super(resultCode, path, Optional.of(ctx), stat, metadata);
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = Optional.of(ctx);
            this.children = children;
            this.stat = stat;
            this.metadata = metadata;
        }
    }
}
