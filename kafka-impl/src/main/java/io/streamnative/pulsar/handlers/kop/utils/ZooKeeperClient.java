package io.streamnative.pulsar.handlers.kop.utils;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeCreated;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;

import com.google.api.client.util.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
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
import java.util.function.BiConsumer;


@Slf4j
@Getter
public class ZooKeeperClient {
    private final String connectString;
    private final int sessionTimeoutMs;
    private final int connectionTimeoutMs;
    private int maxInFlightRequests;
    private volatile ZooKeeper zooKeeper;
    private final ReentrantReadWriteLock initializationLock = new ReentrantReadWriteLock();
    private static final ReentrantLock isConnectedOrExpiredLock = new ReentrantLock();
    private static final Condition isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition();
    private final ConcurrentHashMap<String, ZNodeChangeHandler> zNodeChangeHandlers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ZNodeChildChangeHandler> zNodeChildChangeHandlers =
            new ConcurrentHashMap<>();
    private final Semaphore inFlightRequests;
    private static final ConcurrentHashMap<String, StateChangeHandler> stateChangeHandlers =
            new ConcurrentHashMap<>();
    private static final ScheduledExecutorService expiryScheduler =
            new ScheduledThreadPoolExecutor(1);

    public ZooKeeperClient(String connectString,
                           int sessionTimeoutMs,
                           int connectionTimeoutMs,
                           int maxInFlightRequests) {
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.maxInFlightRequests = maxInFlightRequests;
        this.inFlightRequests = new Semaphore(maxInFlightRequests);
    }

    public void init() {
        log.info("Initializing a new session to {}.", connectString);
        try {
            this.zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, new ZooKeeperClientWatcher());
            waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (IOException | InterruptedException e) {
            log.error("Initializing a new session failed {}", e.getMessage());
            close();
        }
    }

    /**
     * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
     * <p>
     * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
     * with either a GetDataRequest or ExistsRequest.
     * <p>
     * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
     *
     * @param zNodeChangeHandler the handler to register
     */
    public void registerZNodeChangeHandler(ZNodeChangeHandler zNodeChangeHandler) {
        zNodeChangeHandlers.put(zNodeChangeHandler.path(), zNodeChangeHandler);
    }

    /**
     * Unregister the handler from ZooKeeperClient. This is just a local operation.
     *
     * @param path the path of the handler to unregister
     */
    public void unregisterZNodeChangeHandler(String path) {
        zNodeChangeHandlers.remove(path);
    }

    /**
     * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
     * <p>
     * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
     *
     * @param zNodeChildChangeHandler the handler to register
     */
    public void registerZNodeChildChangeHandler(ZNodeChildChangeHandler zNodeChildChangeHandler) {
        zNodeChildChangeHandlers.put(zNodeChildChangeHandler.path(), zNodeChildChangeHandler);
    }

    /**
     * Unregister the handler from ZooKeeperClient. This is just a local operation.
     *
     * @param path the path of the handler to unregister
     */
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
                return zNodeChildChangeHandlers.containsKey(request.getPath());
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
            expiryScheduler.shutdown();
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

            final BiConsumer<AsyncResponse, Throwable> responseCallback = (response, throwable) -> {
                responseQueue.add(response);
                inFlightRequests.release();
                countDownLatch.countDown();
            };

            for (AsyncRequest request : requests) {
                try {
                    inFlightRequests.acquire();
                    initializationLock.readLock().lock();
                    send(request, responseCallback);
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

    private void send(AsyncRequest request,
                      BiConsumer<AsyncResponse, Throwable> callback) {

        long sendTimeMs = System.currentTimeMillis();
        switch (request.getName()) {
            case "ExistsRequest":
                zooKeeper.exists(request.getPath(), shouldWatch(request),
                        (rc, path, ctx, stat) -> callback.accept(new ExistsResponse(
                                KeeperException.Code.get(rc),
                                path,
                                ctx,
                                stat,
                                new ResponseMetadata(sendTimeMs, System.currentTimeMillis())
                        ), null), request.getCtx());
                break;
            case "GetChildrenRequest":
                zooKeeper.getChildren(request.getPath(), shouldWatch(request),
                        (rc, path, ctx, children, stat) -> callback.accept(new GetChildrenResponse(
                                KeeperException.Code.get(rc),
                                path,
                                ctx,
                                children,
                                stat,
                                new ResponseMetadata(sendTimeMs, System.currentTimeMillis())
                        ), null), request.getCtx());
                break;
            case "CreateRequest":
                CreateRequest createRequest = (CreateRequest) request;
                zooKeeper.create(createRequest.getPath(),
                        createRequest.getData(),
                        createRequest.getAcls(),
                        createRequest.getCreateMode(),
                        (rc, path, ctx, name, stat) -> callback.accept(new CreateResponse(
                                KeeperException.Code.get(rc),
                                path,
                                ctx,
                                name,
                                new ResponseMetadata(sendTimeMs, System.currentTimeMillis())
                        ), null), createRequest.getCtx());
                break;
            case "DeleteRequest":
                DeleteRequest deleteRequest = (DeleteRequest) request;
                zooKeeper.delete(deleteRequest.getPath(), deleteRequest.getVersion(),
                        (rc, path, ctx) -> callback.accept(new DeleteResponse(
                                KeeperException.Code.get(rc),
                                path,
                                ctx,
                                new ResponseMetadata(sendTimeMs, System.currentTimeMillis())
                        ), null), deleteRequest.getCtx());
                break;
            case "GetDataRequest":
                zooKeeper.getData(request.getPath(), shouldWatch(request),
                        (rc, path, ctx, data, stat) -> callback.accept(new GetDataResponse(
                                KeeperException.Code.get(rc),
                                path,
                                ctx,
                                data,
                                stat,
                                new ResponseMetadata(sendTimeMs, System.currentTimeMillis())
                        ), null), request.getCtx());
            default:
                throw new IllegalStateException("Unexpected value: " + request);
        }

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
                        this.zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, new ZooKeeperClientWatcher());
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
    static class CreateRequest extends AsyncRequest {
        private final String path;
        private final byte[] data;
        private final List<ACL> acls;
        private final CreateMode createMode;
        private final Optional ctx;
        private final static String name = "CreateRequest";

        CreateRequest(String path,
                      byte[] data,
                      List<ACL> acls,
                      CreateMode createMode,
                      Object ctx) {
            super(path, Optional.of(createMode), name);
            this.path = path;
            this.data = data;
            this.acls = acls;
            this.createMode = createMode;
            this.ctx = Optional.of(ctx);
        }
    }

    @Getter
    static class DeleteRequest extends AsyncRequest {
        private final String path;
        private final int version;
        private final Optional ctx;
        private final static String name = "DeleteRequest";

        DeleteRequest(String path,
                      int version,
                      Object ctx) {
            super(path, Optional.of(ctx), name);
            this.path = path;
            this.version = version;
            this.ctx = Optional.of(ctx);
        }
    }

    @Getter
    static class GetDataRequest extends AsyncRequest {
        private final String path;
        private final Optional ctx;
        private final static String name = "GetDataRequest";

        GetDataRequest(String path, Object ctx) {
            super(path, Optional.of(ctx), name);
            this.path = path;
            this.ctx = Optional.of(ctx);
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

    @Getter
    static class CreateResponse extends AsyncResponse {
        private final KeeperException.Code resultCode;
        private final String path;
        private final Optional ctx;
        private final String name;
        private final ResponseMetadata metadata;

        CreateResponse(KeeperException.Code resultCode,
                       String path,
                       Object ctx,
                       String name,
                       ResponseMetadata metadata) {
            super(resultCode, path, Optional.of(ctx), null, metadata);
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = Optional.of(ctx);
            this.name = name;
            this.metadata = metadata;
        }
    }

    @Getter
    static class DeleteResponse extends AsyncResponse {
        private final KeeperException.Code resultCode;
        private final String path;
        private final Optional ctx;
        private final ResponseMetadata metadata;

        DeleteResponse(KeeperException.Code resultCode,
                       String path,
                       Object ctx,
                       ResponseMetadata metadata) {
            super(resultCode, path, Optional.of(ctx), null, metadata);
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = Optional.of(ctx);
            this.metadata = metadata;
        }
    }

    @Getter
    static class GetDataResponse extends AsyncResponse {
        private final KeeperException.Code resultCode;
        private final String path;
        private final Optional ctx;
        private final byte[] data;
        private final Stat stat;
        private final ResponseMetadata metadata;

        GetDataResponse(KeeperException.Code resultCode,
                        String path,
                        Object ctx,
                        byte[] data,
                        Stat stat,
                        ResponseMetadata metadata) {
            super(resultCode, path, Optional.of(ctx), stat, metadata);
            this.resultCode = resultCode;
            this.path = path;
            this.ctx = Optional.of(ctx);
            this.data = data;
            this.stat = stat;
            this.metadata = metadata;
        }
    }

    public interface StateChangeHandler {
        String name();

        void beforeInitializingSession();

        void afterInitializingSession();

        void onAuthFailure();
    }

    public interface ZNodeChangeHandler {
        String path();

        void handleCreation();

        void handleDeletion();

        void handleDataChange();
    }

    public interface ZNodeChildChangeHandler {
        String path();

        void handleChildChange();
    }

    static class ZooKeeperClientException extends RuntimeException {
        public ZooKeeperClientException(String message) {
            super(message);
        }
    }

    static class ZooKeeperClientExpiredException extends ZooKeeperClientException {
        public ZooKeeperClientExpiredException(String message) {
            super(message);
        }
    }

    static class ZooKeeperClientAuthFailedException extends ZooKeeperClientException {
        public ZooKeeperClientAuthFailedException(String message) {
            super(message);
        }
    }

    static class ZooKeeperClientTimeoutException extends ZooKeeperClientException {
        public ZooKeeperClientTimeoutException(String message) {
            super(message);
        }
    }
}
