package io.streamnative.pulsar.handlers.kop;

public interface KopServerStats {
    String CATEGORY_SERVER = "server";

    String SERVER_SCOPE = "kop_server";

    String SERVER_STATUS = "SERVER_STATUS";


    /**
     * PRODUCE STATS
     */
    String HANDLE_PRODUCE_REQUEST = "HANDLE_PRODUCE_REQUEST";
    String PRODUCE_ENCODE = "PRODUCE_ENCODE";
    String MESSAGE_PUBLISH = "MESSAGE_PUBLISH";
    String MESSAGE_QUEUED_LATENCY = "MESSAGE_QUEUED_LATENCY";

}
