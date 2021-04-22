package tech.gklijs.bkes.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.netty.NettyChannelBuilder;
import tech.gklijs.bkes.api.AddReply;
import tech.gklijs.bkes.api.AddRequest;
import tech.gklijs.bkes.api.BkesGrpc;
import tech.gklijs.bkes.api.RetrieveReply;
import tech.gklijs.bkes.api.RetrieveRequest;
import tech.gklijs.bkes.api.StartReply;
import tech.gklijs.bkes.api.StartRequest;

/**
 * Client working with futures for async processing
 */
public class FutureClient {

    private final BkesGrpc.BkesFutureStub stub;

    /**
     * Creates a new future client
     * @param host host of bkes server
     * @param port port of bkes server
     */
    public FutureClient(String host, int port) {
        stub = BkesGrpc.newFutureStub(
                NettyChannelBuilder.forAddress(host, port).build()
        );
    }

    /**
     * Start a new aggregate
     *
     * @param key   key of the item, should not yet exist
     * @param value value of the item
     * @return error or succes
     */
    public ListenableFuture<StartReply> start(String key, byte[] value) {
        StartRequest request = StartRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFrom(value))
                .build();
        return stub.start(request);
    }

    /**
     * Add to an aggregate
     *
     * @param key   key of the item, should already exist
     * @param value value of the item
     * @param order 0-based order for the message. To prevent concurrent writes
     * @return error or succes
     */
    public ListenableFuture<AddReply> add(String key, byte[] value, int order) {
        AddRequest request = AddRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFrom(value))
                .setOrder(order)
                .build();
        return stub.add(request);
    }

    /**
     * Retrieve all the records for the key
     *
     * @param key key of the item, should not yet exist
     * @return error or success
     */
    public ListenableFuture<RetrieveReply> retrieve(String key) {
        RetrieveRequest request = RetrieveRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build();
        return stub.retrieve(request);
    }
}
