package tech.gklijs.bkes.client;

import com.google.common.util.concurrent.ListenableFuture;
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
     * Start an aggregate
     * @param request data to start
     * @return success or error
     */
    public ListenableFuture<StartReply> start(StartRequest request) {
        return stub.start(request);
    }

    /**
     * Add to aggregate
     * @param request data to add
     * @return success or error
     */
    public ListenableFuture<AddReply> add(AddRequest request) {
        return stub.add(request);
    }

    /**
     * Retrieve al records with a key
     * @param request data to retrieve
     * @return success or error
     */
    public ListenableFuture<RetrieveReply> retrieve(RetrieveRequest request) {
        return stub.retrieve(request);
    }
}
