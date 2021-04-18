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

public class FutureClient {

    private final BkesGrpc.BkesFutureStub stub;

    public FutureClient(String host, int port) {
        stub = BkesGrpc.newFutureStub(
                NettyChannelBuilder.forAddress(host, port).build()
        );
    }

    public ListenableFuture<StartReply> start(StartRequest request) {
        return stub.start(request);
    }

    public ListenableFuture<AddReply> add(AddRequest request) {
        return stub.add(request);
    }

    public ListenableFuture<RetrieveReply> retrieve(RetrieveRequest request) {
        return stub.retrieve(request);
    }
}
