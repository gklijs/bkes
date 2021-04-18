package tech.gklijs.bkes.client;

import io.grpc.netty.NettyChannelBuilder;
import tech.gklijs.bkes.api.AddReply;
import tech.gklijs.bkes.api.AddRequest;
import tech.gklijs.bkes.api.BkesGrpc;
import tech.gklijs.bkes.api.RetrieveReply;
import tech.gklijs.bkes.api.RetrieveRequest;
import tech.gklijs.bkes.api.StartReply;
import tech.gklijs.bkes.api.StartRequest;

public class BlockingClient {

    private final BkesGrpc.BkesBlockingStub stub;

    public BlockingClient(String host, int port) {
        stub = BkesGrpc.newBlockingStub(
                NettyChannelBuilder.forAddress(host, port).build()
        );
    }

    public StartReply start(StartRequest request){
        return stub.start(request);
    }

    public AddReply add(AddRequest request){
        return stub.add(request);
    }

    public RetrieveReply retrieve(RetrieveRequest request){
        return stub.retrieve(request);
    }
}
