package tech.gklijs.bkes.client;

import io.grpc.netty.NettyChannelBuilder;
import tech.gklijs.bkes.api.AddReply;
import tech.gklijs.bkes.api.AddRequest;
import tech.gklijs.bkes.api.BkesGrpc;
import tech.gklijs.bkes.api.RetrieveReply;
import tech.gklijs.bkes.api.RetrieveRequest;
import tech.gklijs.bkes.api.StartReply;
import tech.gklijs.bkes.api.StartRequest;

/**
 * Implementation for a simple blocking client
 */
public class BlockingClient {

    private final BkesGrpc.BkesBlockingStub stub;

    /**
     * Creates a net blocking client
     * @param host host of the bkes server
     * @param port port of the bkes server
     */
    public BlockingClient(String host, int port) {
        stub = BkesGrpc.newBlockingStub(
                NettyChannelBuilder.forAddress(host, port).build()
        );
    }

    /**
     * Start a new aggregate
     * @param request data to start
     * @return error or succes
     */
    public StartReply start(StartRequest request){
        return stub.start(request);
    }

    /**
     * Add to an aggregate
     * @param request data to add
     * @return error or succes
     */
    public AddReply add(AddRequest request){
        return stub.add(request);
    }

    /**
     * Retrieve all the records for the key
     * @param request data to retrieve
     * @return error or success
     */
    public RetrieveReply retrieve(RetrieveRequest request){
        return stub.retrieve(request);
    }
}
