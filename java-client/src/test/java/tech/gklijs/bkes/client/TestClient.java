package tech.gklijs.bkes.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import tech.gklijs.bkes.api.RetrieveReply;

class TestClient {

    @Test
    @Disabled("Requires a running bkes server")
    void test() {
        BlockingClient client = new BlockingClient("localhost", 50031);
        RetrieveReply result = client.retrieve("bla");
        System.out.println(result);
        Assertions.assertNotNull(result);
    }
}
