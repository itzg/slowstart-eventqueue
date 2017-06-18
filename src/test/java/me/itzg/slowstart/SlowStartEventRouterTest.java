package me.itzg.slowstart;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by geoff on 6/18/17.
 */
public class SlowStartEventRouterTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
    }

    @Test(timeout = 5000)
    public void testOneThenReady() throws Exception {
        final CompletableFuture<Void> futureReady = new CompletableFuture<>();
        final Receiver receiver = new Receiver();

        SlowStartEventRouter router = new SlowStartEventRouter(key -> futureReady, receiver,
                                                               temp.newFolder().toPath(), executor);

        router.route("alpha", TestUtils.createPayload(5));
        futureReady.complete(null);
        router.route("alpha", TestUtils.createPayload(6));

        receiver.waitFor(2);

        TestUtils.assertIntInBuf(5, receiver.get(0));
        TestUtils.assertIntInBuf(6, receiver.get(1));
    }

    @Test(timeout = 5000)
    public void testImmediateReady() throws Exception {
        final Receiver receiver = new Receiver();

        SlowStartEventRouter router = new SlowStartEventRouter(key -> CompletableFuture.completedFuture(null), receiver,
                                                               temp.newFolder().toPath(), executor);

        router.route("beta", TestUtils.createPayload(5));
        router.route("beta", TestUtils.createPayload(6));

        receiver.waitFor(2);

        TestUtils.assertIntInBuf(5, receiver.get(0));
        TestUtils.assertIntInBuf(6, receiver.get(1));
    }

    @Test
    public void testRouteSome() throws Exception {
        Map<String, CompletableFuture<Void>> futures = new HashMap<>();
        futures.put("alpha", new CompletableFuture<>());
        futures.put("beta", new CompletableFuture<>());

        final Receiver receiver = new Receiver();

        SlowStartEventRouter router = new SlowStartEventRouter(futures::get, receiver,
                                                               temp.newFolder().toPath(), executor);

        router.route("alpha", TestUtils.createPayload(5));
        router.route("beta", TestUtils.createPayload(15));
        router.route("beta", TestUtils.createPayload(16));
        futures.get("alpha").complete(null);
        router.route("alpha", TestUtils.createPayload(6));
        futures.get("beta").complete(null);

        receiver.waitFor(4);

        receiver.assertContains(5, 6);
        receiver.assertContains(15, 16);

    }
}