package me.itzg.slowstart;

import java.util.concurrent.CompletableFuture;

public interface NewKeyObserver {

    /**
     * This observation happens when a {@link SlowStartEventRouter} first observes a key.
     *
     * @param key the key that has been newly observed by the {@link SlowStartEventRouter}
     * @return a future that will be completed by the application when the underlying {@link SlowStartEventQueue} can
     * be marked ready for events to be provided to its consumer. An observe may also return a pre-completed
     * future to indicate immediate readiness.
     */
    CompletableFuture<Void> observeNewKey(String key);
}
