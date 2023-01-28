package ru.statsklad13.wb.crawler.api.data.misc;

import lombok.Value;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Value
public class PrioritizedTask<T> implements Runnable, Comparable<PrioritizedTask<T>> {

    public enum Level {
        HIGH, MEDIUM, LOW
    }

    Supplier<T> underlyingTask;
    CompletableFuture<T> callback;
    Level priorityLevel;

    @Override
    public int compareTo(PrioritizedTask o) {
        return this.priorityLevel.compareTo(o.priorityLevel);
    }

    @Override
    public void run() {
        try {
            if (!this.callback.isDone()) {
                this.callback.complete(this.underlyingTask.get());
            }
        } catch (Exception ex) {
            this.callback.completeExceptionally(ex);
        }
    }

}
