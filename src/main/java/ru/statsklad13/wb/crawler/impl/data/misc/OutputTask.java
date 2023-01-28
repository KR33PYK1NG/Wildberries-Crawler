package ru.statsklad13.wb.crawler.impl.data.misc;

import lombok.Value;
import lombok.val;
import ru.statsklad13.wb.crawler.impl.CrawlerImpl;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Value
public class OutputTask implements Runnable {

    public static final HashMap<Path, FileChannel> channelCache = new HashMap<>();

    Map<Path, ByteBuffer> transformed;
    CompletableFuture<Void> callback;

    @Override
    public void run() {
        try {
            for (val entry : this.transformed.entrySet()) {
                val path = entry.getKey();
                val buf = entry.getValue();
                var cached = channelCache.get(path);
                if (cached == null) {
                    cached = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    channelCache.put(path, cached);
                }
                cached.write(buf);
            }
            this.callback.complete(null);
        } catch (Exception ex) {
            CrawlerImpl.handleEx("Fatal exception in output thread", ex);
        }
    }

}
