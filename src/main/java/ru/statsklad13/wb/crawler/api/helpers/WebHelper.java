package ru.statsklad13.wb.crawler.api.helpers;

import lombok.val;
import okhttp3.*;
import ru.statsklad13.wb.crawler.api.CrawlerApi;
import ru.statsklad13.wb.crawler.api.data.misc.PrioritizedTask;
import ru.statsklad13.wb.crawler.api.data.misc.WebResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class WebHelper {

    private static OkHttpClient client;
    private static ExecutorService executor;

    public static void init() {
        val disp = new Dispatcher();
        disp.setMaxRequests(Integer.MAX_VALUE);
        disp.setMaxRequestsPerHost(Integer.MAX_VALUE);
        client = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(100, 1, TimeUnit.MINUTES))
                .dispatcher(disp)
                .protocols(List.of(Protocol.HTTP_1_1))
                .retryOnConnectionFailure(false)
                .followRedirects(false)
                .followSslRedirects(false)
                .build();
        val th = CrawlerApi.Settings.getWebThreads();
        executor = new ThreadPoolExecutor(th, th, 0, TimeUnit.MILLISECONDS, new PriorityBlockingQueue<>(),
                CrawlerApi.createFactory("WB CrawlerApi Web Thread", true));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdownNow();
        }));
    }

    public static CompletableFuture<WebResponse> sendGetRequest(String url, int... allowedCodes) {
        return sendGetRequest(CrawlerApi.Constants.DEFAULT_PRIORITY, url, allowedCodes);
    }

    public static CompletableFuture<WebResponse> sendGetRequest(PrioritizedTask.Level priorityLevel, String url, int... allowedCodes) {
        val future = new CompletableFuture<WebResponse>();
        executor.execute(new PrioritizedTask<>(() -> {
            val request = new Request.Builder()
                    .url(url)
                    .build();
            var retryCount = -1;
            Response response = null;
            WebResponse webResponse = null;
            Exception lastEx = null;
            while (retryCount < CrawlerApi.Settings.getWebMaxRetries()) {
                try {
                    response = sendGetRequestAttempt(request, allowedCodes);
                    webResponse = new WebResponse(response.body().string(), response.code());
                    break;
                } catch (Exception ex) {
                    if (response != null) {
                        response.close();
                    }
                    lastEx = ex;
                    try {
                        Thread.sleep(CrawlerApi.Settings.getWebRetryDelayMs());
                    } catch (Exception ignored) {
                    }
                }
                retryCount++;
            }
            if (webResponse == null) {
                throw new CompletionException("Unable to send GET request to " + url, lastEx);
            }
            return webResponse;
        }, future, priorityLevel));
        return future;
    }

    private static Response sendGetRequestAttempt(Request request, int[] allowedCodes) throws IOException {
        val response = client.newCall(request).execute();
        val code = response.code();
        var valid = false;
        for (val allowedCode : allowedCodes) {
            if (allowedCode == code) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            response.close();
            throw new IOException("Invalid response code (" + code + ")");
        }
        return response;
    }

}
