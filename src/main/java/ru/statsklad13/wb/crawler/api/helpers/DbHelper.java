package ru.statsklad13.wb.crawler.api.helpers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Cleanup;
import lombok.val;
import ru.statsklad13.wb.crawler.api.CrawlerApi;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class DbHelper {

    private static HikariDataSource hikari;
    private static ExecutorService executor;

    public static void init() {
        val cfg = new HikariConfig();
        cfg.setJdbcUrl(CrawlerApi.Settings.getDatabaseUrl());
        cfg.setUsername(CrawlerApi.Settings.getDatabaseUsername());
        cfg.setPassword(CrawlerApi.Settings.getDatabasePasswordOnce());
        hikari = new HikariDataSource(cfg);
        val th = CrawlerApi.Settings.getDatabaseThreads();
        executor = new ThreadPoolExecutor(th, th, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                CrawlerApi.createFactory("WB CrawlerApi Database Thread", true));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            hikari.close();
            executor.shutdownNow();
        }));
    }

    public static CompletableFuture<Void> executeUpdate(String sql, Object... args) {
        return CompletableFuture.runAsync(() -> {
            try {
                @Cleanup val connection = hikari.getConnection();
                @Cleanup val statement = connection.prepareStatement(sql);
                for (var i = 0; i < args.length; i++) {
                    statement.setObject(i + 1, args[i]);
                }
                statement.executeUpdate();
            } catch (Exception ex) {
                throw new CompletionException("Unable to execute DB update " + sql + " with args " + Arrays.toString(args), ex);
            }
        }, executor);
    }

    public static CompletableFuture<Void> executeQuery(Consumer<ResultSet> action, String sql, Object... args) {
        return executeQuery(CrawlerApi.Constants.DEFAULT_BATCH_SIZE, action, sql, args);
    }

    public static CompletableFuture<Void> executeQuery(int batchSize, Consumer<ResultSet> action, String sql, Object... args) {
        return CompletableFuture.runAsync(() -> {
            try {
                @Cleanup val connection = hikari.getConnection();
                if (batchSize > 0) {
                    connection.setAutoCommit(false);
                }
                @Cleanup val statement = connection.prepareStatement(sql);
                for (var i = 0; i < args.length; i++) {
                    statement.setObject(i + 1, args[i]);
                }
                statement.setFetchSize(batchSize);
                @Cleanup val result = statement.executeQuery();
                while (result.next()) {
                    action.accept(result);
                }
                if (batchSize > 0) {
                    connection.commit();
                }
            } catch (Exception ex) {
                throw new CompletionException("Unable to execute DB query " + sql + " with args " + Arrays.toString(args), ex);
            }
        }, executor);
    }

}
