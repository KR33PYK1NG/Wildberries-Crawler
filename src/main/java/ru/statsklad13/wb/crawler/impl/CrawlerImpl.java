package ru.statsklad13.wb.crawler.impl;

import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import ru.statsklad13.wb.crawler.api.CrawlerApi;
import ru.statsklad13.wb.crawler.api.data.key.merch.MerchKey;
import ru.statsklad13.wb.crawler.api.data.key.merch.SizeKey;
import ru.statsklad13.wb.crawler.api.data.key.merch.WarehouseKey;
import ru.statsklad13.wb.crawler.api.data.key.product.ProductKey;
import ru.statsklad13.wb.crawler.api.data.key.source.CatalogKey;
import ru.statsklad13.wb.crawler.api.data.merch.Warehouse;
import ru.statsklad13.wb.crawler.api.data.product.Seller;
import ru.statsklad13.wb.crawler.api.data.source.Catalog;
import ru.statsklad13.wb.crawler.api.data.source.Category;
import ru.statsklad13.wb.crawler.api.data.source.Source;
import ru.statsklad13.wb.crawler.api.helpers.CrawlHelper;
import ru.statsklad13.wb.crawler.api.helpers.DbHelper;
import ru.statsklad13.wb.crawler.api.helpers.WebHelper;
import ru.statsklad13.wb.crawler.impl.data.merch.Order;
import ru.statsklad13.wb.crawler.impl.data.merch.Refill;
import ru.statsklad13.wb.crawler.impl.data.misc.OutputTask;
import ru.statsklad13.wb.crawler.impl.data.result.CollectedCatalogPage;
import ru.statsklad13.wb.crawler.impl.data.result.CollectedCategory;
import ru.statsklad13.wb.crawler.impl.helpers.CacheHelper;
import ru.statsklad13.wb.crawler.impl.helpers.DateHelper;
import ru.statsklad13.wb.crawler.impl.helpers.OutputHelper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public class CrawlerImpl {

    public enum TableType {
        DICTIONARY, HISTORY
    }

    public enum Table {

        CATALOGS(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "catalogs.txt"),
                "catalogs",
                "timestamp TIMESTAMPTZ, " +
                        "shard TEXT, " +
                        "query TEXT",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "shard TEXT NOT NULL, " +
                        "query TEXT NOT NULL, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (shard, query)",
                "INSERT INTO %table% (last_timestamp, shard, query) " +
                        "SELECT DISTINCT ON (shard, query) * FROM %table%_tmp " +
                        "ON CONFLICT (shard, query) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp;",
                "last_timestamp"
        ),
        CATEGORIES(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "categories.txt"),
                "categories",
                "timestamp TIMESTAMPTZ, " +
                        "text TEXT, " +
                        "catalog_shard TEXT, " +
                        "catalog_query TEXT",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "text TEXT NOT NULL, " +
                        "catalog_id BIGINT NOT NULL, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (text)",
                "INSERT INTO %table% (last_timestamp, text, catalog_id) " +
                        "SELECT DISTINCT ON (text) timestamp, text, catalogs.id AS catalog_id FROM %table%_tmp " +
                        "LEFT JOIN catalogs ON catalogs.shard = catalog_shard AND catalogs.query = catalog_query " +
                        "ON CONFLICT (text) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp, " +
                        "catalog_id = EXCLUDED.catalog_id;",
                "last_timestamp",
                "catalog_id"
        ),
        QUERIES(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "queries.txt"),
                "queries",
                "timestamp TIMESTAMPTZ, " +
                        "text TEXT, " +
                        "catalog_shard TEXT, " +
                        "catalog_query TEXT",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "text TEXT NOT NULL, " +
                        "catalog_id BIGINT NOT NULL, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (text)",
                "INSERT INTO %table% (last_timestamp, text, catalog_id) " +
                        "SELECT DISTINCT ON (text) timestamp, text, catalogs.id AS catalog_id FROM %table%_tmp " +
                        "LEFT JOIN catalogs ON catalogs.shard = catalog_shard AND catalogs.query = catalog_query " +
                        "ON CONFLICT (text) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp, " +
                        "catalog_id = EXCLUDED.catalog_id;",
                "last_timestamp",
                "catalog_id"
        ),
        BRANDS(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "brands.txt"),
                "brands",
                "timestamp TIMESTAMPTZ, " +
                        "wb_id INTEGER, " +
                        "name TEXT, " +
                        "image_url TEXT",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "wb_id INTEGER NOT NULL, " +
                        "name TEXT NOT NULL, " +
                        "image_url TEXT NOT NULL, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (wb_id)",
                "INSERT INTO %table% (last_timestamp, wb_id, name, image_url) " +
                        "SELECT DISTINCT ON (wb_id) * FROM %table%_tmp " +
                        "ON CONFLICT (wb_id) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp, " +
                        "name = EXCLUDED.name, " +
                        "image_url = EXCLUDED.image_url;",
                "last_timestamp",
                "name"
        ),
        SELLERS(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "sellers.txt"),
                "sellers",
                "timestamp TIMESTAMPTZ, " +
                        "wb_id INTEGER, " +
                        "name TEXT, " +
                        "image_url TEXT, " +
                        "inn TEXT, " +
                        "ogrn TEXT, " +
                        "ogrnip TEXT, " +
                        "address TEXT",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "wb_id INTEGER NOT NULL, " +
                        "name TEXT NOT NULL, " +
                        "image_url TEXT NOT NULL, " +
                        "inn TEXT, " +
                        "ogrn TEXT, " +
                        "ogrnip TEXT, " +
                        "address TEXT, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (wb_id)",
                "INSERT INTO %table% (last_timestamp, wb_id, name, image_url, inn, ogrn, ogrnip, address) " +
                        "SELECT DISTINCT ON (wb_id) * FROM %table%_tmp " +
                        "ON CONFLICT (wb_id) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp, " +
                        "name = EXCLUDED.name, " +
                        "image_url = EXCLUDED.image_url, " +
                        "inn = EXCLUDED.inn, " +
                        "ogrn = EXCLUDED.ogrn, " +
                        "ogrnip = EXCLUDED.ogrnip, " +
                        "address = EXCLUDED.address;",
                "last_timestamp",
                "name"
        ),
        PRODUCTS(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "products.txt"),
                "products",
                "timestamp TIMESTAMPTZ, " +
                        "sku INTEGER, " +
                        "name TEXT, " +
                        "image_url TEXT, " +
                        "brand_wb_id INTEGER, " +
                        "seller_wb_id INTEGER",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "sku INTEGER NOT NULL, " +
                        "name TEXT NOT NULL, " +
                        "image_url TEXT NOT NULL, " +
                        "brand_id BIGINT NOT NULL, " +
                        "seller_id BIGINT, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (sku)",
                "INSERT INTO %table% (last_timestamp, sku, name, image_url, brand_id, seller_id) " +
                        "SELECT DISTINCT ON (sku) timestamp, sku, %table%_tmp.name, %table%_tmp.image_url, brands.id AS brand_id, sellers.id AS seller_id FROM %table%_tmp " +
                        "LEFT JOIN brands ON brands.wb_id = brand_wb_id " +
                        "LEFT JOIN sellers ON sellers.wb_id = seller_wb_id " +
                        "ON CONFLICT (sku) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp, " +
                        "name = EXCLUDED.name, " +
                        "image_url = EXCLUDED.image_url, " +
                        "brand_id = EXCLUDED.brand_id, " +
                        "seller_id = EXCLUDED.seller_id;",
                "last_timestamp",
                "brand_id",
                "seller_id"
        ),
        PRODUCT_DETAILS(
                TableType.HISTORY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "product_details.txt"),
                "product_details",
                "timestamp TIMESTAMPTZ, " +
                        "product_sku INTEGER, " +
                        "price INTEGER, " +
                        "sale_price INTEGER, " +
                        "feedbacks INTEGER, " +
                        "rating SMALLINT",
                "id BIGSERIAL, " +
                        "timestamp TIMESTAMPTZ NOT NULL, " +
                        "product_id BIGINT NOT NULL, " +
                        "price INTEGER NOT NULL, " +
                        "sale_price INTEGER NOT NULL, " +
                        "feedbacks INTEGER NOT NULL, " +
                        "rating SMALLINT NOT NULL, " +
                        "PRIMARY KEY (id, timestamp), " +
                        "UNIQUE (product_id, timestamp)",
                "INSERT INTO %table% (timestamp, product_id, price, sale_price, feedbacks, rating) " +
                        "SELECT DISTINCT ON (product_sku, timestamp) timestamp, products.id AS product_id, price, sale_price, feedbacks, rating FROM %table%_tmp " +
                        "LEFT JOIN products ON products.sku = product_sku " +
                        "ON CONFLICT (product_id, timestamp) DO UPDATE SET " +
                        "price = EXCLUDED.price, " +
                        "sale_price = EXCLUDED.sale_price, " +
                        "feedbacks = EXCLUDED.feedbacks, " +
                        "rating = EXCLUDED.rating;",
                "timestamp"
        ),
        POSITIONS(
                TableType.HISTORY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "positions.txt"),
                "positions",
                "timestamp TIMESTAMPTZ, " +
                        "product_sku INTEGER, " +
                        "catalog_shard TEXT, " +
                        "catalog_query TEXT, " +
                        "place SMALLINT",
                "id BIGSERIAL, " +
                        "timestamp TIMESTAMPTZ NOT NULL, " +
                        "product_id BIGINT NOT NULL, " +
                        "catalog_id BIGINT NOT NULL, " +
                        "place SMALLINT NOT NULL, " +
                        "PRIMARY KEY (id, timestamp), " +
                        "UNIQUE (product_id, catalog_id, timestamp)",
                "INSERT INTO %table% (timestamp, product_id, catalog_id, place) " +
                        "SELECT DISTINCT ON (product_sku, catalog_shard, catalog_query, timestamp) timestamp, products.id AS product_id, catalogs.id AS catalog_id, place FROM %table%_tmp " +
                        "LEFT JOIN products ON products.sku = product_sku " +
                        "LEFT JOIN catalogs ON catalogs.shard = catalog_shard AND catalogs.query = catalog_query " +
                        "ON CONFLICT (product_id, catalog_id, timestamp) DO UPDATE SET " +
                        "place = EXCLUDED.place;",
                "timestamp",
                "catalog_id"
        ),
        SIZES(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "sizes.txt"),
                "sizes",
                "timestamp TIMESTAMPTZ, " +
                        "product_sku INTEGER, " +
                        "name TEXT, " +
                        "alt_name TEXT",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "product_id INTEGER NOT NULL, " +
                        "name TEXT NOT NULL, " +
                        "alt_name TEXT NOT NULL, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (product_id, name)",
                "INSERT INTO %table% (last_timestamp, product_id, name, alt_name) " +
                        "SELECT DISTINCT ON (product_sku, name) timestamp, products.id AS product_id, %table%_tmp.name, alt_name FROM %table%_tmp " +
                        "LEFT JOIN products ON products.sku = product_sku " +
                        "ON CONFLICT (product_id, name) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp, " +
                        "alt_name = EXCLUDED.alt_name;",
                "last_timestamp"
        ),
        WAREHOUSES(
                TableType.DICTIONARY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "warehouses.txt"),
                "warehouses",
                "timestamp TIMESTAMPTZ, " +
                        "wb_id INTEGER, " +
                        "name TEXT",
                "id BIGSERIAL, " +
                        "last_timestamp TIMESTAMPTZ NOT NULL, " +
                        "wb_id INTEGER NOT NULL, " +
                        "name TEXT, " +
                        "PRIMARY KEY (id), " +
                        "UNIQUE (wb_id)",
                "INSERT INTO %table% (last_timestamp, wb_id, name) " +
                        "SELECT DISTINCT ON (wb_id) * FROM %table%_tmp " +
                        "ORDER BY wb_id, name " +
                        "ON CONFLICT (wb_id) DO UPDATE SET " +
                        "last_timestamp = EXCLUDED.last_timestamp, " +
                        "name = EXCLUDED.name;",
                "last_timestamp"
        ),
        STOCKS(
                TableType.HISTORY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "stocks.txt"),
                "stocks",
                "timestamp TIMESTAMPTZ, " +
                        "product_sku INTEGER, " +
                        "size_name TEXT, " +
                        "warehouse_wb_id INTEGER, " +
                        "quantity INTEGER",
                "id BIGSERIAL, " +
                        "timestamp TIMESTAMPTZ NOT NULL, " +
                        "size_id BIGINT NOT NULL, " +
                        "warehouse_id BIGINT NOT NULL, " +
                        "quantity INTEGER NOT NULL, " +
                        "PRIMARY KEY (id, timestamp), " +
                        "UNIQUE (size_id, warehouse_id, timestamp)",
                "INSERT INTO %table% (timestamp, size_id, warehouse_id, quantity) " +
                        "SELECT DISTINCT ON (product_sku, size_name, warehouse_wb_id, timestamp) timestamp, sizes.id AS size_id, warehouses.id AS warehouse_id, quantity FROM %table%_tmp " +
                        "LEFT JOIN products ON products.sku = product_sku " +
                        "LEFT JOIN sizes ON sizes.product_id = products.id AND sizes.name = size_name " +
                        "LEFT JOIN warehouses ON warehouses.wb_id = warehouse_wb_id " +
                        "ON CONFLICT (size_id, warehouse_id, timestamp) DO UPDATE SET " +
                        "quantity = EXCLUDED.quantity;",
                "timestamp",
                "warehouse_id"
        ),
        ORDERS(
                TableType.HISTORY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "orders.txt"),
                "orders",
                "timestamp TIMESTAMPTZ, " +
                        "timestamp_to TIMESTAMPTZ, " +
                        "product_sku INTEGER, " +
                        "size_name TEXT, " +
                        "warehouse_wb_id INTEGER, " +
                        "quantity INTEGER",
                "id BIGSERIAL, " +
                        "timestamp TIMESTAMPTZ NOT NULL, " +
                        "timestamp_to TIMESTAMPTZ NOT NULL, " +
                        "size_id BIGINT NOT NULL, " +
                        "warehouse_id BIGINT NOT NULL, " +
                        "quantity INTEGER NOT NULL, " +
                        "PRIMARY KEY (id, timestamp), " +
                        "UNIQUE (size_id, warehouse_id, timestamp)",
                "INSERT INTO %table% (timestamp, timestamp_to, size_id, warehouse_id, quantity) " +
                        "SELECT DISTINCT ON (product_sku, size_name, warehouse_wb_id, timestamp) timestamp, timestamp_to, sizes.id AS size_id, warehouses.id AS warehouse_id, quantity FROM %table%_tmp " +
                        "LEFT JOIN products ON products.sku = product_sku " +
                        "LEFT JOIN sizes ON sizes.product_id = products.id AND sizes.name = size_name " +
                        "LEFT JOIN warehouses ON warehouses.wb_id = warehouse_wb_id " +
                        "ON CONFLICT (size_id, warehouse_id, timestamp) DO UPDATE SET " +
                        "timestamp_to = EXCLUDED.timestamp_to, " +
                        "quantity = EXCLUDED.quantity;",
                "timestamp",
                "warehouse_id"
        ),
        REFILLS(
                TableType.HISTORY,
                Paths.get(Constants.OUTPUT_DIR_NAME, "refills.txt"),
                "refills",
                "timestamp TIMESTAMPTZ, " +
                        "timestamp_to TIMESTAMPTZ, " +
                        "product_sku INTEGER, " +
                        "size_name TEXT, " +
                        "warehouse_wb_id INTEGER, " +
                        "quantity INTEGER",
                "id BIGSERIAL, " +
                        "timestamp TIMESTAMPTZ NOT NULL, " +
                        "timestamp_to TIMESTAMPTZ NOT NULL, " +
                        "size_id BIGINT NOT NULL, " +
                        "warehouse_id BIGINT NOT NULL, " +
                        "quantity INTEGER NOT NULL, " +
                        "PRIMARY KEY (id, timestamp), " +
                        "UNIQUE (size_id, warehouse_id, timestamp)",
                "INSERT INTO %table% (timestamp, timestamp_to, size_id, warehouse_id, quantity) " +
                        "SELECT DISTINCT ON (product_sku, size_name, warehouse_wb_id, timestamp) timestamp, timestamp_to, sizes.id AS size_id, warehouses.id AS warehouse_id, quantity FROM %table%_tmp " +
                        "LEFT JOIN products ON products.sku = product_sku " +
                        "LEFT JOIN sizes ON sizes.product_id = products.id AND sizes.name = size_name " +
                        "LEFT JOIN warehouses ON warehouses.wb_id = warehouse_wb_id " +
                        "ON CONFLICT (size_id, warehouse_id, timestamp) DO UPDATE SET " +
                        "timestamp_to = EXCLUDED.timestamp_to, " +
                        "quantity = EXCLUDED.quantity;",
                "timestamp",
                "warehouse_id"
        );

        @Getter private final Path outputPath;
        private final TableType type;
        private final String tableName;
        private final String tmpSchema;
        private final String finalSchema;
        private final String importSql;
        private final String[] indexColumns;

        Table(TableType type, Path outputPath, String tableName, String tmpSchema, String finalSchema, String importSql, String... indexColumns) {
            this.type = type;
            this.outputPath = outputPath;
            this.tableName = tableName;
            this.tmpSchema = tmpSchema;
            this.finalSchema = finalSchema;
            this.importSql = importSql;
            this.indexColumns = indexColumns;
        }
    }

    public static class Constants {

        public static final String NULL_FIELD = "\\N";
        public static final String FIELD_SEPARATOR = "\t";
        public static final String OUTPUT_DIR_NAME = "output";
        public static final int QUERY_BATCH_SIZE = 1000000;
        public static final int DAY_HISTORY_LENGTH = 30;

    }

    private static ExecutorService outputExecutor;

    public static void main(String[] args) {
        try {
            CrawlerApi.Settings.load("crawler.properties");
            WebHelper.init();
            DbHelper.init();
            CacheHelper.init();
            outputExecutor = Executors.newSingleThreadExecutor(CrawlerApi.createFactory("WB CrawlerImpl Output Thread", false));
            val taskExecutor = Executors.newSingleThreadScheduledExecutor(CrawlerApi.createFactory("WB CrawlerImpl Task Thread", false));
            val mainTask = new Runnable() {
                @Override
                public void run() {
                    try {
                        val outputDirPath = Paths.get(Constants.OUTPUT_DIR_NAME);
                        Files.createDirectories(outputDirPath);
                        updateDayVariables();
                        if (!CacheHelper.hasTemporary("unfinished_task")) {
                            log.info("No unfinished task left, cleaning up");
                            cleanupOutputDirectory();
                            CacheHelper.writeTemporary("unfinished_task");
                            CacheHelper.writeTemporary("task_timestamp", new Timestamp(DateHelper.pointStartCalendar().getTimeInMillis()));
                        }
                        val dayTimestamp = CacheHelper.<Timestamp>getPermanent("last_timestamp").get();
                        val taskTimestamp = CacheHelper.<Timestamp>getTemporary("task_timestamp").get();
                        if (!CacheHelper.hasTemporary("fc_finish")) {
                            log.info("Full crawl not done, running it now");
                            if (!CacheHelper.hasTemporary("fc_warehouses_done")) {
                                val warehouses = CrawlHelper.crawlWarehouses().join().getWarehouses();
                                processWarehouses(warehouses, taskTimestamp);
                                CacheHelper.writeTemporary("fc_warehouses_done");
                            } else {
                                log.info("All warehouses already processed");
                            }
                            if (!CacheHelper.hasTemporary("fc_categories_done")) {
                                val categories = CrawlHelper.crawlCategories().join().getCategories();
                                processCategories(categories, taskTimestamp);
                                CacheHelper.writeTemporary("fc_categories_done");
                            } else {
                                log.info("All categories already processed");
                            }
                            if (!CacheHelper.hasTemporary("fc_catalogs_done")) {
                                val catalogs = loadStoredCatalogs();
                                processCatalogs(catalogs, taskTimestamp);
                                CacheHelper.writeTemporary("fc_catalogs_done");
                            } else {
                                log.info("All catalogs already processed");
                            }
                            if (!CacheHelper.hasTemporary("fc_tables_done")) {
                                val futures = new ArrayList<CompletableFuture<Void>>();
                                for (val table : Table.values()) {
                                    var future = table.type == TableType.DICTIONARY ?
                                            DbHelper.executeUpdate("CREATE TABLE IF NOT EXISTS " + table.tableName + " (" + table.finalSchema + ");") :
                                            DbHelper.executeUpdate("CREATE TABLE IF NOT EXISTS " + table.tableName + " (" + table.finalSchema + ") PARTITION BY RANGE (timestamp);");
                                    for (val column : table.indexColumns) {
                                        future = future.thenComposeAsync(ignored -> {
                                            return DbHelper.executeUpdate("CREATE INDEX IF NOT EXISTS " + table.tableName + "_" + column + "_idx ON " + table.tableName + " (" + column + ");");
                                        });
                                    }
                                    futures.add(future);
                                }
                                DbHelper.executeUpdate("CREATE TABLE IF NOT EXISTS partitions (id SMALLSERIAL, timestamp TIMESTAMPTZ NOT NULL, table_name TEXT NOT NULL, PRIMARY KEY (id), UNIQUE (table_name));").join();
                                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                                CacheHelper.writeTemporary("fc_tables_done");
                            } else {
                                log.info("All tables and indices already created");
                            }
                            if (!CacheHelper.hasTemporary("fc_imports_done")) {
                                val catalogsImport = importIntoDatabase(Table.CATALOGS, taskTimestamp, null);
                                val categoriesImport = catalogsImport.thenComposeAsync(ignored -> {
                                    return importIntoDatabase(Table.CATEGORIES, taskTimestamp, null);
                                });
                                val queriesImport = catalogsImport.thenComposeAsync(ignored -> {
                                    return importIntoDatabase(Table.QUERIES, taskTimestamp, null);
                                });
                                val brandsImport = importIntoDatabase(Table.BRANDS, taskTimestamp, null);
                                val sellersImport = importIntoDatabase(Table.SELLERS, taskTimestamp, null);
                                val productsImport = brandsImport.thenComposeAsync(ignored -> {
                                    return sellersImport.thenComposeAsync(ignored2 -> {
                                        return importIntoDatabase(Table.PRODUCTS, taskTimestamp, null);
                                    });
                                });
                                val productDetailsImport = productsImport.thenComposeAsync(ignored -> {
                                    return importIntoDatabase(Table.PRODUCT_DETAILS, taskTimestamp, null);
                                });
                                val positionsImport = catalogsImport.thenComposeAsync(ignored -> {
                                    return productsImport.thenComposeAsync(ignored2 -> {
                                        return importIntoDatabase(Table.POSITIONS, taskTimestamp, null);
                                    });
                                });
                                val sizesImport = productsImport.thenComposeAsync(ignored -> {
                                    return importIntoDatabase(Table.SIZES, taskTimestamp, null);
                                });
                                val warehousesImport = importIntoDatabase(Table.WAREHOUSES, taskTimestamp, null);
                                val stocksImport = productsImport.thenComposeAsync(ignored -> {
                                    return sizesImport.thenComposeAsync(ignored2 -> {
                                        return warehousesImport.thenComposeAsync(ignored3 -> {
                                            return importIntoDatabase(Table.STOCKS, taskTimestamp, null);
                                        });
                                    });
                                });
                                catalogsImport.join();
                                categoriesImport.join();
                                queriesImport.join();
                                brandsImport.join();
                                sellersImport.join();
                                productsImport.join();
                                productDetailsImport.join();
                                positionsImport.join();
                                sizesImport.join();
                                warehousesImport.join();
                                stocksImport.join();
                                CacheHelper.writeTemporary("fc_imports_done");
                            } else {
                                log.info("Everything already imported into database");
                            }
                            log.info("Searching stocks older than {}", dayTimestamp);
                            val lastTimestamp = new AtomicReference<Timestamp>();
                            DbHelper.executeQuery(result -> {
                                try {
                                    lastTimestamp.set(result.getTimestamp("timestamp"));
                                } catch (Exception ex) {
                                    handleEx("Fatal exception while processing last timestamp query result", ex);
                                }
                            }, "SELECT timestamp FROM stocks WHERE timestamp < ? ORDER BY timestamp DESC LIMIT 1;", dayTimestamp).join();
                            if (lastTimestamp.get() != null) {
                                if (!CacheHelper.hasTemporary("fc_ordersrefills_done")) {
                                    log.info("Last timestamp found: {}", lastTimestamp.get());
                                    prepareProcessOrdersRefills(taskTimestamp, dayTimestamp, lastTimestamp.get());
                                    CacheHelper.writeTemporary("fc_ordersrefills_done");
                                } else {
                                    log.info("Orders and refills already calculated");
                                }
                                if (!CacheHelper.hasTemporary("fc_minify_done")) {
                                    val lastDayStartTimestamp = new Timestamp(DateHelper.dayStartCalendar(lastTimestamp.get().getTime(), 0).getTimeInMillis());
                                    val secondLastTimestamp = new AtomicReference<Timestamp>();
                                    DbHelper.executeQuery(result -> {
                                        try {
                                            secondLastTimestamp.set(result.getTimestamp("timestamp"));
                                        } catch (Exception ex) {
                                            handleEx("Fatal exception while processing second last timestamp query result", ex);
                                        }
                                    }, "SELECT timestamp FROM stocks WHERE timestamp < ? ORDER BY timestamp DESC LIMIT 1;", lastDayStartTimestamp).join();
                                    if (secondLastTimestamp.get() != null) {
                                        val secondLastDayStartTimestamp = new Timestamp(DateHelper.dayStartCalendar(secondLastTimestamp.get().getTime(), 0).getTimeInMillis());
                                        val secondLastNextDayTimestamp = new Timestamp(DateHelper.dayStartCalendar(secondLastTimestamp.get().getTime(), 1).getTimeInMillis());
                                        log.info("Second last timestamp found: {}", secondLastTimestamp.get());
                                        log.info("Second last day start timestamp: {}", secondLastDayStartTimestamp);
                                        val secondLastFirstTimestamp = new AtomicReference<Timestamp>();
                                        DbHelper.executeQuery(result -> {
                                            try {
                                                secondLastFirstTimestamp.set(result.getTimestamp("timestamp"));
                                            } catch (Exception ex) {
                                                handleEx("Fatal exception while processing first timestamp query result", ex);
                                            }
                                        }, "SELECT timestamp FROM stocks WHERE timestamp >= ? ORDER BY timestamp ASC LIMIT 1;", secondLastDayStartTimestamp).join();
                                        log.info("First timestamp found: {}", secondLastFirstTimestamp.get().toString());
                                        val partName = "_" + secondLastDayStartTimestamp.toString().split(" ")[0].replace("-", "");
                                        if (!CacheHelper.hasTemporary("fc_minify_stocks")) {
                                            DbHelper.executeUpdate("DROP TABLE IF EXISTS stocks_new;").join();
                                            DbHelper.executeUpdate("CREATE TABLE stocks_new (LIKE stocks INCLUDING ALL);").join();
                                            DbHelper.executeUpdate("INSERT INTO stocks_new (timestamp, size_id, warehouse_id, quantity) " +
                                                            "SELECT timestamp, size_id, warehouse_id, quantity FROM stocks WHERE timestamp = ?;",
                                                    secondLastFirstTimestamp.get()).join();
                                            DbHelper.executeUpdate("BEGIN; " +
                                                    "ALTER TABLE stocks" + partName + " RENAME TO stocks_old; " +
                                                    "ALTER TABLE stocks_new RENAME TO stocks" + partName + "; " +
                                                    "DROP TABLE stocks_old; " +
                                                    "ALTER TABLE stocks ATTACH PARTITION stocks" + partName + " FOR VALUES FROM ('" + secondLastDayStartTimestamp + "') TO ('" + secondLastNextDayTimestamp + "'); " +
                                                    "COMMIT;").join();
                                            log.info("Minified last day stocks successfully");
                                            CacheHelper.writeTemporary("fc_minify_stocks");
                                        }
                                        if (!CacheHelper.hasTemporary("fc_minify_orders")) {
                                            DbHelper.executeUpdate("DROP TABLE IF EXISTS orders_new;").join();
                                            DbHelper.executeUpdate("CREATE TABLE orders_new (LIKE orders INCLUDING ALL);").join();
                                            DbHelper.executeUpdate("INSERT INTO orders_new (timestamp, timestamp_to, size_id, warehouse_id, quantity) " +
                                                            "SELECT MIN(timestamp), MAX(timestamp_to), size_id, warehouse_id, SUM(quantity) FROM orders WHERE timestamp >= ? AND timestamp < ? GROUP BY size_id, warehouse_id;",
                                                    secondLastFirstTimestamp.get(), secondLastNextDayTimestamp).join();
                                            DbHelper.executeUpdate("BEGIN; " +
                                                    "ALTER TABLE orders" + partName + " RENAME TO orders_old; " +
                                                    "ALTER TABLE orders_new RENAME TO orders" + partName + "; " +
                                                    "DROP TABLE orders_old; " +
                                                    "ALTER TABLE orders ATTACH PARTITION orders" + partName + " FOR VALUES FROM ('" + secondLastDayStartTimestamp + "') TO ('" + secondLastNextDayTimestamp + "'); " +
                                                    "COMMIT;").join();
                                            log.info("Minified last day orders successfully");
                                            CacheHelper.writeTemporary("fc_minify_orders");
                                        }
                                        if (!CacheHelper.hasTemporary("fc_minify_refills")) {
                                            DbHelper.executeUpdate("DROP TABLE IF EXISTS refills_new;").join();
                                            DbHelper.executeUpdate("CREATE TABLE refills_new (LIKE refills INCLUDING ALL);").join();
                                            DbHelper.executeUpdate("INSERT INTO refills_new (timestamp, timestamp_to, size_id, warehouse_id, quantity) " +
                                                            "SELECT MIN(timestamp), MAX(timestamp_to), size_id, warehouse_id, SUM(quantity) FROM refills WHERE timestamp >= ? AND timestamp < ? GROUP BY size_id, warehouse_id;",
                                                    secondLastFirstTimestamp.get(), secondLastNextDayTimestamp).join();
                                            DbHelper.executeUpdate("BEGIN; " +
                                                    "ALTER TABLE refills" + partName + " RENAME TO refills_old; " +
                                                    "ALTER TABLE refills_new RENAME TO refills" + partName + "; " +
                                                    "DROP TABLE refills_old; " +
                                                    "ALTER TABLE refills ATTACH PARTITION refills" + partName + " FOR VALUES FROM ('" + secondLastDayStartTimestamp + "') TO ('" + secondLastNextDayTimestamp + "'); " +
                                                    "COMMIT;").join();
                                            log.info("Minified last day refills successfully");
                                            CacheHelper.writeTemporary("fc_minify_refills");
                                        }
                                    } else {
                                        log.info("No second last timestamp found, no minifiying needed");
                                    }
                                    CacheHelper.writeTemporary("fc_minify_done");
                                } else {
                                    log.info("Last timestamp stocks/orders/refills already minified");
                                }
                                if (!CacheHelper.hasTemporary("fc_cleanup_done")) {
                                    val edgeTimestamp = new Timestamp(DateHelper.dayStartCalendar(dayTimestamp.getTime(), -Constants.DAY_HISTORY_LENGTH).getTimeInMillis());
                                    val futures = new ArrayList<CompletableFuture<Void>>();
                                    for (val table : Table.values()) {
                                        futures.add(DbHelper.executeUpdate("DROP TABLE IF EXISTS " + table.tableName + "_tmp;"));
                                        futures.add(DbHelper.executeUpdate("DROP TABLE IF EXISTS " + table.tableName + "_old;"));
                                        futures.add(DbHelper.executeUpdate("DROP TABLE IF EXISTS " + table.tableName + "_new;"));
                                        if (table.type == TableType.DICTIONARY) {
                                            futures.add(DbHelper.executeUpdate("DELETE FROM " + table.tableName + " WHERE last_timestamp < ?;", edgeTimestamp));
                                        }
                                    }
                                    val tablesToDelete = new ArrayList<String>();
                                    DbHelper.executeQuery(result -> {
                                        try {
                                            tablesToDelete.add(result.getString("table_name"));
                                        } catch (Exception ex) {
                                            handleEx("Fatal exception while processing old partitions query result", ex);
                                        }
                                    }, "SELECT table_name FROM partitions WHERE timestamp < ?;", edgeTimestamp).join();
                                    for (val tableName : tablesToDelete) {
                                        futures.add(DbHelper.executeUpdate("DROP TABLE IF EXISTS " + tableName + ";"));
                                    }
                                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                                    DbHelper.executeUpdate("DELETE FROM partitions WHERE timestamp < ?;", edgeTimestamp).join();
                                    log.info("Cleaned up old entries and partitions");
                                    CacheHelper.writeTemporary("fc_cleanup_done");
                                } else {
                                    log.info("Cleanup of old entries already done");
                                }
                            } else {
                                log.info("No last timestamp found, skipping minifying and orders/refills calc");
                            }
                            CacheHelper.writeTemporary("fc_finish");
                            log.info("Done with full crawl for today!");
                        } else {
                            log.info("Full crawl is done, trying to run iteration for {}", taskTimestamp);
                            if (!CacheHelper.hasTemporary(taskTimestamp + "_finish")) {
                                if (!CacheHelper.hasTemporary(taskTimestamp + "_warehouses_done")) {
                                    val warehouses = CrawlHelper.crawlWarehouses().join().getWarehouses();
                                    processWarehouses(warehouses, taskTimestamp);
                                    CacheHelper.writeTemporary(taskTimestamp + "_warehouses_done");
                                } else {
                                    log.info("All warehouses already done");
                                }
                                if (!CacheHelper.hasTemporary(taskTimestamp + "_sku_batches_done")) {
                                    val skus = new HashSet<Integer>();
                                    val count = new AtomicInteger();
                                    val futures = new ArrayList<CompletableFuture<Void>>();
                                    DbHelper.executeQuery(Constants.QUERY_BATCH_SIZE, result -> {
                                        try {
                                            int sku = result.getInt("sku");
                                            skus.add(sku);
                                            if (count.incrementAndGet() == CrawlerApi.Constants.PRODUCTS_PER_PAGE) {
                                                log.info("Scheduling batch because it reached the limit");
                                                futures.add(CrawlHelper.crawlStocksBySku(skus)
                                                        .thenComposeAsync(crawled -> {
                                                            return storeOutput(OutputHelper.crawledStocksToOutput(crawled, taskTimestamp));
                                                        }));
                                                skus.clear();
                                                count.set(0);
                                            }
                                            if (futures.size() * CrawlerApi.Constants.PRODUCTS_PER_PAGE >= Constants.QUERY_BATCH_SIZE) {
                                                log.info("Await batch executions before continuing");
                                                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                                                futures.clear();
                                            }
                                        } catch (Exception ex) {
                                            handleEx("Fatal exception while processing iteration query result", ex);
                                        }
                                    }, "SELECT sku FROM products WHERE last_timestamp >= ? ORDER BY id ASC;", dayTimestamp).join();
                                    if (!skus.isEmpty()) {
                                        log.info("Scheduling leftovers from last batch");
                                        futures.add(CrawlHelper.crawlStocksBySku(skus)
                                                .thenComposeAsync(crawled -> {
                                                    return storeOutput(OutputHelper.crawledStocksToOutput(crawled, taskTimestamp));
                                                }));
                                    }
                                    if (!futures.isEmpty()) {
                                        log.info("Await leftover executions before finishing");
                                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                                    }
                                    CacheHelper.writeTemporary(taskTimestamp + "_sku_batches_done");
                                } else {
                                    log.info("All sku batches already done");
                                }
                                if (!CacheHelper.hasTemporary(taskTimestamp + "_imports_done")) {
                                    val sizesImport = importIntoDatabase(Table.SIZES, taskTimestamp, null);
                                    val warehousesImport = importIntoDatabase(Table.WAREHOUSES, taskTimestamp, null);
                                    val stocksImport = sizesImport.thenComposeAsync(ignored -> {
                                        return warehousesImport.thenComposeAsync(ignored2 -> {
                                            return importIntoDatabase(Table.STOCKS, taskTimestamp, null);
                                        });
                                    });
                                    sizesImport.join();
                                    warehousesImport.join();
                                    stocksImport.join();
                                    CacheHelper.writeTemporary(taskTimestamp + "_imports_done");
                                } else {
                                    log.info("Everything already imported into database");
                                }
                                log.info("Searching stocks older than {}", taskTimestamp);
                                val lastTimestamp = new AtomicReference<Timestamp>();
                                DbHelper.executeQuery(result -> {
                                    try {
                                        lastTimestamp.set(result.getTimestamp("timestamp"));
                                    } catch (Exception ex) {
                                        handleEx("Fatal exception while processing last timestamp query result", ex);
                                    }
                                }, "SELECT timestamp FROM stocks WHERE timestamp < ? ORDER BY timestamp DESC LIMIT 1;", taskTimestamp).join();
                                if (lastTimestamp.get() != null) {
                                    if (!CacheHelper.hasTemporary(taskTimestamp + "_ordersrefills_done")) {
                                        log.info("Last timestamp found: {}", lastTimestamp.get());
                                        prepareProcessOrdersRefills(taskTimestamp, dayTimestamp, lastTimestamp.get());
                                        CacheHelper.writeTemporary(taskTimestamp + "_ordersrefills_done");
                                    } else {
                                        log.info("Orders and refills already calculated");
                                    }
                                } else {
                                    log.info("No last timestamp found, skipping cleanup and orders/refills calc");
                                }
                            } else {
                                log.info("Iteration for current point already done, skipping");
                            }
                        }
                        val endTs = new Timestamp(DateHelper.pointStartCalendar().getTimeInMillis());
                        CacheHelper.writeTemporary(endTs + "_finish");
                        CacheHelper.removeTemporary("unfinished_task");
                        for (val cached : OutputTask.channelCache.values()) {
                            cached.close();
                        }
                        OutputTask.channelCache.clear();
                        cleanupOutputDirectory();
                        val delay = DateHelper.nextPointCalendar().getTimeInMillis() - DateHelper.currentCalendar().getTimeInMillis() + 60000;
                        taskExecutor.schedule(this, delay, TimeUnit.MILLISECONDS);
                        log.info("Finished task, waiting until next point ({}ms left)", delay);
                    } catch (Exception ex) {
                        handleEx("Fatal exception in task thread", ex);
                    }
                }
            };
            updateDayVariables();
            if (CacheHelper.hasTemporary("unfinished_task")) {
                taskExecutor.submit(mainTask);
                log.info("Found unfinished task, finishing it now");
            } else {
                val delay = DateHelper.nextPointCalendar().getTimeInMillis() - DateHelper.currentCalendar().getTimeInMillis() + 60000;
                taskExecutor.schedule(mainTask, delay, TimeUnit.MILLISECONDS);
                log.info("No unfinished task, waiting until next point ({}ms left)", delay);
            }
        } catch (Exception ex) {
            handleEx("Fatal exception in main thread", ex);
        }
    }

    public static <T> T handleEx(String message, Throwable ex) {
        log.error(message, ex);
        System.exit(1);
        return null;
    }

    private static void updateDayVariables() {
        val currentTs = new Timestamp(DateHelper.dayStartCalendar(0).getTimeInMillis());
        val lastTs = CacheHelper.getPermanent("last_timestamp");
        if (lastTs.isEmpty() || !lastTs.get().equals(currentTs)) {
            CacheHelper.clearTemporary();
            CacheHelper.writePermanent("last_timestamp", currentTs);
            log.info("Changed last timestamp to {}", currentTs);
        } else {
            log.info("Last timestamp identical to current, no changes");
        }
    }

    private static void cleanupOutputDirectory() throws IOException {
        @Cleanup val stream = Files.list(Paths.get(Constants.OUTPUT_DIR_NAME));
        for (val path : stream.toList()) {
            Files.delete(path);
        }
        log.info("Cleaned up output directory");
    }

    private static HashSet<Catalog> loadStoredCatalogs() throws IOException {
        val catalogs = new HashSet<Catalog>();
        @Cleanup val reader = new BufferedReader(new FileReader(Table.CATALOGS.outputPath.toFile()));
        String line;
        while ((line = reader.readLine()) != null) {
            val split = line.split(Constants.FIELD_SEPARATOR);
            val shard = split[1];
            val query = split[2];
            val catalogKey = new CatalogKey(shard, query);
            val catalog = new Catalog(catalogKey);
            catalogs.add(catalog);
        }
        log.info("Loaded previously stored catalogs");
        return catalogs;
    }

    private static void processWarehouses(Set<Warehouse> warehouses, Timestamp taskTimestamp) {
        log.info("Processing warehouses, please wait...");
        val futures = new ArrayList<CompletableFuture<Void>>();
        for (val warehouse : warehouses) {
            val cache = taskTimestamp.toString() +
                    "_proc_warehouse_" +
                    warehouse.getKey().getWbId();
            if (!CacheHelper.hasTemporary(cache)) {
                futures.add(storeOutput(OutputHelper.warehouseToOutput(warehouse, taskTimestamp))
                        .thenAcceptAsync(ignored -> {
                            CacheHelper.writeTemporary(cache);
                        }));
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        log.info("Done processing warehouses");
    }

    private static void processCategories(Set<Category> categories, Timestamp taskTimestamp) {
        log.info("Processing categories, please wait...");
        val catsToProcess = new AtomicInteger();
        val catsProcessed = new AtomicInteger();
        val futures = new ArrayList<CompletableFuture<Void>>();
        for (val category : categories) {
            val cache = taskTimestamp.toString() +
                    "_proc_category_" +
                    category.getKey().getText();
            if (!CacheHelper.hasTemporary(cache)) {
                catsToProcess.incrementAndGet();
                futures.add(collectCategory(category)
                        .thenComposeAsync(collected -> {
                            return storeOutput(OutputHelper.collectedCategoryToOutput(collected, taskTimestamp));
                        })
                        .thenAcceptAsync(ignored -> {
                            CacheHelper.writeTemporary(cache);
                            catsProcessed.incrementAndGet();
                            if (catsProcessed.get() % 10 == 0 || catsProcessed.get() == catsToProcess.get()) {
                                log.info("Category {} / {} ({}%) - {}",
                                        catsProcessed.get(),
                                        catsToProcess.get(),
                                        Math.round((float) catsProcessed.get() / catsToProcess.get() * 10000) / 100,
                                        formatUsedMemoryInMb());
                            }
                        }));
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        log.info("Done processing categories");
    }

    private static void processCatalogs(Set<Catalog> catalogs, Timestamp taskTimestamp) {
        log.info("Processing catalogs, please wait...");
        for (var page = 1; page <= CrawlerApi.Constants.PAGES_PER_CATALOG; page++) {
            val pagesToProcess = new AtomicInteger();
            val pagesProcessed = new AtomicInteger();
            val futures = new ArrayList<CompletableFuture<Void>>();
            for (val catalog : catalogs) {
                val cache = taskTimestamp.toString() +
                        "_proc_catalog_" +
                        catalog.getKey().getShard() +
                        "_" +
                        catalog.getKey().getQuery() +
                        "_" +
                        page;
                if (!CacheHelper.hasTemporary(cache)) {
                    val finalPage = page;
                    pagesToProcess.incrementAndGet();
                    futures.add(collectCatalogPage(catalog, page)
                            .thenComposeAsync(collected -> {
                                return storeOutput(OutputHelper.collectedCatalogPageToOutput(collected, taskTimestamp));
                            })
                            .thenAcceptAsync(ignored -> {
                                CacheHelper.writeTemporary(cache);
                                pagesProcessed.incrementAndGet();
                                if (pagesProcessed.get() % 10 == 0 || pagesProcessed.get() == pagesToProcess.get()) {
                                    log.info("Catalog page {} / {}: {} / {} ({}%) - {}",
                                            finalPage,
                                            CrawlerApi.Constants.PAGES_PER_CATALOG,
                                            pagesProcessed.get(),
                                            pagesToProcess.get(),
                                            Math.round((float) pagesProcessed.get() / pagesToProcess.get() * 10000) / 100,
                                            formatUsedMemoryInMb());
                                }
                            }));
                }
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }
        log.info("Done processing catalogs");
    }

    private static void prepareProcessOrdersRefills(Timestamp taskTimestamp, Timestamp dayTimestamp, Timestamp lastTimestamp) {
        processOrdersRefills(lastTimestamp, taskTimestamp);
        val importOrders = importIntoDatabase(Table.ORDERS, taskTimestamp, lastTimestamp);
        val importRefills = importIntoDatabase(Table.REFILLS, taskTimestamp, lastTimestamp);
        importOrders.join();
        importRefills.join();
    }

    private static void processOrdersRefills(Timestamp taskTimestamp, Timestamp timestampTo) {
        val sql = "SELECT products.sku AS product_sku, " +
                "sizes.name AS size_name, " +
                "sizes.alt_name AS size_alt_name, " +
                "warehouses.wb_id AS warehouse_wb_id, " +
                "warehouses.name AS warehouse_name, " +
                "s2.quantity - s1.quantity AS diff " +
                "FROM stocks s1 " +
                "LEFT JOIN sizes ON sizes.id = s1.size_id " +
                "LEFT JOIN warehouses ON warehouses.id = s1.warehouse_id " +
                "LEFT JOIN products ON products.id = sizes.product_id " +
                "LEFT JOIN stocks s2 ON s2.size_id = s1.size_id AND s2.warehouse_id = s1.warehouse_id AND s2.timestamp = ? " +
                "WHERE s1.timestamp = ?;";
        val orders = new HashSet<Order>();
        val refills = new HashSet<Refill>();
        val count = new AtomicInteger();
        DbHelper.executeQuery(Constants.QUERY_BATCH_SIZE, result -> {
            try {
                val productSku = result.getInt("product_sku");
                val sizeName = result.getString("size_name");
                val sizeAltName = result.getString("size_alt_name");
                val warehouseWbId = result.getInt("warehouse_wb_id");
                val warehouseName = result.getString("warehouse_name");
                val diff = result.getInt("diff");
                val productKey = new ProductKey(productSku);
                val sizeKey = new SizeKey(productKey, sizeName);
                val warehouseKey = new WarehouseKey(warehouseWbId);
                val merchKey = new MerchKey(sizeKey, warehouseKey);
                val order = diff < 0;
                val refill = diff > 0;
                if (order) {
                    orders.add(new Order(merchKey, Math.abs(diff)));
                } else if (refill) {
                    refills.add(new Refill(merchKey, diff));
                }
                if (count.incrementAndGet() == Constants.QUERY_BATCH_SIZE) {
                    log.info("Storing batch because it reached the limit");
                    storeOutput(OutputHelper.ordersRefillsToOutput(orders, refills, taskTimestamp, timestampTo)).join();
                    orders.clear();
                    refills.clear();
                    count.set(0);
                }
            } catch (Exception ex) {
                handleEx("Fatal exception while processing stocks/refills query result", ex);
            }
        }, sql, timestampTo, taskTimestamp).join();
        if (!orders.isEmpty() || !refills.isEmpty()) {
            log.info("Storing leftovers from last batch");
            storeOutput(OutputHelper.ordersRefillsToOutput(orders, refills, taskTimestamp, timestampTo)).join();
        }
    }

    private static CompletableFuture<Void> importIntoDatabase(Table table, Timestamp taskTimestamp, Timestamp timestampFrom) {
        val cache = taskTimestamp.toString() +
                "_imp_table_" +
                table.tableName;
        if (CacheHelper.hasTemporary(cache)) {
            return CompletableFuture.completedFuture(null);
        }
        return DbHelper.executeUpdate("DROP TABLE IF EXISTS " + table.tableName + "_tmp;")
                .thenComposeAsync(ignored -> {
                    return DbHelper.executeUpdate("CREATE TABLE " + table.tableName + "_tmp (" + table.tmpSchema + ");");
                })
                .thenComposeAsync(ignored -> {
                    log.info("Started copying {}", table.tableName);
                    return DbHelper.executeUpdate("COPY " + table.tableName + "_tmp FROM '" + table.outputPath.toAbsolutePath() + "';");
                })
                .thenComposeAsync(ignored -> {
                    log.info("Finished copying {}", table.tableName);
                    if (table.type == TableType.HISTORY) {
                        val lowTs = new Timestamp(DateHelper.dayStartCalendar(timestampFrom != null ? timestampFrom.getTime() : taskTimestamp.getTime(), 0).getTimeInMillis());
                        val highTs = new Timestamp(DateHelper.dayStartCalendar(timestampFrom != null ? timestampFrom.getTime() : taskTimestamp.getTime(), 1).getTimeInMillis());
                        val partName = "_" + lowTs.toString().split(" ")[0].replace("-", "");
                        return DbHelper.executeUpdate("INSERT INTO partitions (timestamp, table_name) VALUES (?, ?) ON CONFLICT (table_name) DO NOTHING;", lowTs, table.tableName + partName)
                                .thenComposeAsync(ignored2 -> {
                                    return DbHelper.executeUpdate("CREATE TABLE IF NOT EXISTS " + table.tableName + partName + " PARTITION OF " + table.tableName + " FOR VALUES FROM ('" + lowTs + "') TO ('" + highTs + "');");
                                });
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenComposeAsync(ignored -> {
                    log.info("Started importing {}", table.tableName);
                    return DbHelper.executeUpdate(table.importSql.replace("%table%", table.tableName));
                })
                .thenComposeAsync(ignored -> {
                    log.info("Finished importing {}", table.tableName);
                    return DbHelper.executeUpdate("DROP TABLE " + table.tableName + "_tmp;");
                })
                .thenAcceptAsync(ignored -> {
                    CacheHelper.writeTemporary(cache);
                });
    }

    private static CompletableFuture<CollectedCategory> collectCategory(Category category) {
        return CrawlHelper.crawlQueries(category).exceptionallyAsync(ex -> handleEx("Unable to collect category because queries crawl failed", ex))
                .thenComposeAsync(queriesRes -> {
                    val sources = new HashSet<Source>(queriesRes.getQueries());
                    sources.add(category);
                    var catalogFuture = CompletableFuture.completedFuture(new HashSet<Catalog>());
                    for (val source : sources) {
                        catalogFuture = catalogFuture.thenCombineAsync(CrawlHelper.crawlCatalog(source).exceptionallyAsync(ex -> handleEx("Unable to collect category because catalog crawl failed", ex)), (oldSet, catalogRes) -> {
                            val newSet = new HashSet<>(oldSet);
                            newSet.add(catalogRes.getCatalog());
                            return newSet;
                        });
                    }
                    return catalogFuture.thenApplyAsync(catalogs -> {
                        return new CollectedCategory(catalogs, sources);
                    });
                });
    }

    private static CompletableFuture<CollectedCatalogPage> collectCatalogPage(Catalog catalog, int page) {
        return CrawlHelper.crawlCatalogPage(catalog, page).exceptionallyAsync(ex -> handleEx("Unable to collect catalog page because page crawl failed", ex))
                .thenComposeAsync(pageRes -> {
                    var sellerFuture = CompletableFuture.completedFuture(new HashSet<Seller>());
                    for (val product : pageRes.getProducts()) {
                        sellerFuture = sellerFuture.thenCombineAsync(CrawlHelper.crawlSeller(product), (oldSet, sellerRes) -> {
                            val sel = sellerRes.getSeller();
                            if (sel != null) {
                                val newSet = new HashSet<>(oldSet);
                                newSet.add(sel);
                                return newSet;
                            } else {
                                return oldSet;
                            }
                        });
                    }
                    return sellerFuture.thenComposeAsync(sellers -> {
                        return CrawlHelper.crawlStocksByProduct(pageRes.getProducts()).exceptionallyAsync(ex -> handleEx("Unable to collect catalog page because stocks crawl failed", ex))
                                .thenApplyAsync(stocksRes -> {
                                    return new CollectedCatalogPage(stocksRes.getSizes(), stocksRes.getWarehouses(), stocksRes.getStocks(), sellers, pageRes.getBrands(), pageRes.getProducts(), pageRes.getProductDetails(), pageRes.getPositions());
                                });
                    });
                });
    }

    private static CompletableFuture<Void> storeOutput(Map<Path, ByteBuffer> transformed) {
        val callback = new CompletableFuture<Void>();
        val task = new OutputTask(transformed, callback);
        outputExecutor.execute(task);
        return callback;
    }

    private static String formatUsedMemoryInMb() {
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1048576 + " MB";
    }

}
