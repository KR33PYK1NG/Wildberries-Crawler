package ru.statsklad13.wb.crawler.api;

import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import ru.statsklad13.wb.crawler.api.data.misc.PrioritizedTask;
import ru.statsklad13.wb.crawler.api.data.source.Catalog;
import ru.statsklad13.wb.crawler.api.data.source.Category;
import ru.statsklad13.wb.crawler.api.data.source.Source;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CrawlerApi {

    public static class Constants {

        public static final String CATEGORIES_URL = "https://www.wildberries.ru/webapi/menu/main-menu-ru-ru.json";
        public static final String CATALOG_URL_T = "https://search.wb.ru/exactmatch/ru/common/v4/search?resultset=dummy&query=%query%";
        public static final String CATEGORY_PAGE_URL_T = "https://catalog.wb.ru/catalog/%shard%/catalog?locale=ru&limit=300&%query%&page=%page%";
        public static final String QUERY_PAGE_URL_T = "https://search.wb.ru/%shard%/catalog?dest=-1029256,-2095259,-369838,-1784077&locale=ru&limit=300&%query%&page=%page%";
        public static final String CATEGORY_QUERIES_URL_T = "https://similar-queries.wildberries.ru/catalog?url=%url%";
        public static final String SIMILAR_QUERIES_URL_T = "https://similar-queries.wildberries.ru/api/v2/search/query?query=%query%&regions=68,64,83,4,38,80,33,70,82,86,30,69,1,48,22,66,31,40&locale=ru&lang=ru&dest=-1029256,-2095259,-369838,-1784077";
        public static final String SELLER_URL_T = "https://basket-%basket%.wb.ru/vol%vol%/part%part%/%sku%/info/sellers.json";
        public static final String STOCKS_URL_T = "https://card.wb.ru/cards/detail?spp=26&regions=80,64,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&pricemarginCoeff=1.0&reg=1&appType=1&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,7,3,18,21&sppFixGeo=4&dest=-1029256,-2095259,-369838,-1784077&nm=%skus%";
        public static final String PRODUCT_IMAGE_URL_T = "https://basket-%basket%.wb.ru/vol%vol%/part%part%/%sku%/images/big/1.jpg";
        public static final String BRAND_IMAGE_URL_T = "https://images.wbstatic.net/brands/small/%id%.jpg";
        public static final String SELLER_IMAGE_URL_T = "https://images.wbstatic.net/shops/%id%_logo.jpg";
        public static final String CATEGORY_START = "/catalog";
        public static final String CATEGORY_SEPARATOR = "/";
        public static final int PRODUCTS_PER_PAGE = 300;
        public static final int PAGES_PER_CATALOG = 17;
        public static final int POSITION_PLACE_CAP = 5000;
        public static final Path WAREHOUSE_RESPONSE_PATH = Paths.get("warehouses.json");
        public static final int DEFAULT_BATCH_SIZE = 0;
        public static final PrioritizedTask.Level DEFAULT_PRIORITY = PrioritizedTask.Level.MEDIUM;

    }

    public static class Settings {

        @Getter private static String timezone;
        @Getter private static String databaseUrl;
        @Getter private static String databaseUsername;
        @Getter private static int databaseThreads;
        @Getter private static int webThreads;
        @Getter private static int webMaxRetries;
        @Getter private static long webRetryDelayMs;
        private static String databasePassword;

        public static String getDatabasePasswordOnce() {
            val password = databasePassword;
            databasePassword = null;
            return password;
        }

        public static void load(String settingsFile) throws IOException {
            @Cleanup val stream = Files.newInputStream(Paths.get(settingsFile));
            val props = new Properties();
            props.load(stream);
            timezone = props.getProperty("timezone");
            databaseUrl = props.getProperty("database_url");
            databaseUsername = props.getProperty("database_username");
            databasePassword = props.getProperty("database_password");
            databaseThreads = Integer.parseInt(props.getProperty("database_threads"));
            webThreads = Integer.parseInt(props.getProperty("web_threads"));
            webMaxRetries = Integer.parseInt(props.getProperty("web_max_retries"));
            webRetryDelayMs = Long.parseLong(props.getProperty("web_retry_delay_ms"));
            TimeZone.setDefault(TimeZone.getTimeZone(timezone));
        }

    }

    public static ThreadFactory createFactory(String name, boolean numbered) {
        return new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                val thread = new Thread(r);
                thread.setName(name + (numbered ? " " + this.counter.incrementAndGet() : ""));
                return thread;
            }
        };
    }

    public static String createCatalogUrl(Source source) {
        return Constants.CATALOG_URL_T.replace("%query%", URLEncoder.encode(source.getKey().getText(), StandardCharsets.UTF_8));
    }

    public static String createCatalogPageUrl(Catalog catalog, int page) {
        val s = catalog.getKey().getShard();
        return (s.startsWith("presets/") || s.startsWith("brands/") || s.startsWith("merger") ?
                Constants.QUERY_PAGE_URL_T :
                Constants.CATEGORY_PAGE_URL_T).replace("%shard%", s)
                .replace("%query%", catalog.getKey().getQuery())
                .replace("%page%", String.valueOf(page));
    }

    public static String createQueriesUrl(Source source) {
        return source instanceof Category ?
                Constants.CATEGORY_QUERIES_URL_T.replace("%url%", ((Category) source).getUrl()) :
                Constants.SIMILAR_QUERIES_URL_T.replace("%query%", URLEncoder.encode(source.getKey().getText(), StandardCharsets.UTF_8));
    }

    public static String createSellerUrl(int sku) {
        return Constants.SELLER_URL_T.replace("%basket%", extractBasketFromSku(sku))
                .replace("%vol%", extractVolFromSku(sku))
                .replace("%part%", extractPartFromSku(sku))
                .replace("%sku%", String.valueOf(sku));
    }

    public static String createStocksUrl(Collection<Integer> skus) {
        val sj = new StringJoiner(";");
        for (val sku : skus) {
            sj.add(String.valueOf(sku));
        }
        return Constants.STOCKS_URL_T.replace("%skus%", sj.toString());
    }

    public static String createProductImageUrl(int sku) {
        return Constants.PRODUCT_IMAGE_URL_T.replace("%basket%", extractBasketFromSku(sku))
                .replace("%vol%", extractVolFromSku(sku))
                .replace("%part%", extractPartFromSku(sku))
                .replace("%sku%", String.valueOf(sku));
    }

    public static String createBrandImageUrl(int brandId) {
        return Constants.BRAND_IMAGE_URL_T.replace("%id%", String.valueOf(brandId));
    }

    public static String createSellerImageUrl(int sellerId) {
        return Constants.SELLER_IMAGE_URL_T.replace("%id%", String.valueOf(sellerId));
    }

    private static String extractBasketFromSku(int sku) {
        if (sku < 14400000) {
            return "01";
        } else if (sku < 28800000) {
            return "02";
        } else if (sku < 43200000) {
            return "03";
        } else if (sku < 72000000) {
            return "04";
        } else if (sku < 100800000) {
            return "05";
        } else if (sku < 106200000) {
            return "06";
        } else if (sku < 111600000) {
            return "07";
        } else if (sku < 117000000) {
            return "08";
        } else if (sku < 131400000) {
            return "09";
        } else if (sku < 160200000) {
            return "10";
        } else {
            return "11";
        }
    }

    private static String extractVolFromSku(int sku) {
        return String.valueOf(sku / 100000);
    }

    private static String extractPartFromSku(int sku) {
        return String.valueOf(sku / 1000);
    }

}
