package ru.statsklad13.wb.crawler.api.helpers;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;
import lombok.val;
import ru.statsklad13.wb.crawler.api.CrawlerApi;
import ru.statsklad13.wb.crawler.api.data.key.merch.MerchKey;
import ru.statsklad13.wb.crawler.api.data.key.merch.SizeKey;
import ru.statsklad13.wb.crawler.api.data.key.merch.WarehouseKey;
import ru.statsklad13.wb.crawler.api.data.key.product.*;
import ru.statsklad13.wb.crawler.api.data.key.source.CatalogKey;
import ru.statsklad13.wb.crawler.api.data.key.source.SourceKey;
import ru.statsklad13.wb.crawler.api.data.merch.Size;
import ru.statsklad13.wb.crawler.api.data.merch.Stock;
import ru.statsklad13.wb.crawler.api.data.merch.Warehouse;
import ru.statsklad13.wb.crawler.api.data.misc.PrioritizedTask;
import ru.statsklad13.wb.crawler.api.data.product.*;
import ru.statsklad13.wb.crawler.api.data.result.*;
import ru.statsklad13.wb.crawler.api.data.source.Catalog;
import ru.statsklad13.wb.crawler.api.data.source.Category;
import ru.statsklad13.wb.crawler.api.data.source.Query;
import ru.statsklad13.wb.crawler.api.data.source.Source;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class CrawlHelper {

    public static CompletableFuture<CrawledCatalog> crawlCatalog(Source source) {
        return crawlCatalog(CrawlerApi.Constants.DEFAULT_PRIORITY, source);
    }

    public static CompletableFuture<CrawledCatalog> crawlCatalog(PrioritizedTask.Level priorityLevel, Source source) {
        val url = CrawlerApi.createCatalogUrl(source);
        return WebHelper.sendGetRequest(priorityLevel, url, 200).thenApplyAsync(webResponse -> {
            try {
                val json = (JsonObject) Jsoner.deserialize(webResponse.getBody());
                val jsonQuery = (String) json.get("query");
                val jsonShardKey = (String) json.get("shardKey");
                val catalogKey = new CatalogKey(jsonShardKey, jsonQuery);
                val catalog = new Catalog(catalogKey);
                source.getRelatedCatalogKey().set(catalogKey);
                return new CrawledCatalog(catalog);
            } catch (Exception ex) {
                throw new CompletionException("Unable to crawl catalog! url: " + url, ex);
            }
        });
    }

    public static CompletableFuture<CrawledCatalogPage> crawlCatalogPage(Catalog catalog, int page) {
        return crawlCatalogPage(CrawlerApi.Constants.DEFAULT_PRIORITY, catalog, page);
    }

    public static CompletableFuture<CrawledCatalogPage> crawlCatalogPage(PrioritizedTask.Level priorityLevel, Catalog catalog, int page) {
        if (page > catalog.getEmptyPage().get()) {
            return CompletableFuture.completedFuture(new CrawledCatalogPage(new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>()));
        }
        val url = CrawlerApi.createCatalogPageUrl(catalog, page);
        return WebHelper.sendGetRequest(priorityLevel, url, 200, 400, 404).thenApplyAsync(webResponse -> {
            try {
                val brands = new HashSet<Brand>();
                val products = new HashSet<Product>();
                val productDetails = new HashSet<ProductDetail>();
                val positions = new HashSet<Position>();
                if (webResponse.getCode() == 200 && !webResponse.getBody().isEmpty()) {
                    val jsonArr = (JsonArray) ((JsonObject) ((JsonObject) Jsoner.deserialize(webResponse.getBody())).get("data")).get("products");
                    for (var pos = 0; pos < jsonArr.size(); pos++) {
                        val actualPos = (page - 1) * CrawlerApi.Constants.PRODUCTS_PER_PAGE + pos + 1;
                        if (actualPos > CrawlerApi.Constants.POSITION_PLACE_CAP) {
                            continue;
                        }
                        val json = (JsonObject) jsonArr.get(pos);
                        val jsonId = (BigDecimal) json.get("id");
                        val jsonName = (String) json.get("name");
                        val jsonBrand = (String) json.get("brand");
                        val jsonBrandId = (BigDecimal) json.get("brandId");
                        val jsonPriceU = (BigDecimal) json.get("priceU");
                        val jsonSalePriceU = (BigDecimal) json.get("salePriceU");
                        val jsonRating = (BigDecimal) json.get("rating");
                        val jsonFeedbacks = (BigDecimal) json.get("feedbacks");
                        val brandKey = new BrandKey(jsonBrandId.intValueExact());
                        val brand = new Brand(brandKey, jsonBrand, CrawlerApi.createBrandImageUrl(jsonBrandId.intValueExact()));
                        val productKey = new ProductKey(jsonId.intValueExact());
                        val product = new Product(productKey, jsonName, CrawlerApi.createProductImageUrl(jsonId.intValueExact()));
                        product.getRelatedBrandKey().set(brandKey);
                        val productDetailKey = new ProductDetailKey(productKey);
                        val productDetail = new ProductDetail(productDetailKey, jsonPriceU.intValueExact() / 100, jsonSalePriceU.intValueExact() / 100, jsonFeedbacks.intValueExact(), jsonRating.intValueExact());
                        val positionKey = new PositionKey(productKey, catalog.getKey());
                        val position = new Position(positionKey, actualPos);
                        brands.add(brand);
                        products.add(product);
                        productDetails.add(productDetail);
                        positions.add(position);
                    }
                }
                if (brands.isEmpty() && products.isEmpty() && productDetails.isEmpty() && positions.isEmpty()) {
                    catalog.getEmptyPage().set(page);
                }
                return new CrawledCatalogPage(brands, products, productDetails, positions);
            } catch (Exception ex) {
                throw new CompletionException("Unable to crawl catalog page! url: " + url, ex);
            }
        });
    }

    public static CompletableFuture<CrawledCategories> crawlCategories() {
        return crawlCategories(CrawlerApi.Constants.DEFAULT_PRIORITY);
    }

    public static CompletableFuture<CrawledCategories> crawlCategories(PrioritizedTask.Level priorityLevel) {
        return WebHelper.sendGetRequest(priorityLevel, CrawlerApi.Constants.CATEGORIES_URL, 200).thenApplyAsync(webResponse -> {
            try {
                val jsonArr = (JsonArray) Jsoner.deserialize(webResponse.getBody());
                val categories = traverseCategoryTree(jsonArr, "");
                return new CrawledCategories(categories);
            } catch (Exception ex) {
                throw new CompletionException("Unable to crawl categories!", ex);
            }
        });
    }

    public static CompletableFuture<CrawledQueries> crawlQueries(Source source) {
        return crawlQueries(CrawlerApi.Constants.DEFAULT_PRIORITY, source);
    }

    public static CompletableFuture<CrawledQueries> crawlQueries(PrioritizedTask.Level priorityLevel, Source source) {
        val url = CrawlerApi.createQueriesUrl(source);
        return WebHelper.sendGetRequest(priorityLevel, url, 200).thenApplyAsync(webResponse -> {
            try {
                val queries = new HashSet<Query>();
                val jsonArr = (JsonArray) (source instanceof Category ?
                        Jsoner.deserialize(webResponse.getBody()) :
                        ((JsonObject) Jsoner.deserialize(webResponse.getBody())).get("query"));
                for (val obj : jsonArr) {
                    val text = (String) obj;
                    if (!text.isEmpty()) {
                        try {
                            Integer.parseInt(text);
                        } catch (Exception ignored) {
                            val sourceKey = new SourceKey(text);
                            val query = new Query(sourceKey);
                            queries.add(query);
                        }
                    }
                }
                return new CrawledQueries(queries);
            } catch (Exception ex) {
                throw new CompletionException("Unable to crawl queries! url: " + url, ex);
            }
        });
    }

    public static CompletableFuture<CrawledSeller> crawlSeller(Product product) {
        return crawlSeller(CrawlerApi.Constants.DEFAULT_PRIORITY, product);
    }

    public static CompletableFuture<CrawledSeller> crawlSeller(PrioritizedTask.Level priorityLevel, Product product) {
        val url = CrawlerApi.createSellerUrl(product.getKey().getSku());
        return WebHelper.sendGetRequest(priorityLevel, url, 200, 404).thenApplyAsync(webResponse -> {
            try {
                if (webResponse.getCode() == 200) {
                    val json = (JsonObject) Jsoner.deserialize(webResponse.getBody());
                    val jsonSupplierName = (String) json.get("supplierName");
                    val jsonTrademark = (String) json.get("trademark");
                    val name = jsonTrademark != null && !jsonTrademark.isEmpty() ? jsonTrademark : jsonSupplierName;
                    if (name != null && !name.isEmpty()) {
                        val jsonSupplierId = (BigDecimal) json.get("supplierId");
                        val jsonInn = (String) json.get("inn");
                        val jsonOgrn = (String) json.get("ogrn");
                        val jsonOgrnip = (String) json.get("ogrnip");
                        val jsonLegalAddress = (String) json.get("legalAddress");
                        val sellerKey = new SellerKey(jsonSupplierId.intValueExact());
                        val seller = new Seller(sellerKey, name, CrawlerApi.createSellerImageUrl(jsonSupplierId.intValueExact()), jsonInn, jsonOgrn, jsonOgrnip, jsonLegalAddress);
                        product.getRelatedSellerKey().set(sellerKey);
                        return new CrawledSeller(seller);
                    }
                }
                return new CrawledSeller(null);
            } catch (Exception ex) {
                throw new CompletionException("Unable to crawl seller! url: " + url, ex);
            }
        });
    }

    public static CompletableFuture<CrawledStocks> crawlStocksByProduct(Collection<Product> products) {
        return crawlStocksByProduct(CrawlerApi.Constants.DEFAULT_PRIORITY, products);
    }

    public static CompletableFuture<CrawledStocks> crawlStocksByProduct(PrioritizedTask.Level priorityLevel, Collection<Product> products) {
        val skus = new HashSet<Integer>();
        for (val product : products) {
            skus.add(product.getKey().getSku());
        }
        return crawlStocksBySku(priorityLevel, skus);
    }

    public static CompletableFuture<CrawledStocks> crawlStocksBySku(Collection<Integer> skus) {
        return crawlStocksBySku(CrawlerApi.Constants.DEFAULT_PRIORITY, skus);
    }

    public static CompletableFuture<CrawledStocks> crawlStocksBySku(PrioritizedTask.Level priorityLevel, Collection<Integer> skus) {
        val url = CrawlerApi.createStocksUrl(skus);
        return WebHelper.sendGetRequest(priorityLevel, url, 200).thenApplyAsync(webResponse -> {
            try {
                val sizes = new HashSet<Size>();
                val warehouses = new HashSet<Warehouse>();
                val stocks = new HashSet<Stock>();
                val jsonArr = (JsonArray) ((JsonObject) ((JsonObject) Jsoner.deserialize(webResponse.getBody())).get("data")).get("products");
                for (val obj : jsonArr) {
                    val json = (JsonObject) obj;
                    val jsonId = (BigDecimal) json.get("id");
                    val jsonSizes = (JsonArray) json.get("sizes");
                    for (val obj2 : jsonSizes) {
                        val json2 = (JsonObject) obj2;
                        val json2Name = (String) json2.get("name");
                        val json2OrigName = (String) json2.get("origName");
                        val json2Stocks = (JsonArray) json2.get("stocks");
                        val sizeKey = new SizeKey(new ProductKey(jsonId.intValueExact()), json2OrigName);
                        val size = new Size(sizeKey, json2Name);
                        sizes.add(size);
                        for (val obj3 : json2Stocks) {
                            val json3 = (JsonObject) obj3;
                            val json3Wh = (BigDecimal) json3.get("wh");
                            val json3Qty = (BigDecimal) json3.get("qty");
                            val warehouseKey = new WarehouseKey(json3Wh.intValueExact());
                            val warehouse = new Warehouse(warehouseKey, null);
                            val merchKey = new MerchKey(sizeKey, warehouseKey);
                            val stock = new Stock(merchKey, json3Qty.intValueExact());
                            warehouses.add(warehouse);
                            stocks.add(stock);
                        }
                    }
                }
                return new CrawledStocks(sizes, warehouses, stocks);
            } catch (Exception ex) {
                throw new CompletionException("Unable to crawl stocks! url: " + url, ex);
            }
        });
    }

    public static CompletableFuture<CrawledWarehouses> crawlWarehouses() {
        return crawlWarehouses(CrawlerApi.Constants.DEFAULT_PRIORITY);
    }

    public static CompletableFuture<CrawledWarehouses> crawlWarehouses(PrioritizedTask.Level priorityLevel) {
        val future = new CompletableFuture<CrawledWarehouses>();
        try {
            val warehouses = new HashSet<Warehouse>();
            val line = Files.readAllLines(CrawlerApi.Constants.WAREHOUSE_RESPONSE_PATH).get(0);
            val jsonArr = (JsonArray) ((JsonObject) ((JsonObject) ((JsonObject) Jsoner.deserialize(line)).get("result")).get("resp")).get("data");
            for (val obj : jsonArr) {
                val json = (JsonObject) obj;
                val jsonOrigid = (BigDecimal) json.get("origid");
                val jsonWarehouse = (String) json.get("warehouse");
                val name = jsonWarehouse.contains("Ð") ? decodeWarehouse(jsonWarehouse) : jsonWarehouse;
                val warehouseKey = new WarehouseKey(jsonOrigid.intValueExact());
                val warehouse = new Warehouse(warehouseKey, name);
                warehouses.add(warehouse);
            }
            future.complete(new CrawledWarehouses(warehouses));
        } catch (Exception ex) {
            future.completeExceptionally(ex);
        }
        return future;
    }

    private static HashSet<Category> traverseCategoryTree(JsonArray jsonArr, String prefix) {
        val categories = new HashSet<Category>();
        for (val obj : jsonArr) {
            val json = (JsonObject) obj;
            val jsonName = (String) json.get("name");
            val jsonChilds = (JsonArray) json.get("childs");
            if (jsonChilds != null) {
                categories.addAll(traverseCategoryTree(jsonChilds, prefix + jsonName + CrawlerApi.Constants.CATEGORY_SEPARATOR));
            }
            val jsonUrl = (String) json.get("url");
            val jsonShard = (String) json.get("shard");
            val jsonQuery = (String) json.get("query");
            val jsonLanding = (Boolean) json.get("landing");
            if (jsonUrl.startsWith(CrawlerApi.Constants.CATEGORY_START) && jsonShard != null && jsonLanding == null) {
                val sourceKey = new SourceKey(prefix + jsonName);
                val category = new Category(sourceKey, jsonUrl, jsonShard, jsonQuery);
                categories.add(category);
            }
        }
        return categories;
    }

    private static String decodeWarehouse(String encoded) {
        val output = new byte[encoded.length()];
        var add = 0;
        var counter = 0;
        char value;
        char extra;
        while (counter < output.length) {
            value = encoded.charAt(counter++);
            if (value >= 0xD800 && value <= 0xDBFF && counter < output.length) {
                extra = encoded.charAt(counter++);
                if ((extra & 0xFC00) == 0xDC00) {
                    output[add++] = (byte) (((value & 0x3FF) << 10) + (extra & 0x3FF) + 0x10000);
                } else {
                    output[add++] = (byte) value;
                    counter--;
                }
            } else {
                output[add++] = (byte) value;
            }
        }
        return new String(output, StandardCharsets.UTF_8);
    }

}
