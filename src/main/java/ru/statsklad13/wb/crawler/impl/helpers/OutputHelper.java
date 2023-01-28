package ru.statsklad13.wb.crawler.impl.helpers;

import lombok.val;
import ru.statsklad13.wb.crawler.api.data.merch.Merch;
import ru.statsklad13.wb.crawler.api.data.merch.Size;
import ru.statsklad13.wb.crawler.api.data.merch.Warehouse;
import ru.statsklad13.wb.crawler.api.data.product.*;
import ru.statsklad13.wb.crawler.api.data.result.CrawledStocks;
import ru.statsklad13.wb.crawler.api.data.source.Catalog;
import ru.statsklad13.wb.crawler.api.data.source.Category;
import ru.statsklad13.wb.crawler.api.data.source.Query;
import ru.statsklad13.wb.crawler.api.data.source.Source;
import ru.statsklad13.wb.crawler.impl.CrawlerImpl;
import ru.statsklad13.wb.crawler.impl.data.merch.Order;
import ru.statsklad13.wb.crawler.impl.data.merch.Refill;
import ru.statsklad13.wb.crawler.impl.data.result.CollectedCatalogPage;
import ru.statsklad13.wb.crawler.impl.data.result.CollectedCategory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.*;

public class OutputHelper {

    public static Map<Path, ByteBuffer> warehouseToOutput(Warehouse warehouse, Timestamp taskTimestamp) {
        val transformed = new HashMap<Path, ByteBuffer>();
        transformed.put(CrawlerImpl.Table.WAREHOUSES.getOutputPath(), createByteBuffer(warehouse, taskTimestamp, null));
        return transformed;
    }

    public static Map<Path, ByteBuffer> collectedCategoryToOutput(CollectedCategory collected, Timestamp taskTimestamp) {
        val transformed = new HashMap<Path, ByteBuffer>();
        val categories = new ArrayList<Category>();
        val queries = new ArrayList<Query>();
        for (val source : collected.getSources()) {
            if (source instanceof Category) {
                categories.add((Category) source);
            } else {
                queries.add((Query) source);
            }
        }
        transformed.put(CrawlerImpl.Table.CATALOGS.getOutputPath(), createByteBuffer(collected.getCatalogs(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.CATEGORIES.getOutputPath(), createByteBuffer(categories, taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.QUERIES.getOutputPath(), createByteBuffer(queries, taskTimestamp, null));
        return transformed;
    }

    public static Map<Path, ByteBuffer> collectedCatalogPageToOutput(CollectedCatalogPage collected, Timestamp taskTimestamp) {
        val transformed = new HashMap<Path, ByteBuffer>();
        transformed.put(CrawlerImpl.Table.SIZES.getOutputPath(), createByteBuffer(collected.getSizes(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.WAREHOUSES.getOutputPath(), createByteBuffer(collected.getWarehouses(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.STOCKS.getOutputPath(), createByteBuffer(collected.getStocks(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.SELLERS.getOutputPath(), createByteBuffer(collected.getSellers(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.BRANDS.getOutputPath(), createByteBuffer(collected.getBrands(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.PRODUCTS.getOutputPath(), createByteBuffer(collected.getProducts(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.PRODUCT_DETAILS.getOutputPath(), createByteBuffer(collected.getProductDetails(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.POSITIONS.getOutputPath(), createByteBuffer(collected.getPositions(), taskTimestamp, null));
        return transformed;
    }

    public static Map<Path, ByteBuffer> ordersRefillsToOutput(Collection<Order> orders, Collection<Refill> refills, Timestamp taskTimestamp, Timestamp timestampTo) {
        val transformed = new HashMap<Path, ByteBuffer>();
        transformed.put(CrawlerImpl.Table.ORDERS.getOutputPath(), createByteBuffer(orders, taskTimestamp, timestampTo));
        transformed.put(CrawlerImpl.Table.REFILLS.getOutputPath(), createByteBuffer(refills, taskTimestamp, timestampTo));
        return transformed;
    }

    public static Map<Path, ByteBuffer> crawledStocksToOutput(CrawledStocks crawled, Timestamp taskTimestamp) {
        val transformed = new HashMap<Path, ByteBuffer>();
        transformed.put(CrawlerImpl.Table.SIZES.getOutputPath(), createByteBuffer(crawled.getSizes(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.WAREHOUSES.getOutputPath(), createByteBuffer(crawled.getWarehouses(), taskTimestamp, null));
        transformed.put(CrawlerImpl.Table.STOCKS.getOutputPath(), createByteBuffer(crawled.getStocks(), taskTimestamp, null));
        return transformed;
    }

    private static ByteBuffer createByteBuffer(String fileStr) {
        return ByteBuffer.wrap((fileStr + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuffer createByteBuffer(Collection<?> objs, Timestamp taskTimestamp, Timestamp timestampTo) {
        if (objs.isEmpty()) {
            return ByteBuffer.allocate(0);
        }
        return createByteBuffer(collectionToFileString(objs, taskTimestamp, timestampTo));
    }

    private static ByteBuffer createByteBuffer(Object obj, Timestamp taskTimestamp, Timestamp timestampTo) {
        return createByteBuffer(createLineString(obj, taskTimestamp, timestampTo));
    }

    private static String collectionToFileString(Collection<?> objs, Timestamp taskTimestamp, Timestamp timestampTo) {
        val sj = new StringJoiner(System.lineSeparator());
        for (val obj : objs) {
            sj.add(createLineString(obj, taskTimestamp, timestampTo));
        }
        return sj.toString();
    }

    private static String createLineString(Object obj, Timestamp taskTimestamp, Timestamp timestampTo) {
        val sj = new StringJoiner(CrawlerImpl.Constants.FIELD_SEPARATOR);
        join(sj, taskTimestamp);
        if (timestampTo != null) {
            join(sj, timestampTo);
        }
        if (obj instanceof Catalog catalog) {
            val key = catalog.getKey();
            join(sj, key.getShard());
            join(sj, key.getQuery());
        } else if (obj instanceof Source source) {
            val key = source.getKey();
            val catalogKey = source.getRelatedCatalogKey().get();
            join(sj, key.getText());
            join(sj, catalogKey.getShard());
            join(sj, catalogKey.getQuery());
        } else if (obj instanceof Brand brand) {
            val key = brand.getKey();
            join(sj, key.getWbId());
            join(sj, brand.getName());
            join(sj, brand.getImageUrl());
        } else if (obj instanceof Seller seller) {
            val key = seller.getKey();
            join(sj, key.getWbId());
            join(sj, seller.getName());
            join(sj, seller.getImageUrl());
            join(sj, seller.getInn() != null ? seller.getInn() : CrawlerImpl.Constants.NULL_FIELD);
            join(sj, seller.getOgrn() != null ? seller.getOgrn() : CrawlerImpl.Constants.NULL_FIELD);
            join(sj, seller.getOgrnip() != null ? seller.getOgrnip() : CrawlerImpl.Constants.NULL_FIELD);
            join(sj, seller.getAddress() != null ? seller.getAddress() : CrawlerImpl.Constants.NULL_FIELD);
        } else if (obj instanceof Product product) {
            val key = product.getKey();
            val brandKey = product.getRelatedBrandKey().get();
            val sellerKey = product.getRelatedSellerKey().get();
            join(sj, key.getSku());
            join(sj, product.getName());
            join(sj, product.getImageUrl());
            join(sj, brandKey.getWbId());
            join(sj, sellerKey != null ? sellerKey.getWbId() : CrawlerImpl.Constants.NULL_FIELD);
        } else if (obj instanceof ProductDetail productDetail) {
            val key = productDetail.getKey();
            val productKey = key.getProductKey();
            join(sj, productKey.getSku());
            join(sj, productDetail.getPrice());
            join(sj, productDetail.getSalePrice());
            join(sj, productDetail.getFeedbacks());
            join(sj, productDetail.getRating());
        } else if (obj instanceof Position position) {
            val key = position.getKey();
            val productKey = key.getProductKey();
            val catalogKey = key.getCatalogKey();
            join(sj, productKey.getSku());
            join(sj, catalogKey.getShard());
            join(sj, catalogKey.getQuery());
            join(sj, position.getPlace());
        } else if (obj instanceof Size size) {
            val key = size.getKey();
            val productKey = key.getProductKey();
            join(sj, productKey.getSku());
            join(sj, key.getName());
            join(sj, size.getAltName());
        } else if (obj instanceof Warehouse warehouse) {
            val key = warehouse.getKey();
            join(sj, key.getWbId());
            join(sj, warehouse.getName() != null ? warehouse.getName() : CrawlerImpl.Constants.NULL_FIELD);
        } else if (obj instanceof Merch merch) {
            val key = merch.getKey();
            val sizeKey = key.getSizeKey();
            val warehouseKey = key.getWarehouseKey();
            val productKey = sizeKey.getProductKey();
            join(sj, productKey.getSku());
            join(sj, sizeKey.getName());
            join(sj, warehouseKey.getWbId());
            join(sj, merch.getQuantity());
        }
        return sj.toString();
    }

    private static String normalizeString(String str) {
        return str.replace("\\", "\\\\")
                .replace("\n", "")
                .replace("\r", "")
                .replace(CrawlerImpl.Constants.FIELD_SEPARATOR, "");
    }

    private static void join(StringJoiner sj, Object obj) {
        sj.add(obj == CrawlerImpl.Constants.NULL_FIELD ? obj.toString() : normalizeString(obj.toString()));
    }

}
