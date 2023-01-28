package ru.statsklad13.wb.crawler.api.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.product.Brand;
import ru.statsklad13.wb.crawler.api.data.product.Position;
import ru.statsklad13.wb.crawler.api.data.product.Product;
import ru.statsklad13.wb.crawler.api.data.product.ProductDetail;

import java.util.Set;

@Value
public class CrawledCatalogPage {

    Set<Brand> brands;
    Set<Product> products;
    Set<ProductDetail> productDetails;
    Set<Position> positions;

}
