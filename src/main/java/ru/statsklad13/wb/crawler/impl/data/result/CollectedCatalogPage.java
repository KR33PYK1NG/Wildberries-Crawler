package ru.statsklad13.wb.crawler.impl.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.merch.Size;
import ru.statsklad13.wb.crawler.api.data.merch.Stock;
import ru.statsklad13.wb.crawler.api.data.merch.Warehouse;
import ru.statsklad13.wb.crawler.api.data.product.*;

import java.util.Set;

@Value
public class CollectedCatalogPage {

    Set<Size> sizes;
    Set<Warehouse> warehouses;
    Set<Stock> stocks;
    Set<Seller> sellers;
    Set<Brand> brands;
    Set<Product> products;
    Set<ProductDetail> productDetails;
    Set<Position> positions;

}
