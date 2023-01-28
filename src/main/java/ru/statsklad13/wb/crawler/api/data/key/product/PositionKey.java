package ru.statsklad13.wb.crawler.api.data.key.product;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.source.CatalogKey;

@Value
public class PositionKey {

    ProductKey productKey;
    CatalogKey catalogKey;

}
