package ru.statsklad13.wb.crawler.api.data.key.merch;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.product.ProductKey;

@Value
public class SizeKey {

    ProductKey productKey;
    String name;

}
