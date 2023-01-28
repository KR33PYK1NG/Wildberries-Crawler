package ru.statsklad13.wb.crawler.api.data.product;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.product.BrandKey;
import ru.statsklad13.wb.crawler.api.data.key.product.ProductKey;
import ru.statsklad13.wb.crawler.api.data.key.product.SellerKey;
import ru.statsklad13.wb.crawler.api.data.misc.Relation;

@Value
public class Product {

    ProductKey key;
    @EqualsAndHashCode.Exclude String name;
    @EqualsAndHashCode.Exclude String imageUrl;
    @EqualsAndHashCode.Exclude Relation<BrandKey> relatedBrandKey = new Relation<>();
    @EqualsAndHashCode.Exclude Relation<SellerKey> relatedSellerKey = new Relation<>();

}
