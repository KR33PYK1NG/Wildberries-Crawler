package ru.statsklad13.wb.crawler.api.data.product;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.product.ProductDetailKey;

@Value
public class ProductDetail {

    ProductDetailKey key;
    @EqualsAndHashCode.Exclude int price;
    @EqualsAndHashCode.Exclude int salePrice;
    @EqualsAndHashCode.Exclude int feedbacks;
    @EqualsAndHashCode.Exclude int rating;

}
