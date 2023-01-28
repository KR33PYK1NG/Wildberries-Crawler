package ru.statsklad13.wb.crawler.api.data.product;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.product.BrandKey;

@Value
public class Brand {

    BrandKey key;
    @EqualsAndHashCode.Exclude String name;
    @EqualsAndHashCode.Exclude String imageUrl;

}
