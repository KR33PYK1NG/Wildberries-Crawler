package ru.statsklad13.wb.crawler.api.data.product;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.product.SellerKey;

@Value
public class Seller {

    SellerKey key;
    @EqualsAndHashCode.Exclude String name;
    @EqualsAndHashCode.Exclude String imageUrl;
    @EqualsAndHashCode.Exclude String inn;
    @EqualsAndHashCode.Exclude String ogrn;
    @EqualsAndHashCode.Exclude String ogrnip;
    @EqualsAndHashCode.Exclude String address;

}
