package ru.statsklad13.wb.crawler.impl.data.merch;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.merch.MerchKey;
import ru.statsklad13.wb.crawler.api.data.merch.Merch;

@Value
public class Order implements Merch {

    MerchKey key;
    int quantity;

}
