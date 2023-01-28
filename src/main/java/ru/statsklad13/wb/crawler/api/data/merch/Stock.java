package ru.statsklad13.wb.crawler.api.data.merch;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.merch.MerchKey;

@Value
public class Stock implements Merch {

    MerchKey key;
    @EqualsAndHashCode.Exclude int quantity;

}
