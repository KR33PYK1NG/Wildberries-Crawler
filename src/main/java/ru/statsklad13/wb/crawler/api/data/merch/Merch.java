package ru.statsklad13.wb.crawler.api.data.merch;

import ru.statsklad13.wb.crawler.api.data.key.merch.MerchKey;

public interface Merch {

    MerchKey getKey();

    int getQuantity();

}
