package ru.statsklad13.wb.crawler.api.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.merch.Size;
import ru.statsklad13.wb.crawler.api.data.merch.Stock;
import ru.statsklad13.wb.crawler.api.data.merch.Warehouse;

import java.util.Set;

@Value
public class CrawledStocks {

    Set<Size> sizes;
    Set<Warehouse> warehouses;
    Set<Stock> stocks;

}
