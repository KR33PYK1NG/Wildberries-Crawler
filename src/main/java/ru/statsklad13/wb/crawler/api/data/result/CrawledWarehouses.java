package ru.statsklad13.wb.crawler.api.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.merch.Warehouse;

import java.util.Set;

@Value
public class CrawledWarehouses {

    Set<Warehouse> warehouses;

}
