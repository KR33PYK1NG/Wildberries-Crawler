package ru.statsklad13.wb.crawler.api.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.source.Catalog;

@Value
public class CrawledCatalog {

    Catalog catalog;

}
