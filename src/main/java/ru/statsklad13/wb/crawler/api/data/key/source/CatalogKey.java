package ru.statsklad13.wb.crawler.api.data.key.source;

import lombok.Value;

@Value
public class CatalogKey {

    String shard;
    String query;

}
