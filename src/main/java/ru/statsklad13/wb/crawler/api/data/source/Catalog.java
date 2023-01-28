package ru.statsklad13.wb.crawler.api.data.source;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.source.CatalogKey;

import java.util.concurrent.atomic.AtomicInteger;

@Value
public class Catalog {

    CatalogKey key;
    @EqualsAndHashCode.Exclude AtomicInteger emptyPage = new AtomicInteger(Integer.MAX_VALUE);

}
