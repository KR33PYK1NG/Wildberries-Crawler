package ru.statsklad13.wb.crawler.api.data.source;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.source.CatalogKey;
import ru.statsklad13.wb.crawler.api.data.key.source.SourceKey;
import ru.statsklad13.wb.crawler.api.data.misc.Relation;

@Value
public class Category implements Source {

    SourceKey key;
    @EqualsAndHashCode.Exclude String url;
    @EqualsAndHashCode.Exclude String shard;
    @EqualsAndHashCode.Exclude String query;
    @EqualsAndHashCode.Exclude Relation<CatalogKey> relatedCatalogKey = new Relation<>();

}
