package ru.statsklad13.wb.crawler.api.data.source;

import ru.statsklad13.wb.crawler.api.data.key.source.CatalogKey;
import ru.statsklad13.wb.crawler.api.data.key.source.SourceKey;
import ru.statsklad13.wb.crawler.api.data.misc.Relation;

public interface Source {

    SourceKey getKey();

    Relation<CatalogKey> getRelatedCatalogKey();

}
