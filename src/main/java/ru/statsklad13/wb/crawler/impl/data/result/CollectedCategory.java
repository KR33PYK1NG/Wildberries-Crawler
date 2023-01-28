package ru.statsklad13.wb.crawler.impl.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.source.Catalog;
import ru.statsklad13.wb.crawler.api.data.source.Source;

import java.util.Set;

@Value
public class CollectedCategory {

    Set<Catalog> catalogs;
    Set<Source> sources;

}
