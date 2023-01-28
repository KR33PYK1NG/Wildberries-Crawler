package ru.statsklad13.wb.crawler.api.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.source.Query;

import java.util.Set;

@Value
public class CrawledQueries {

    Set<Query> queries;

}
