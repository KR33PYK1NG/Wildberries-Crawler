package ru.statsklad13.wb.crawler.api.data.result;

import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.source.Category;

import java.util.Set;

@Value
public class CrawledCategories {

    Set<Category> categories;

}
