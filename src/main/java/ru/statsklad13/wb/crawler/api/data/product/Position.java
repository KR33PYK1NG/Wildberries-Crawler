package ru.statsklad13.wb.crawler.api.data.product;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.product.PositionKey;

@Value
public class Position {

    PositionKey key;
    @EqualsAndHashCode.Exclude int place;

}
