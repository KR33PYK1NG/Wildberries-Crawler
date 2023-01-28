package ru.statsklad13.wb.crawler.api.data.merch;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.merch.WarehouseKey;

@Value
public class Warehouse {

    WarehouseKey key;
    @EqualsAndHashCode.Exclude String name;

}
