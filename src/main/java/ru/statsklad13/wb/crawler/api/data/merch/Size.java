package ru.statsklad13.wb.crawler.api.data.merch;

import lombok.EqualsAndHashCode;
import lombok.Value;
import ru.statsklad13.wb.crawler.api.data.key.merch.SizeKey;

@Value
public class Size {

    SizeKey key;
    @EqualsAndHashCode.Exclude String altName;

}
