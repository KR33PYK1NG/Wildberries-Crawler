package ru.statsklad13.wb.crawler.api.data.misc;

import lombok.Value;

@Value
public class WebResponse {

    String body;
    int code;

}
