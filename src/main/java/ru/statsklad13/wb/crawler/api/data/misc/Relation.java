package ru.statsklad13.wb.crawler.api.data.misc;

public class Relation<T> {

    private T related;

    public T get() {
        return this.related;
    }

    public void set(T related) {
        this.related = related;
    }

}
