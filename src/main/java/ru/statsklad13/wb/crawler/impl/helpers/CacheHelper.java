package ru.statsklad13.wb.crawler.impl.helpers;

import org.h2.mvstore.MVStore;

import java.util.Map;
import java.util.Optional;

public class CacheHelper {

    private static MVStore cache;
    private static Map<String, Object> temporaryMap;
    private static Map<String, Object> permanentMap;

    public static void init() {
        cache = new MVStore.Builder()
                .fileName("cache.mv.db")
                .autoCommitDisabled()
                .open();
        temporaryMap = cache.openMap("temporary");
        permanentMap = cache.openMap("permanent");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            cache.closeImmediately();
        }));
    }

    public static <T> Optional<T> getTemporary(String key) {
        return Optional.ofNullable((T) temporaryMap.get(key));
    }

    public static <T> Optional<T> getPermanent(String key) {
        return Optional.ofNullable((T) permanentMap.get(key));
    }

    public static boolean hasTemporary(String key) {
        return getTemporary(key).isPresent();
    }

    public static boolean hasPermanent(String key) {
        return getPermanent(key).isPresent();
    }

    public static void writeTemporary(String key, Object value) {
        temporaryMap.put(key, value);
        cache.commit();
    }

    public static void writePermanent(String key, Object value) {
        permanentMap.put(key, value);
        cache.commit();
    }

    public static void writeTemporary(String key) {
        writeTemporary(key, true);
    }

    public static void writePermanent(String key) {
        writePermanent(key, true);
    }

    public static void removeTemporary(String key) {
        temporaryMap.remove(key);
        cache.commit();
    }

    public static void removePermanent(String key) {
        permanentMap.remove(key);
        cache.commit();
    }

    public static void clearTemporary() {
        temporaryMap.clear();
        cache.commit();
    }

    public static void clearPermanent() {
        permanentMap.clear();
        cache.commit();
    }

}
