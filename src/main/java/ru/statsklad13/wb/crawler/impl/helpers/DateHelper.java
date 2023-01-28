package ru.statsklad13.wb.crawler.impl.helpers;

import lombok.val;

import java.util.Calendar;

public class DateHelper {

    public static Calendar currentCalendar() {
        return Calendar.getInstance();
    }

    public static Calendar dayStartCalendar(int dayShift) {
        return dayStartCalendar(currentCalendar().getTimeInMillis(), dayShift);
    }

    public static Calendar dayStartCalendar(long pointMs, int dayShift) {
        val cal = Calendar.getInstance();
        cal.setTimeInMillis(pointMs);
        cal.add(Calendar.DAY_OF_MONTH, dayShift);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }

    public static Calendar pointStartCalendar() {
        val cal = currentCalendar();
        val currentHour = cal.get(Calendar.HOUR_OF_DAY);
        var targetHour = 0;
        if (currentHour >= 21) {
            targetHour = 21;
        } else if (currentHour >= 18) {
            targetHour = 18;
        } else if (currentHour >= 15) {
            targetHour = 15;
        } else if (currentHour >= 12) {
            targetHour = 12;
        } else if (currentHour >= 9) {
            targetHour = 9;
        } else if (currentHour >= 6) {
            targetHour = 6;
        } else if (currentHour >= 3) {
            targetHour = 3;
        }
        val out = dayStartCalendar(0);
        out.set(Calendar.HOUR_OF_DAY, targetHour);
        return out;
    }

    public static Calendar nextPointCalendar() {
        val cal = currentCalendar();
        val currentHour = cal.get(Calendar.HOUR_OF_DAY);
        var targetHour = 0;
        if (currentHour < 3) {
            targetHour = 3;
        } else if (currentHour < 6) {
            targetHour = 6;
        } else if (currentHour < 9) {
            targetHour = 9;
        } else if (currentHour < 12) {
            targetHour = 12;
        } else if (currentHour < 15) {
            targetHour = 15;
        } else if (currentHour < 18) {
            targetHour = 18;
        } else if (currentHour < 21) {
            targetHour = 21;
        }
        val out = dayStartCalendar(targetHour == 0 ? 1 : 0);
        out.set(Calendar.HOUR_OF_DAY, targetHour);
        return out;
    }

}
