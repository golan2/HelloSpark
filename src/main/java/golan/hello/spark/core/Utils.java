package golan.hello.spark.core;

import org.apache.commons.cli.*;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by golaniz on 17/02/2016.
 */
public class Utils {
    private static final SimpleDateFormat sdf        = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS", Locale.US);
    private static final GregorianCalendar calendar   = new GregorianCalendar(TimeZone.getTimeZone("US/Central"));

    public static void consolog(String s) {
        System.out.println(getCurrentDateAndTime()+"~~T["+Thread.currentThread().getName()+"] "  + s );
    }

    private static String getCurrentDateAndTime() {
        calendar.setTimeInMillis(System.currentTimeMillis());
        return sdf.format(calendar.getTime());
    }

    public static String findFileInClasspath(String filename) {
        URL url = ReadFiles.class.getClassLoader().getResource(filename);
        if (url==null) {
            throw new RuntimeException("Can't find file: " + filename);
        }
        return url.getPath();
    }
}
