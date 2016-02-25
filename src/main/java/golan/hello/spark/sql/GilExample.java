package golan.hello.spark.sql;

import golan.hello.spark.core.AbsSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by golaniz on 14/02/2016.
 */
public class GilExample extends AbsSpark {

    public static void main(String[] args) throws IOException, URISyntaxException {
        JavaSparkContext sc = getJavaSparkContext(GilExample.class.getSimpleName());
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        JavaRDD<String> wikiStats = sc.textFile("C:\\Users\\golaniz\\Desktop\\Misc\\rdd\\locations_sorted_with_id.csv.utxt");
        JavaRDD<String> filtered = wikiStats.filter(row -> !row.startsWith("#"));
        JavaRDD<String[]> rows = filtered.map(record -> (record).split(","));
        JavaRDD<GeoLocation> records = rows.map(GeoLocation::fromStrings);
        DataFrame locations = sqlContext.createDataFrame(records, GeoLocation.class); //WikiPage.class);
        locations.registerTempTable("locations");
        locations.cache();
        DataFrame result = sqlContext.sql("SELECT * FROM locations WHERE country like 'A%'");
        System.out.println(result.count());
    }


    public static class GeoLocation {
        int id;
        String country;
        String state;
        String city;
        float latitude;
        float longitude;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public float getLatitude() {
            return latitude;
        }

        public void setLatitude(float latitude) {
            this.latitude = latitude;
        }

        public float getLongitude() {
            return longitude;
        }

        public void setLongitude(float longitude) {
            this.longitude = longitude;
        }

        public static GeoLocation fromStrings(String[] a) {
            GeoLocation result = new GeoLocation();
            result.id = Integer.parseInt(a[0]);
            result.country = a[1];
            result.state = a[2];
            result.city = a[3];
            result.latitude = Float.parseFloat(a[4]);
            result.longitude = Float.parseFloat(a[5]);
            return result;
        }
    }
}
