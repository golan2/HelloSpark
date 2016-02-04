package golan.hello.spark;

import java.io.Serializable;

/**
 * Created by golaniz on 20/01/2016.
 */
class StocksVal implements Serializable {
  public static final String N_A = "N/A";
  String id;
  String date = N_A;
  double val1;
  double val2;
  double val3;
  double val4;
  double val5;

  public StocksVal(String s) {
    try {
      if (s == null) return;
      String[] cols = s.split(",");
      id = cols[0];
      date = cols[1];
      val1 = parseDouble(cols[2]);
      val2 = parseDouble(cols[3]);
      val3 = parseDouble(cols[4]);
      val4 = parseDouble(cols[5]);
      val5 = parseDouble(cols[6]);
    } catch (IndexOutOfBoundsException ignore) {
    }
  }

  private double parseDouble(String s) {
    try {
      if (s == null || s.length() == 0) return Double.NaN;
      return Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return Double.NaN;
    }
  }


  @Override
  public String toString() {
    System.out.println("["+id+"]");
    return "StocksVal{" +
            "id='" + id + '\'' +
            ", date='" + date + '\'' +
            ", val1=" + val1 +
            ", val2=" + val2 +
            ", val3=" + val3 +
            ", val4=" + val4 +
            ", val5=" + val5 +
            '}';
  }

  public int getDay() {
    if (StocksVal.N_A.equals(date)) return -1;
    String[] cols = date.split("/");
    if (cols.length<1) return -1;
    String day = cols[0].trim();
    try {
      return Integer.parseInt(day);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  public int getMonth() {
    if (StocksVal.N_A.equals(date)) return -1;
    String[] cols = date.split("/");
    if (cols.length<1) return -1;
    String day = cols[1].trim();
    try {
      return Integer.parseInt(day);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  public static boolean isFirstDayOfMonth(String stockAsString) {
    return new StocksVal(stockAsString).getDay()==1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    StocksVal rhs = (StocksVal) o;
    return this.getMonth()==rhs.getMonth();

  }

  @Override
  public int hashCode() {
    return getDay();
  }
}
