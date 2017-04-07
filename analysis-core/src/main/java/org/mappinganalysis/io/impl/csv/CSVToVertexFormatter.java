package org.mappinganalysis.io.impl.csv;

import com.google.common.math.DoubleMath;
import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Musicbrainz vertex formatter.
 */
public class CSVToVertexFormatter
    extends RichMapFunction<Tuple10<Long, Long, Long, String, String, String, String, String, String, String>,
        Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(CSVToVertexFormatter.class);

  public static final String GE = "german";
  public static final String EN = "english";
  public static final String SP = "spanish";
  public static final String IT = "italian";
  public static final String FR = "french";
  public static final String LA = "latin";
  public static final String HU = "hungarian";
  public static final String PO = "polish";
  public static final String UN = "unknown";
  public static final String MU = "multiple";
  public static final String CH = "chinese";
  public static final String CA = "catalan";
  public static final String GR = "greek";
  public static final String NO = "norwegian";
  public static final String ES = "esperanto";
  public static final String POR = "portuguese";
  public static final String FI = "finnish";
  public static final String JA = "japanese";
  public static final String SW = "swedish";
  public static final String DU = "dutch";
  public static final String RU = "russian";
  public static final String TU = "turkish";
  public static final String DA = "danish";

  private final Vertex<Long, ObjectMap> reuseVertex;

  public CSVToVertexFormatter() {
    reuseVertex = new Vertex<>();
    reuseVertex.setValue(new ObjectMap());
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public Vertex<Long, ObjectMap> map(
      Tuple10<Long,  //0 TID --> vertex id
                Long,  //1 CID --> cluster id
      //          Long,  //2 CTID --> cluster intern id
                Long,  //2 SourceID, 1 to 5
      //          String,  //4xx id - IGNORE - strange mix numbers and letters
                String,  //3 number - song number? sometimes letters involved
                String,  //4 title
                String,  //5 length, e.g., 4m 32sec, 432, 432000, 4.58
                String,  //6 artist
                String,  //7 album
                String,  //8 year, e.g., 2009, '09
                String> value)  //9 language
      throws Exception {
        reuseVertex.setId(value.f0);
//        System.out.println(value.toString());
        ObjectMap properties = reuseVertex.getValue();
        properties.setCcId(value.f2);
        properties.setLabel(value.f4);

        properties.put("source", value.f2);
        properties.put("number", value.f3);

        properties.put("length", fixSongLength(value.f0, value.f5));
        // System.out.println("songlength: " + fixSongLength(value.f0, value.f4));
        properties.put("artist", value.f6);
        properties.put("album", value.f7);
        properties.put("year", fixYear(value.f8));
        // properties.put("oYear", value.f10);
        properties.put("lang", fixLanguage(value.f9));
        // properties.put("orig", value.f11);  // tmp for test
    return reuseVertex;
  }

  private Integer fixYear(String year) {
    if (year == null || year.isEmpty()) {
      return null;
    }
    year = year.replaceAll("\\s+", "");
    year = year.replaceAll("[oO]", "0");

    Matcher moreThanFour = Pattern.compile(".*(\\d{5,20}).*").matcher(year);
    if (moreThanFour.find()) {
      return null;
    }
    Matcher fourDigitsMatcher = Pattern.compile(".*(\\d{4}).*").matcher(year);

//    if (year.startsWith("'")) {
    if (year.matches("^'\\d+")) {
      year = year.replace("'", "");
//      System.out.println("start with ': " + year);
      int tmp = Ints.tryParse(year);
      if (tmp < 20 && year.startsWith("0")) {
        return tmp + 2000;
      } else if (tmp <= 99) {
        return tmp + 1900;
      }
    } else if (year.matches("[0-9]+")) {
      int tmp = Ints.tryParse(year);
      if (tmp < 20) {
        return tmp + 2000;
      } else if (tmp <= 99) {
        return tmp + 1900;
      } else if (tmp > 2017) {
        return null;
      } else {
        return tmp;
      }
    } else if (year.length() > 9 && fourDigitsMatcher.find()) {
//      System.out.println("4d: " + fourDigitsMatcher.group(1));
//      System.out.println("... in year: " + year);
      return Ints.tryParse(fourDigitsMatcher.group(1));
    }
    return null;
  }

  private Integer fixSongLength(Long f0, String songLength) throws Exception {
//    System.out.println(f0 + " pre: " + songLength);
    String backup = songLength;
    songLength = songLength.toLowerCase().replaceAll("\\s+", "");

    if (songLength.isEmpty()
        || (songLength.contains(".") && songLength.contains(":"))
        || songLength.contains("g")
        || songLength.contains("q")
        || songLength.contains("&") // "3.&67"
        || songLength.contains("|")
        || songLength.contains("p") // "0p.6"
        ) {
      return null;
    }
    if (songLength.length() > 11 || songLength.matches("[a-zA-Z]+\\d{4}\\d+")) {
      return null;
    }
    if (songLength.contains("-") || songLength.equals("--")) {
//      LOG.info("Contains '-': " + songLength);
      return null;
    }


    if ( // special cases from 20k
        songLength.equals("28q666") // 4969 soll 289 sein
//        || songLength.equals("3.g83") // 9901 ~239 - whats with o instead of 0 - 13873
        || songLength.equals("3318-a033") // 10112
//        || songLength.equals("7.o67")
//        || songLength.equals("4.g1")
//        || songLength.equals("03:1g")
        ) {
//      System.out.println(f0 + "###### return null for " + songLength);
      return null;
    } else {
      if (songLength.matches(".*\\d+.*")) {
        songLength = songLength
            .replaceAll("[oO]", "0")
            .replaceAll("l", "1")
            .replaceAll("z", "2");
        if ((songLength.contains("m") || songLength.contains("s"))
            && songLength.matches("^[0-9].*")) {
//          System.out.println(f0 + "m or s " + songLength);
          if (songLength.contains("m") && songLength.contains("sec")) {
            songLength = songLength.replaceAll("[^0-9msec]", "");
          }
          if (songLength.contains("sec") && songLength.contains("n")) {
            songLength = songLength.replaceAll("n", "m");
          }
          if (songLength.matches("\\d+m\\d+sec")) {
            int time = 0;
            if (songLength.contains("m")) {
              String[] ms = songLength.split("m");
              time = Integer.valueOf(ms[0]) * 60;
              songLength = ms[1];
            }
            if (songLength.contains("s")) {
//            System.out.println("s split: " + songLength.split("s")[0]);
              time += Integer.valueOf(songLength.split("s")[0]);
            }
//          LOG.info(f0 + "end: " + time);
            return time;
          } else {
            return null;
          }
        }

        songLength = songLength.replaceAll("[,nyur_b]","");

        if (songLength.contains(".") && songLength.matches("[0-9]+\\.[0-9]+")) {
          return DoubleMath.roundToInt(
              Double.valueOf(songLength) * 60, RoundingMode.HALF_UP);
        } else if (songLength.contains(":") && songLength.matches("[0-9]+:[0-9]+")) {
          String[] split = songLength.split(":");
          if (songLength.contains("r0ud12")) {
            System.out.println("backup: " + backup);
          }
          if (split.length < 2) {
            return null;
          }
          int splitZeroLength = split[0].length();
          if (splitZeroLength > 2) {
            split[0] = split[0].substring(splitZeroLength - 2, splitZeroLength -1);
          }
          if (split[1].length() > 2) {
            split[1] = split[1].substring(0, 1);
          }
          if (split[0].equals("")) {
            return null;
          }

          if (split[0].equals("") || split[1].equals("")) {
            System.out.println("backup: " + backup);
          }

          return Integer.valueOf(split[0]) * 60 + Integer.valueOf(split[1]);
        } else if (songLength.matches("[0-9]+")) {
          if (songLength.endsWith("000") || Integer.valueOf(songLength) > 10000) {
            return IntMath.divide(Integer.valueOf(songLength), 1000, RoundingMode.HALF_UP);
          }
//          System.out.println("n: " + songLength);
          return Integer.valueOf(songLength);
        }
      }
//      LOG.info("will return NULL for value: " + songLength);
      return null; // only letters are not handled
    }
  }

  private String fixLanguage(String lang) {
    lang = lang.toLowerCase();
    if (lang.contains(",")) {
//      LOG.info("contains ,: " + lang);
      return MU;
    }
    if (lang.startsWith("en")) {
      return EN;
    }
    if (lang.startsWith("ge")) {
      return GE;
    }
    if (lang.startsWith("sp")) {
      return SP;
    }
    if (lang.startsWith("fr")) {
      return FR;
    }
    if (lang.startsWith("it")) {
      return IT;
    }
    if (lang.startsWith("la")) {
      return LA;
    }
    if (lang.startsWith("hu")) {
      return HU;
    }
    if (lang.startsWith("po")) { // por
      if (lang.startsWith("por")) {
        return POR;
      } else {
        return PO;
      }
    }
    if (lang.startsWith("un")) {
      return UN;
    }
    if (lang.startsWith("[m")) {
      return MU;
    }
    if (lang.startsWith("ch")) {
      return CH;
    }
    if (lang.startsWith("ca")) {
      return CA;
    }
    if (lang.startsWith("gr")) {
      return GR;
    }
    if (lang.startsWith("es")) {
      return ES;
    }
    if (lang.startsWith("no")) {
      return NO;
    }
    if (lang.startsWith("sw")) {
      return SW;
    }
    if (lang.startsWith("fi")) {
      return FI;
    }
    if (lang.startsWith("ja")) {
      return JA;
    }
    if (lang.startsWith("du")) {
      return DU;
    }
    if (lang.startsWith("da")) {
      return DA;
    }
    if (lang.startsWith("ru")) {
      return RU;
    }
    if (lang.startsWith("tu")) {
      return TU;
    }

//    LOG.info(lang);
    return null;
  }
}