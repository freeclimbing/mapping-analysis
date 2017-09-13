package org.mappinganalysis.io.impl.csv;

import com.google.common.math.DoubleMath;
import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.math.RoundingMode;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Musicbrainz vertex formatter.
 */
public class MusicCSVToVertexFormatter
    extends RichMapFunction<Tuple10<Long, Long, Long, String, String, String, String, String, String, String>,
        Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MusicCSVToVertexFormatter.class);
  /**
   * no reuse vertex, not every attribute gets new value
   */
  private Vertex<Long, ObjectMap> vertex = new Vertex<>();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public Vertex<Long, ObjectMap> map(
      Tuple10<Long,  //0 TID --> vertex id
                Long,  //1 CID --> cluster id
      //          Long,  // CTID --> cluster intern id
                Long,  //2 SourceID, 1 to 5
      //          String,  //IGNORE - strange mix numbers and letters
                String,  //3 number - song number? sometimes letters involved
                String,  //4 title
                String,  //5 length, e.g., 4m 32sec, 432, 432000, 4.58
                String,  //6 artist
                String,  //7 album
                String,  //8 year, e.g., 2009, '09
                String> value)  //9 language
      throws Exception {
        vertex.setValue(new ObjectMap(Constants.MUSIC));
        vertex.setId(value.f0);
        ObjectMap properties = vertex.getValue();

        properties.setCcId(value.f1);
        properties.setLabel(value.f4);

        properties.setDataSource(value.f2.toString()); // int would be better, but data source is string
        properties.put(Constants.NUMBER, value.f3);
//        if (value.f0 == 4L) {
//          LOG.info("test: " + value.toString());
//        }
//        if (value.f0 == 4L)
//          LOG.info("N: " + vertex.toString());
        fixSongLength(value.f5);
//        if (value.f0 == 4L)
//          LOG.info("L: " + vertex.toString());
//        properties.put(Constants.LENGTH, fixSongLength(value.f5));
//        properties.put("oLength", value.f5);
        properties.put(Constants.ARTIST, value.f6);
        properties.put(Constants.ALBUM, value.f7);
        fixYear(value.f8);
//         properties.put("oYear", value.f8);
        properties.put(Constants.LANGUAGE, fixLanguage(value.f9));
//         properties.put("orig", value.f9);  // tmp for test

//        if (value.f0 == 4L)
//        LOG.info(vertex.toString());

    return vertex;
  }

  private void fixYear(String year) {
    if (year == null || year.isEmpty()) {
      return;
    }
    year = year.replaceAll("\\s+", "");
    year = year.replaceAll("[oO]", "0");

    Matcher moreThanFour = Pattern.compile(".*(\\d{5,20}).*").matcher(year);
    if (moreThanFour.find()) {
      return;
    }
    Matcher fourDigitsMatcher = Pattern.compile(".*(\\d{4}).*").matcher(year);

    /**
     * '11, '05
     */
    if (year.matches("^'\\d+")) {
      year = year.replace("'", "");
//      System.out.println("start with ': " + year);
      int tmp = Ints.tryParse(year);
      if (tmp < 20) {//&& year.startsWith("0")) {
        vertex.getValue().put(Constants.YEAR, tmp + 2000);
//        return tmp + 2000;
      } else if (tmp <= 99) {
        vertex.getValue().put(Constants.YEAR, tmp + 1900);
//        return tmp + 1900;
      }
    } else
    /**
     * 04, 11, 2009, 1911
     */
    if (year.matches("[0-9]+")) {
      int tmp = Ints.tryParse(year);
      if (tmp < 20) {
        vertex.getValue().put(Constants.YEAR, tmp + 2000);
      } else if (tmp <= 99) {
        vertex.getValue().put(Constants.YEAR, tmp + 1900);
//        return tmp + 1900;
      } else if (tmp > 2017) {
        return; // needed atm
      } else {
        vertex.getValue().put(Constants.YEAR, tmp);
//        return tmp;
      }
    } else
    /**
     * Any number with 4 digits within "long" string, e.g.,
     * "Spider in the Snow - Live in Japan 2011"
     */
    if (year.length() > 9 && fourDigitsMatcher.find()) {
//      System.out.println("4d: " + fourDigitsMatcher.group(1));
      vertex.getValue().put(Constants.YEAR, Ints.tryParse(fourDigitsMatcher.group(1)));

//      return Ints.tryParse(fourDigitsMatcher.group(1));
    }
  }

  private void fixSongLength(String songLength) throws Exception {
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
      return;
    }
    if (songLength.length() > 11 || songLength.matches("[a-zA-Z]+\\d{4}\\d+")) {
      return;
    }
    if (songLength.contains("-") || songLength.equals(Constants.CSV_NO_VALUE)) {
      return;
    }


    if ( // special cases from 20k
        !songLength.equals("28q666") // 4969 soll 289 sein
//        || songLength.equals("3.g83") // 9901 ~239 - whats with o instead of 0 - 13873
        && !songLength.equals("3318-a033") // 10112
//        || songLength.equals("7.o67")
//        || songLength.equals("4.g1")
//        || songLength.equals("03:1g")
        ) {
      if (songLength.matches(".*\\d+.*")) {
        songLength = songLength
            .replaceAll("[oO]", "0")
            .replaceAll("l", "1")
            .replaceAll("z", "2");
        /**
         * min + sec format
         */
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
            vertex.getValue().put(Constants.LENGTH, time);
          } else {
            return;
          }
        }

        songLength = songLength.replaceAll("[,nyur_b]","");

        /**
         * 2.1, 3.08 format
         */
        if (songLength.contains(".") && songLength.matches("[0-9]+\\.[0-9]+")) {
          vertex.getValue().put(Constants.LENGTH, DoubleMath.roundToInt(
              Double.valueOf(songLength) * 60, RoundingMode.HALF_UP));
        } else
        /**
         * 2:30 format
         */
        if (songLength.contains(":") && songLength.matches("[0-9]+:[0-9]+")) {
          String[] split = songLength.split(":");
//          if (songLength.contains("r0ud12")) {
//            System.out.println("backup: " + backup);
//          }
          if (split.length < 2) {
            return;
          }

          int splitZeroLength = split[0].length();
          if (splitZeroLength > 2) {
            split[0] = split[0].substring(splitZeroLength - 2, splitZeroLength -1);
          }
          if (split[1].length() > 2) {
            split[1] = split[1].substring(0, 1);
          }
          if (split[0].equals("")) {
            return ;
          }
//          if (split[0].equals("") || split[1].equals("")) {
//            System.out.println("backup: " + backup);
//          }
          vertex
              .getValue()
              .put(Constants.LENGTH, Integer.valueOf(split[0]) * 60 + Integer.valueOf(split[1]));
        } else
        /**
         * 3, 456000, 456, 456789
         */
        if (songLength.matches("[0-9]+")) {
          if (songLength.endsWith("000") || Integer.valueOf(songLength) > 10000) {
            vertex
                .getValue()
                .put(Constants.LENGTH,
                    IntMath.divide(Integer.valueOf(songLength), 1000, RoundingMode.HALF_UP));
          } else {
//          System.out.println("n: " + songLength);
            vertex
                .getValue()
                .put(Constants.LENGTH, Integer.valueOf(songLength));
          }
        }
      }
    }
  }

  private String fixLanguage(String lang) {
    lang = lang.toLowerCase();
    if (lang.contains(",")) {
      return Constants.MU;
    }
    if (lang.startsWith("en")) {
      return Constants.EN;
    }
    if (lang.startsWith("ge")) {
      return Constants.GE;
    }
    if (lang.startsWith("sp")) {
      return Constants.SP;
    }
    if (lang.startsWith("fr")) {
      return Constants.FR;
    }
    if (lang.startsWith("it")) {
      return Constants.IT;
    }
    if (lang.startsWith("la")) {
      return Constants.LA;
    }
    if (lang.startsWith("hu")) {
      return Constants.HU;
    }
    if (lang.startsWith("po")) { // por
      if (lang.startsWith("por")) {
        return Constants.POR;
      } else {
        return Constants.PO;
      }
    }
    if (lang.startsWith("un")) {
      return Constants.UN;
    }
    if (lang.startsWith("[m")) {
      return Constants.MU;
    }
    if (lang.startsWith("ch")) {
      return Constants.CH;
    }
    if (lang.startsWith("ca")) {
      return Constants.CA;
    }
    if (lang.startsWith("gr")) {
      return Constants.GR;
    }
    if (lang.startsWith("es")) {
      return Constants.ES;
    }
    if (lang.startsWith("no")) {
      return Constants.NO;
    }
    if (lang.startsWith("sw")) {
      return Constants.SW;
    }
    if (lang.startsWith("fi")) {
      return Constants.FI;
    }
    if (lang.startsWith("ja")) {
      return Constants.JA;
    }
    if (lang.startsWith("du")) {
      return Constants.DU;
    }
    if (lang.startsWith("da")) {
      return Constants.DA;
    }
    if (lang.startsWith("ru")) {
      return Constants.RU;
    }
    if (lang.startsWith("tu")) {
      return Constants.TU;
    }

//    LOG.info(lang);
    return Constants.NO_OR_MINOR_LANG;
  }
}