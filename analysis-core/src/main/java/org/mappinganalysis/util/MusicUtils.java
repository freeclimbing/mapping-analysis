package org.mappinganalysis.util;

import com.google.common.math.DoubleMath;
import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;

import java.math.RoundingMode;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MusicUtils {

  public static Integer fixSongLength(String songLength) throws Exception {
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
    if (songLength.contains("-") || songLength.equals(Constants.CSV_NO_VALUE)) {
      return null;
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
        /*
          min + sec format
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
            return time;
          } else {
            return null;
          }
        }

        songLength = songLength.replaceAll("[,nyur_b]","");

        /*
          2.1, 3.08 format
         */
        if (songLength.contains(".") && songLength.matches("[0-9]+\\.[0-9]+")) {
          return DoubleMath.roundToInt(
              Double.valueOf(songLength) * 60, RoundingMode.HALF_UP);
        } else
        /*
          2:30 format
         */
        if (songLength.contains(":") && songLength.matches("[0-9]+:[0-9]+")) {
          String[] split = songLength.split(":");
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
//          if (split[0].equals("") || split[1].equals("")) {
//            System.out.println("backup: " + backup);
//          }
          return Integer.valueOf(split[0]) * 60 + Integer.valueOf(split[1]);
        } else
        /*
          3, 456000, 456, 456789
         */
        if (songLength.matches("[0-9]+")) {
          if (songLength.endsWith("000") || Integer.valueOf(songLength) > 10000) {
            return IntMath.divide(Integer.valueOf(songLength), 1000, RoundingMode.HALF_UP);
          } else {
            return Integer.valueOf(songLength);
          }
        }
      }
    }
    return null;
  }

  public static Integer fixYear(String year) {
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

    /*
      '11, '05
     */
    if (year.matches("^'\\d+")) {
      year = year.replace("'", "");
//      System.out.println("start with ': " + year);
      int tmp = Ints.tryParse(year);
      if (tmp < 20) {//&& year.startsWith("0")) {
        return tmp + 2000;
      } else if (tmp <= 99) {
        return tmp + 1900;
      }
    } else
    /*
      04, 11, 2009, 1911
     */
    if (year.matches("[0-9]+")) {
      int tmp = Ints.tryParse(year);
      if (tmp < 20) {
        return tmp + 2000;
      } else if (tmp <= 99) {
        return tmp + 1900;
      } else if (tmp > 2017) {
        return null; // needed atm
      } else {
        return tmp;
      }
    } else
    /*
      Any number with 4 digits within "long" string, e.g.,
      "Spider in the Snow - Live in Japan 2011"
     */
    if (year.length() > 9 && fourDigitsMatcher.find()) {
//      System.out.println("4d: " + fourDigitsMatcher.group(1));
      return Ints.tryParse(fourDigitsMatcher.group(1));

//      return Ints.tryParse(fourDigitsMatcher.group(1));
    }
    return null;
  }

  public static String fixLanguage(String lang) {
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
