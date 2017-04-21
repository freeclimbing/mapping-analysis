package org.mappinganalysis.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by markus on 4/21/17.
 */
public class AbstractionUtilsTest {

  @Test
  public void testAbstractionUtils() throws Exception {
    HashMap<String, Integer> sourcesMap = AbstractionUtils.getSourcesMap(Constants.GEO_SOURCES);

    System.out.println(sourcesMap.toString());
    for (Map.Entry<String, Integer> entry : sourcesMap.entrySet()) {
      if (entry.getKey().equals(Constants.NYT_NS)) {
        assertTrue(1 == entry.getValue());
      }
    }

    HashMap<String, Integer> musicMap = AbstractionUtils.getSourcesMap(Constants.MUSIC_SOURCES);

    System.out.println(musicMap.toString());
    for (Map.Entry<String, Integer> entry : musicMap.entrySet()) {
      if (entry.getKey().equals("5")) {
        assertTrue(16 == entry.getValue());
      }
    }
  }

  @Test
  public void testGetMap() throws Exception {
    Set<String> sources = Sets.newHashSet();
    sources.add(Constants.DBP_NS);
    sources.add(Constants.FB_NS);

    Integer sourcesInt = AbstractionUtils.getSourcesInt(Constants.GEO, sources);
    System.out.println(sourcesInt);
  }
}