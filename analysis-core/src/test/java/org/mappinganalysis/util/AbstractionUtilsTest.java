package org.mappinganalysis.util;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

  // helper test getSourcesInt
  @Test
  public void bitShiftTest() throws Exception {
    int foo = 1;
    int foobar = foo >> 1;
    int bar = foo << 9;

    assertEquals(512, bar);
    assertEquals(0, foobar);
  }

  @Test
  public void sourcesSizeTest() throws Exception {
    Set<String> sources = Sets.newHashSet();
    sources.add(Constants.DBP_NS);
    sources.add(Constants.FB_NS);
    assertEquals(10, AbstractionUtils.getSourcesInt(Constants.GEO, sources));

    sources = AbstractionUtils.getSourcesStringSet(Constants.GEO, 17);
    assertEquals(2, sources.size());
    assertTrue(sources.contains(Constants.NYT_NS));
    assertTrue(sources.contains(Constants.GN_NS));

    sources = AbstractionUtils.getSourcesStringSet(Constants.GEO, 31);
    assertEquals(5, sources.size());
    assertTrue(sources.contains(Constants.NYT_NS));
    assertTrue(sources.contains(Constants.DBP_NS));
    assertTrue(sources.contains(Constants.FB_NS));
    assertTrue(sources.contains(Constants.GN_NS));
    assertTrue(sources.contains(Constants.LGD_NS));

    sources = AbstractionUtils.getSourcesStringSet(Constants.NC, 1023);
    assertEquals(10, sources.size());
    for (String ncSource : Constants.NC_MAP.keySet()) {
      assertTrue(sources.contains(ncSource));
    }
  }
}