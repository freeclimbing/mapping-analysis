package org.mappinganalysis.utils;

import org.junit.Test;

import java.util.HashSet;

/**
 * Simple test
 */
public class LinkedGeoDataPropertyHandlerTest {

  @Test
  public void testGetPropertiesForURI() throws Exception {
    LinkedGeoDataPropertyHandler handler = new LinkedGeoDataPropertyHandler();

    HashSet<String[]> one = handler.getPropertiesForURI("http://linkedgeodata.org/triplify/node356553997");
    for (String[] strings : one) {
      System.out.println(strings[0] + " " + strings[1]);
    }
    HashSet<String[]> two = handler.getPropertiesForURI("http://linkedgeodata.org/triplify/node564418249");
    for (String[] strings : two) {
      System.out.println(strings[0] + " " + strings[1]);
    }

  }
}