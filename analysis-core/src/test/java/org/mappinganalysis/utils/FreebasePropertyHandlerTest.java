package org.mappinganalysis.utils;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.net.SocketTimeoutException;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * Parse props for example URI. Tests sometimes fail if resources are offline/slow.
 */
public class FreebasePropertyHandlerTest {
  private static final Logger LOG = Logger.getLogger(FreebasePropertyHandler.class);

  public static final String URI_FREEBASE = "http://rdf.freebase.com/ns/en.berlin";
  // TODO
  String abc = "http://rdf.freebase.com/ns/en.daytona_beach";

  @Test
  public void testHandler() throws Exception {
    FreebasePropertyHandler modeParser = new FreebasePropertyHandler(Utils.MODE_TYPE);
    FreebasePropertyHandler latLongTypeParser = new FreebasePropertyHandler(Utils.MODE_LAT_LONG_TYPE);
    FreebasePropertyHandler allParser = new FreebasePropertyHandler(Utils.MODE_ALL);

    checkProperties(modeParser, URI_FREEBASE, 8);
    checkProperties(latLongTypeParser, URI_FREEBASE, 11);
    checkProperties(allParser, URI_FREEBASE, 12);
  }

  public void checkProperties(FreebasePropertyHandler parser, String url, int expectedPropertyCount) throws Exception {
    LOG.info("check for properties: " + url);

    try {
      HashSet<String[]> labelProperties = parser.getPropertiesForURI(url);

      if (labelProperties.isEmpty()) {
        LOG.error("URL " + url + " not reachable, try again later.");
      }

      for (String[] propertyAndValue : labelProperties) {
        String property = propertyAndValue[0];
        String value = propertyAndValue[1];
        System.out.println("Property: " + property + " Value: " + value);
      }

      assertEquals(expectedPropertyCount, labelProperties.size());
    } catch (ConnectTimeoutException | SocketTimeoutException e) {
      LOG.error("Error on URL " + url + " , try again later:"
          + e.toString());
    }
  }
}