package org.mappinganalysis.utils;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * Parse labels for 3 example URIs. Tests sometimes fail if resources are offline/slow.
 */
public class FreebasePropertyHandlerTest {
  private static final Logger LOG = Logger.getLogger(FreebasePropertyHandler.class);
  public static final String URI_FREEBASE = "http://rdf.freebase.com/ns/en.berlin";

  private static final String MODE_LAT_LONG_TYPE = "latLongType";
//  private static final String MODE_LABEL = "labelMode";
  private static final String MODE_TYPE = "typeMode";

//  public static final String geoLocation = "ns:location.location.geolocation";
//  public static final String locationType = "ns:rdf:type";

  @Test
  public void testHandler() throws Exception {
    FreebasePropertyHandler modeParser = new FreebasePropertyHandler(MODE_TYPE);
    FreebasePropertyHandler latLongTypeParser = new FreebasePropertyHandler(MODE_LAT_LONG_TYPE);

    checkProperties(modeParser, URI_FREEBASE, 8);
    checkProperties(latLongTypeParser, URI_FREEBASE, 11);
  }

  public void checkProperties(FreebasePropertyHandler parser, String url, int expectedPropertyCount) throws Exception {
    LOG.info("check for properties: " + url);

    try {
      HashSet<String[]> labelProperties = parser.getPropertiesForURI(url);

      if (labelProperties.isEmpty()) {
        LOG.error("URL " + url + " not reachable, try again later.");
      }

      assertEquals(expectedPropertyCount, labelProperties.size());

      for (String[] propertyAndValue : labelProperties) {
        String property = propertyAndValue[0];
        String value = propertyAndValue[1];
        System.out.println("Property: " + property + " Value: " + value);
      }

    } catch (ConnectTimeoutException | SocketTimeoutException e) {
      LOG.error("Error on URL " + url + " , try again later:"
          + e.toString());
    }
  }
}