package org.mappinganalysis.utils;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.URISyntaxException;
import java.util.HashSet;

/**
 * Retrieve HTTP response with RDF content and handle Freebase properties.
 */
public class FreebasePropertyHandler {
  /**
   * All labels which can contain properties.
   */
  private static final String GEO_LOCATION = "ns:location.location.geolocation";
  private static final String TYPE = "ns:rdf:type";
  private static final String ELEVATION = "ns:location.geocode.elevation";
  private static final String LATITUDE = "ns:location.geocode.latitude";
  private static final String LONGITUDE = "ns:location.geocode.longitude";
  private static final String LOCATION_START = "ns:location";
  private static final String FREEBASE_NS = "http://rdf.freebase.com/ns/";
  private static final String FB_SERVICE_URL = "https://www.googleapis.com/freebase/v1/rdf/";


  private static final String MODE_LAT_LONG_TYPE = "latLongType";
//  private static final String MODE_LABEL = "labelMode";
//  private static final String MODE_TYPE = "typeMode";

  private String mode = "";

  public FreebasePropertyHandler(String mode) {
    this.mode = mode;
  }

  /**
   * Default method to retrieve properties for an URI.
   * @param uri URI to be enriched
   * @return set of key value pairs with properties
   * @throws IOException
   */
  public HashSet<String[]> getPropertiesForURI(String uri) throws Exception {
    return getPropertiesForURI(uri, false);
  }

    /**
     * Method to retrieve properties for an URI, special case geo location can be triggered
     * @param uri URL to be parsed
     * @param isGeoLocation if true, geo properties will be retrieved
     * @return HashMap with all propertiesMap
     * @throws IOException
     */
  public HashSet<String[]> getPropertiesForURI(String uri, Boolean isGeoLocation) throws Exception {
    // start measuring time
    long startTime = System.nanoTime();

    String topicId = uri.substring(uri.lastIndexOf("/") + 1);
    topicId = topicId.replace('.', '/');
    URIBuilder builder = new URIBuilder(FB_SERVICE_URL.concat(topicId));
    HttpGet get = new HttpGet(builder.setParameter("key", Utils.getGoogleApiKey()).build());

    HttpResponse response = getDefaultHttpClient().execute(get);
    HttpEntity entity = response.getEntity();
    String result = EntityUtils.toString(entity);
    EntityUtils.consume(entity);

    HashSet<String[]> properties = new HashSet<>();
    BufferedReader reader = new BufferedReader(new StringReader(result));
    String line;
    while ((line = reader.readLine()) != null) {
      Iterable<String> lineIterable = Splitter.on(' ').omitEmptyStrings().split(line);
      String[] keyValue = Iterables.toArray(lineIterable, String.class);

      properties.addAll(getProperty(isGeoLocation, keyValue));
    }
    reader.close();

    long endTime = System.nanoTime();
    long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.#
    System.out.println("Time: " + duration / 1000000 + " for URI: " + uri);

    return properties;
  }

  /**
   * Get the property value from a single key value pair
   * @param isGeoLocation act on special keys if geo location
   * @param keyValue key and value
   * @return set of properties, if a geolocation is given, more than one property is returned
   * @throws IOException
   * @throws URISyntaxException
   */
  private HashSet<String[]> getProperty(Boolean isGeoLocation, String[] keyValue) throws Exception {
    HashSet<String[]> properties = new HashSet<>();
    if (keyValue.length == 2) {
      String key = keyValue[0];
      String value = removeTrailingSemicolon(keyValue[1]);
      if (!isGeoLocation) {
        if (key.equals(TYPE) && value.startsWith(LOCATION_START)) {
          value = FREEBASE_NS.concat(value.substring(value.indexOf(":") + 1));
//          System.out.println(key + " " + value);
          properties.add(new String[]{key, value});
        } else if (key.equals(GEO_LOCATION) && mode.equals(MODE_LAT_LONG_TYPE)) {
          String splitValue = value.substring(value.indexOf(":") + 1);
//          System.out.println("SPLITVALUE: " + splitValue);
          properties.addAll(getPropertiesForURI(FREEBASE_NS + splitValue, true));
        }
      } else if (mode.equals(MODE_LAT_LONG_TYPE) && (key.equals(ELEVATION)
          || key.equals(LATITUDE) || key.equals(LONGITUDE))) {
        value = value.substring(value.indexOf(":") + 1);
        properties.add(new String[]{key, value});
      }
    }
    return properties;
  }

  /**
   * Get Custom Http Client with high timeouts for slow SPARQL endpoints.
   * @return http client
   */
  private DefaultHttpClient getDefaultHttpClient() {
    final HttpParams params = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(params, 8800);
    HttpConnectionParams.setSoTimeout(params, 8800);

    return new DefaultHttpClient(params);
  }

  /**
   * Remove trailing semicolon from freebase property value
   * @param value value to be checked
   * @return string without semicolon
   */
  private String removeTrailingSemicolon(String value) {
    if (value.length() > 0 && value.charAt(value.length() - 1) == ';') {
      value = value.substring(0, value.length() - 1);
    }
    return value;
  }
}
