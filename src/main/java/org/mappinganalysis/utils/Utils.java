package org.mappinganalysis.utils;

import com.google.common.primitives.Doubles;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.simplifiers.Simplifiers;
import org.simmetrics.tokenizers.Tokenizers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;

import static org.simmetrics.builders.StringMetricBuilder.with;

public class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class);

  private static String DB_NAME = "";
  public static final String LL_DB_NAME = "linklion_links_9_2015";
  public static final String BIO_DB_NAME = "bioportal_mappings_11_08_2015";
  public static final String GEO_PERFECT_DB_NAME = "hartung_perfect_geo_links";
  public static final String LL_FULL_NAME = "dbURLfullLinkLion";
  public static final String GEO_FULL_NAME = "dbURLfull";

  public static final String MODE_LAT_LONG_TYPE = "modeLatLongType";
  public static final String MODE_LABEL = "modeLabel";
  public static final String MODE_TYPE = "modeType";
  public static final String MODE_ALL = "modeAll";

  public static final String DB_PROPERY_FILE_NAME = "db";

  public static final String DB_URL_FIELD = "url";
  public static final String DB_ID_FIELD = "id";
  public static final String DB_ATT_VALUE_TYPE = "attValueType";
  public static final String DB_ONTID_FIELD = "ontID_fk";
  /**
   * DB connected components ID field
   */
  public static final String DB_CCID_FIELD = "ccID";
  /**
   * DB concept id field in connected components table
   */
  public static final String DB_CONCEPTID_FIELD = "conceptID";
  /**
   * DB connected components table name.
   */
  public static final String DB_CC_TABLE = "connectedComponents";
  /**
   * Minimum trigram similarity which is needed to keep link for further processing.
   */
  public static final Float TRIGRAM_INITIAL_THRESHOLD = 0.6f;
  /**
   * Maximal distance (in meter) between two resources where link is kept for further processing.
   */
  public static final Double GEO_COORD_INITIAL_THRESHOLD = 50000.0;
  /**
   * DB att name for 'rdf:type'/... field.
   */
  public static final String TYPE = "type";
  /**
   * DB attName for gn type detail information
   */
  public static final String TYPE_DETAIL = "typeDetail";
  /**
   * DB column name for 'geo:lat' field.
   */
  public static final String LAT = "lat";
  /**
   * DB column name for 'geo:lon' field.
   */
  public static final String LON = "lon";
  /**
   * DB column name for 'rdfs:label'/'gn:name'/... field.
   */
  public static final String LABEL = "label";
  /**
   * DB column name for GeoNames second type value field.
   */
  public static final String TYPE_INTERN = "typeIntern";
  /**
   * Field name for distance between two geo points.
   */
  public static final String DISTANCE = "distance";
  /**
   * Temp value for no label, should not be present in result.
   */
  public static final String NO_VALUE = "no value found";
  /**
   * Temp value for no type, should not be in result.
   */
  public static final String MINUS_ONE = "-1";
  /**
   * Field name for type match
   */
  public static final String TYPE_MATCH = "typeMatch";
  /**
   * Field name for trigram similarity
   */
  public static final String TRIGRAM = "trigramSim";
  /**
   * field name for connected component ID
   */
  public static final String CC_ID = "ccId";
  /**
   * property name in cluster representative where clustered vertivces are put into.
   */
  public static final String CL_VERTICES = "clusteredVertices";
  /**
   * source name of resource. internal name for db field ontID_fk
   */
  public static final String ONTOLOGY = "ontology";
  /**
   * DBpedia namespace
   */
  public static final String DBP_NAMESPACE = "http://dbpedia.org/";
  /**
   * GeoNames namespace
   */
  public static final String GN_NAMESPACE = "http://sws.geonames.org/";

  private static boolean DB_UTF8_MODE = false;

  public static HttpURLConnection openUrlConnection(URL url) {
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
		
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static Connection openDbConnection() throws SQLException {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    ResourceBundle prop = ResourceBundle.getBundle("db", Locale.getDefault(), loader);

    Connection con;
    String url = prop.getString("dbURL");
    LOG.info(url);
    url += DB_NAME;
    if (DB_UTF8_MODE) {
      url += "?useUnicode=true&characterEncoding=utf-8";
    }
    con = DriverManager.getConnection(url, prop.getString("user"), prop.getString("pw"));

    return con;
	}

  /**
   * Custom API key for Freebase data retrieval - 100.000 queries/day possible
   * @return api key
   */
  public static String getGoogleApiKey() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    ResourceBundle prop = ResourceBundle.getBundle("db", Locale.getDefault(), loader);

    return prop.getString("apiKey");
  }
  /**
   * Get a MongoDB (currently only working on wdi05 -> localhost)
   * @param dbName mongo db name
   * @return MongoDatabase
   */
  public static MongoDatabase getMongoDatabase(String dbName) {
    MongoClient client = new MongoClient("localhost", 27017);

    return client.getDatabase(dbName);
  }

  /**
   * Set UTF8 Mode for database access
   * @param value true
   */
  public static void setUtf8Mode(boolean value) {
    DB_UTF8_MODE = value;
  }

  /**
   * Open a db connection to a specified database name.
   * @param dbName db name
   * @return db connection
   * @throws SQLException
   */
  public static Connection openDbConnection(String dbName) throws SQLException {
    DB_NAME = dbName;
    return openDbConnection();
  }

  public static String simplify(String value) {
    value = Simplifiers.removeNonWord().simplify(value);
    return Simplifiers.toLowerCase().simplify(value);
  }

  /**
   * Get trigram string metric.
   * @param simplify if true, non words are replaces and strings as lower case
   * @return metric
   */
  public static StringMetric getTrigramMetric(boolean simplify) {
    if (simplify) {
      return getTrigramMetric();
    } else {
      return with(new CosineSimilarity<String>())
          .tokenize(Tokenizers.qGram(3))
          .build();
    }
  }

  private static StringMetric getTrigramMetric() {
    return with(new CosineSimilarity<String>())
        .simplify(Simplifiers.replaceNonWord())
        .simplify(Simplifiers.toLowerCase())
        .tokenize(Tokenizers.qGram(3))
        .build();
  }

  /**
   * Geo coords helper function
   * @param latlon one of the two geocoordinates
   * @return single double value
   */
  public static Double getDouble(Object latlon) {
    // TODO how to handle multiple values in lat/lon correctly?

    if (latlon instanceof Set) {
      return Doubles.tryParse(((Set) latlon).iterator().next().toString());
    } else {
      return Doubles.tryParse(latlon.toString());
    }
  }
}
