package org.mappinganalysis.util;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;

public class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class);

  /**
   * Database property names.
   */
  private static String DB_NAME = "";

  public static final String LL_DB_NAME = "linklion_links_9_2015";
  public static final String BIO_DB_NAME = "bioportal_mappings_11_08_2015";
  public static final String GEO_PERFECT_DB_NAME = "hartung_perfect_geo_links";
  public static final String LL_FULL_NAME = "dbURLfullLinkLion";
  public static final String GEO_FULL_NAME = "dbURLfull";

  /**
   * Modes restricting entity enrichment.
   */
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
   * DB fields
   */
  public static final String DB_CCID_FIELD = "ccID";
  public static final String DB_CONCEPTID_FIELD = "conceptID";
  public static final String DB_CC_TABLE = "connectedComponents";

  /**
   * DB column name for 'rdf:type'/..., 'geo:lat/lon', 'rdfs:label'/'gn:name'/... field.
   */
  public static final String TYPE = "type";

  private static boolean DB_UTF8_MODE = false;
	public static Connection openDbConnection() throws SQLException {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    ResourceBundle prop = ResourceBundle.getBundle(Utils.DB_PROPERY_FILE_NAME,
        Locale.getDefault(), loader);

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
    ResourceBundle prop = ResourceBundle.getBundle(Utils.DB_PROPERY_FILE_NAME,
        Locale.getDefault(), loader);

    return prop.getString("apiKey");
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
}