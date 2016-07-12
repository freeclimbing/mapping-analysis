package org.mappinganalysis.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
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

  public static final String DB_PROPERY_FILE_NAME = "db";

  public static final String DB_URL_FIELD = "url";
  public static final String DB_ATT_VALUE_TYPE = "attValueType";
  public static final String DB_ONTID_FIELD = "ontID_fk";

  /**
   * DB fields
   */
  public static final String DB_CCID_FIELD = "ccID";

  /**
   * DB column name for 'rdf:type'/..., 'geo:lat/lon', 'rdfs:label'/'gn:name'/... field.
   */
  public static final String TYPE = "type";

  private static boolean DB_UTF8_MODE = false;
  private static final HashFunction HF = Hashing.md5();

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
}