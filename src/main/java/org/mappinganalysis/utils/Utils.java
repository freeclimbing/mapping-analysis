package org.mappinganalysis.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

public class Utils {

  private static final Logger LOG = Logger.getLogger(Utils.class);

  public static final String MONGO_DB_HASH = "hash"; //linklion
  public static final String HASH_OUT = "geo-linklion.txt";
  public static final String MONGO_DB_HARTUNG = "hartung";//hartung low sim res
  public static final String HARTUNG_OUT = "geo-hartung.txt";
  public static final String MONGO_DB_PERFECT = "perfectGeo";// perfect hartung
  public static final String PERFECT_OUT = "geo-hartung-perfect.txt";

  public static final String LL_DB_NAME = "linklion_links_9_2015";
  public static final String BIO_DB_NAME = "bioportal_mappings_11_08_2015";

  public static final String COL_CC = "cc";
  public static final String CC_ATTR_VERTEX = "vertex";
  public static final String CC_ATTR_COMPONENT = "component";

  public static final String COL_VERTICES = "vertices";
  public static final String VERTICES_ATTR_RES = "res";
  public static final String VERTICES_ATTR_COUNT = "count";

  public static final String COL_EDGES = "edges";
  public static final String EDGES_ATTR_SUBJECT = "subject";
  public static final String EDGES_ATTR_OBJECT = "object";

  public static final String COL_LABELS = "labels";
  public static final String LABELS_ATTR_ID = "id";
  public static final String LABELS_ATTR_LABEL = "label";

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

	public static Connection openDbConnection() {
	
		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("db.properties");
			// load a properties file
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		Connection con = null;
		try {
      String url = prop.getProperty("dbURL");
      if (DB_UTF8_MODE) {
         url += "?useUnicode=true&characterEncoding=utf-8";
      }
			con = DriverManager.getConnection(url, prop.getProperty("user"), prop
        .getProperty("pw"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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

  public static void setUtf8Mode(boolean value) {
    DB_UTF8_MODE = value;
  }
}
