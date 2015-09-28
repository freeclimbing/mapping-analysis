package org.mappinganalysis.utils;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Literal;
import com.mysql.jdbc.exceptions.jdbc4
  .MySQLIntegrityConstraintViolationException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Retrieve additional labels for LinkLion data.
 */
public class LinkLionLabelCompletion {

//  private static final Logger LOG =
//    Logger.getLogger(LinkLionLabelCompletion.class);

  static final String GEONAMES = "http://www.geonames.org/ontology#name";
  static final String SKOS_LABEL =
    "http://www.w3.org/2004/02/skos/core#prefLabel";
  static final String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
  static final String LAT_URL = "http://www.w3.org/2003/01/geo/wgs84_pos#lat";
  static final String LONG_URL = "http://www.w3.org/2003/01/geo/wgs84_pos#long";
  static final String TYPE_URL =
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

  static final String MODE_LAT_LONG_TYPE = "latLongType";
  static final String MODE_LABEL = "labelMode";
  static final String LABEL_NAME = "label";
  static final String LAT_NAME = "lat";
  static final String LON_NAME = "lon";
  static final String TYPE_NAME = "type";

  // TODO additional partly available: ele, population, feature class, country

  static final String LL_ENDPOINT = "http://linklion.org:8890/sparql";
  static final String DBP_ENDPOINT = "http://dbpedia.org/sparql";

  String processingMode = "";
  String dbName = "";
  Connection con = null;

  public LinkLionLabelCompletion() throws SQLException {
    Utils.setUtf8Mode(true);
    // TODO choose DB to process
    //this.dbName = Utils.GEO_PERFECT_DB_NAME;
    this.dbName = Utils.GEO_PERFECT_DB_NAME;
    this.con = Utils.openDbConnection(dbName);

    // TODO choose processing mode
    this.processingMode = MODE_LABEL;
  }

  /**
   * main - care for db connection
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    LinkLionLabelCompletion ll = new LinkLionLabelCompletion();

    System.out.println(ll.processingMode);
    System.out.println("Get nodes without specified properties ..");
//    ResultSet nodes = ll.getNodesWithoutLabels(ll.con);

    ResultSet vertices = ll.getAllNodes();
    // ll.getAllNodesWithAnyType();

//    ResultSet errorNodes = ll.getAllErrorNodes(ll.con);
//    ResultSet errorNodes = ll.getErrorNodes();

//    HashMap<Integer, ArrayList<String>> errorIDs = getErrorIDs(errorNodes);

    System.out.println("Process nodes one by one ..");
    ll.processResult(vertices);
  }

  /**
   * Add error node ids to set of error ids
   * @param errorNodes SQL result set
   * @return set of integers
   * @throws SQLException
   */
  private static HashMap<Integer, ArrayList<String>> getErrorIDs(
    ResultSet errorNodes) throws SQLException {
    HashMap<Integer, ArrayList<String>> resultMap = new HashMap<>();

    while (errorNodes.next()) {
      int key = errorNodes.getInt(1);
      ArrayList<String> list = resultMap.get(key);
      if (list == null) {
        list = new ArrayList<>();
        resultMap.put(key, list);
      }
      list.add(errorNodes.getString(2));
    }

    return resultMap;
  }

  /**
   * Process all nodes returned from database for label enrichment.
   * @param nodes SQL result set
   * @throws SQLException
   */
  private void processResult(ResultSet nodes) throws SQLException {
//    HashMap<Integer, String> processed = new HashMap<>();
    while (nodes.next()) {
      String url = nodes.getString("url");
      int id = nodes.getInt("id");
//      String type = nodes.getString("attName");
//      type = type != null ? type : "";
      // don't process if type is already existing or in error list
//      if (errorMap.containsKey(id)) {
//        if (errorMap.get(id).contains(type)) {
//          break;
//        }
//      }
//      System.out.println(id + " " + url + " " + type);
//      if (type.equals(LABEL_NAME)
//          || type.equals(LAT_NAME)
//          || type.equals(LON_NAME)
//          || type.equals(TYPE_NAME)) {
//        System.out.println(id + " with type " + type + " already processed");
//        continue;
//      }

      System.out.println(" id: " + id + " url: " + url);

      com.hp.hpl.jena.query.ResultSet properties = null;
      String endpoint = "";
      if (dbName.equals(Utils.LL_DB_NAME)) {
        if (url.startsWith("http://dbpedia.org")) {
          endpoint = DBP_ENDPOINT;
        } else {
          endpoint = LL_ENDPOINT;
        }
        properties = getProperties(endpoint, id, url);
      } else if (dbName.equals(Utils.GEO_PERFECT_DB_NAME)) {
        endpoint = LL_ENDPOINT;
        String graph = "http://www.linklion.org/geo-properties";
        properties = getProperties(endpoint, id, url, graph);
      }

      if (properties != null) {
        HashMap<String, Boolean> propsMap = new HashMap<>();
        propsMap.put(LABEL_NAME, Boolean.FALSE);
        propsMap.put(LAT_NAME, Boolean.FALSE);
        propsMap.put(LON_NAME, Boolean.FALSE);
        propsMap.put(TYPE_NAME, Boolean.FALSE);

        while (properties.hasNext()) {
          propsMap = getPropertyAndWriteToDb(properties.next(), con, id,
            propsMap);
        }
        reportErrors(url, id, endpoint, propsMap);
      }
    }
  }

  /**
   * Write potential errors for vertex to DB.
   * @param url vertex url
   * @param id vertex id
   * @param endpoint SPARQL endpoint
   * @param propsMap error map
   * @throws SQLException
   */
  private void reportErrors(String url, int id, String endpoint,
    HashMap<String, Boolean> propsMap) throws SQLException {
    String error = "property not found on " + endpoint;
    if (processingMode.equals(MODE_LAT_LONG_TYPE)) {
      if (!propsMap.get(LAT_NAME)) {
        writeError(id, url, error, LAT_NAME);
      }
      if (!propsMap.get(LON_NAME)) {
        writeError(id, url, error, LON_NAME);
      }
      if (!propsMap.get(TYPE_NAME)) {
        writeError(id, url, error, TYPE_NAME);
      }
    }
    if (processingMode.equals(MODE_LABEL) && !propsMap.get(LABEL_NAME)) {
      writeError(id, url, error, LABEL_NAME);
    }
  }

  /**
   * Get property value for vertex and write to DB.
   * @param line SPARQL result line
   * @param con db connection
   * @param id node id
   * @param propsMap error map
   * @throws SQLException
   */
  private HashMap<String, Boolean> getPropertyAndWriteToDb(QuerySolution line,
    Connection con, int id, HashMap<String, Boolean> propsMap) throws
    SQLException {
    String keyUrl = line.getResource("p").getURI();
    String key = getDbPropertyName(keyUrl);
    String value = getProperty(line);
//    if (keyUrl.equals(TYPE_URL)) {
      System.out.println("keyUrl: " + keyUrl + " key: " + key + " value: " +
        value);
//    }

    if (!value.isEmpty() && !key.isEmpty()) {
      String insert = "INSERT INTO concept_attributes (id, attName, " +
        "attValue) VALUES (?, ?, ?);";

      PreparedStatement insertStmt = con.prepareStatement(insert);
      insertStmt.setInt(1, id);
      insertStmt.setString(2, key);
      insertStmt.setString(3, value);
      try {
        insertStmt.executeUpdate();
        System.out.println("Written for Vertex: " + id + " Property: " +
          key + " Value: " + value);
        propsMap.put(key, Boolean.TRUE);
      } catch (MySQLIntegrityConstraintViolationException ignore) {
        if (keyUrl.equals(TYPE_URL)) {
          System.out.println("error");
        }
      } finally {
        insertStmt.close();
      }
    }
    return propsMap;
  }

  private String getDbPropertyName(String propTypeUrl) {
    String writeProperty = "";
    if (processingMode.equals(MODE_LAT_LONG_TYPE)) {
      switch (propTypeUrl) {
      case LAT_URL:
        writeProperty = LAT_NAME;
        break;
      case LONG_URL:
        writeProperty = LON_NAME;
        break;
      case TYPE_URL:
        writeProperty = TYPE_NAME;
        break;
      default:
        break;
      }
    } else if (processingMode.equals(MODE_LABEL) && (propTypeUrl.equals(
      RDFS_LABEL) || propTypeUrl.equals(SKOS_LABEL))) {
      writeProperty = LABEL_NAME;
    }
    return writeProperty;
  }

  /**
   * Get the property value from a single SQL result set line - if it is the
   * label, latitude, longitude or type.
   * @param line SQL query solution
   * @return property value, empty string if not found
   * @throws SQLException
   */
  private String getProperty(QuerySolution line) throws
    SQLException {
    if (line.get("o").isLiteral()) {
      Literal object = line.getLiteral("o");
      String predicateUri = line.getResource("p").getURI();
      String value = object.getString();

      switch (predicateUri) {
      case GEONAMES:
      case LAT_URL:
      case LONG_URL:
      case TYPE_URL:
        return value;
      case SKOS_LABEL:
      case RDFS_LABEL:
        if (object.getLanguage().equals("en") ||   // best option
          object.getLanguage().equals("")) {      // second best option
          return value;
        }
      default:
        return "";
      }
    }
    return "";
  }

  /**
   * Get all properties for a single url on a given SPARQL endpoint
   * @param endpoint SPARQL endpoint
   * @param id node id
   * @param url node url
   * @return all properties
   * @throws SQLException
   */
  private com.hp.hpl.jena.query.ResultSet getProperties(String endpoint, int
    id, String url) throws SQLException {
    return getProperties(endpoint, id, url, "");
  }

  /**
   * Get all properties for a single url on a given SPARQL endpoint from a graph
   * @param endpoint SPARQL endpoint
   * @param id node id
   * @param url node url
   * @param graph graph
   * @return all properties
   * @throws SQLException
   */
  private com.hp.hpl.jena.query.ResultSet getProperties(String endpoint, int
    id, String url, String graph) throws SQLException {
    if (!graph.isEmpty()) {
      graph = " FROM <" + graph + "> ";
    }
    String query = "SELECT * " + graph + " WHERE { <" + url + "> ?p ?o } " +
      "ORDER BY ?p ?o";
    Query jenaQuery = QueryFactory.create(query, Syntax.syntaxARQ);

    com.hp.hpl.jena.query.ResultSet results = null;
    try {
      QueryExecution qExec = QueryExecutionFactory
        .sparqlService(endpoint, jenaQuery);
      results = ResultSetFactory.copyResults(qExec.execSelect());
      qExec.close();
    } catch (Exception e) {
      System.out.println("id: " + id + " url: " + url + " e: " + e.getMessage());
      writeError(id, url, e.getCause() + " " + e.getMessage());
    }

    return results;
  }

  /**
   * Write potential error while retrieving label to DB for later analysis.
   * @param id node id
   * @param url node url
   * @param e error
   * @param type error type
   * @throws SQLException
   */
  private void writeError(int id, String url, String e, String type) throws
    SQLException {
    String error = "INSERT INTO error_concept (id, url, error, error_type) " +
      "VALUES (?, ?, ?, ?);";

    PreparedStatement insertStmt = con.prepareStatement(error);
    insertStmt.setInt(1, id);
    insertStmt.setString(2, url);
    insertStmt.setString(3, e);
    insertStmt.setString(4, type);
    insertStmt.executeUpdate();
    insertStmt.close();
  }

  /**
   * Write potential error while retrieving label to DB for later analysis.
   * @param id node id
   * @param url node url
   * @param e error
   * @throws SQLException
   */
  private void writeError(int id, String url, String e) throws
    SQLException {
    writeError(id, url, e, "general_error");
  }

  /**
   * Get all nodes without coordinates or type
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getAllNodes() throws SQLException {
    String sql = "SELECT DISTINCT id, url FROM concept;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Get all nodes without coordinates or type
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getAllNodesWithAnyType() throws SQLException {
    String sql = "SELECT DISTINCT c.id, c.url, a.attName FROM concept AS c " +
      "LEFT JOIN concept_attributes AS a ON c.id = a.id;";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Get all nodes where labels have not been found in a previously program run.
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getErrorNodes() throws SQLException {
    String custom;
    if (processingMode.equals(MODE_LAT_LONG_TYPE)) {
      custom = "IN (?, ?, ?);";
    } else {
      custom = "= ?;";
    }
    String sqlErrorNodes = "SELECT id, error_type FROM error_concept WHERE " +
      "error_type " + custom;
    PreparedStatement s = con.prepareStatement(sqlErrorNodes);
    if (processingMode.equals(MODE_LAT_LONG_TYPE)) {
      s.setString(1, LON_NAME);
      s.setString(2, LAT_NAME);
      s.setString(3, TYPE_NAME);
    } else {
      s.setString(1, LABEL_NAME);
    }

    return s.executeQuery();
  }
}
