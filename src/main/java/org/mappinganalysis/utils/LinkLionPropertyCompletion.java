package org.mappinganalysis.utils;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import javax.xml.xpath.XPathExpressionException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Retrieve additional property information for LinkLion data.
 */
public class LinkLionPropertyCompletion {

//  private static final Logger LOG =
//    Logger.getLogger(LinkLionLabelCompletion.class);

  private static final String GN_ONTOLOGY = "http://www.geonames.org/ontology";
  private static final String DBP_ONTOLOGY = "http://dbpedia.org/ontology";
  private static final String SCHEMA_ONTOLOGY = "http://schema.org";
  private static final String UMBEL_ONTOLOGY = "http://umbel.org/umbel/rc";
  private static final String LGD_ONTOLOGY = "http://linkedgeodata.org/ontology";
  private static final String FB_NS = "http://rdf.freebase.com/ns/";
  private static final String DBP_NS = "http://dbpedia.org/";

  private static final String GN_NAME = "http://www.geonames.org/ontology#name";
  private static final String SKOS_LABEL =
    "http://www.w3.org/2004/02/skos/core#prefLabel";
  private static final String RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
  private static final String LAT_URL = "http://www.w3.org/2003/01/geo/wgs84_pos#lat";
  private static final String LONG_URL = "http://www.w3.org/2003/01/geo/wgs84_pos#long";
  private static final String TYPE_URL =
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
  private static final String GN_CLASS_TYPE =
    "http://www.geonames.org/ontology#featureClass";
  private static final String GN_CODE_TYPE =
    "http://www.geonames.org/ontology#featureCode";
  private static final String FB_TYPE = "ns:rdf:type";
  private static final String FB_ELEVATION = "ns:location.geocode.elevation";
  private static final String FB_LATITUDE = "ns:location.geocode.latitude";
  private static final String FB_LONGITUDE = "ns:location.geocode.longitude";

  private static final String LABEL_NAME = "label";
  private static final String LAT_NAME = "lat";
  private static final String LON_NAME = "lon";
  private static final String ELE_NAME = "ele";
  private static final String TYPE_NAME = "type";
  private static final String DB_URL_FIELD = "url";
  private static final String DB_ID_FIELD = "id";
  private static final String DB_ONTID_FIELD = "ontID_fk";

  private static final String TYPE_DETAIL_NAME = "typeDetail";

  // TODO additional partly available: ele, population, feature class, country

  private static final String LL_ENDPOINT = "http://linklion.org:8890/sparql";
  private static final String DBP_ENDPOINT = "http://dbpedia.org/sparql";

  String processingMode = "";
  boolean repairMode = Boolean.FALSE;
  boolean sourceToDbMode = Boolean.FALSE;

  String dbName = "";
  Connection con = null;
  // TODO 3. hartung dataset: only label + lat + lon are in this graph, for rdf:type use empty graph
  //String graph = "http://www.linklion.org/geo-properties";
  String graph = "";

  private GeonamesTypeRetriever tr = new GeonamesTypeRetriever("ontology_v3.1.rdf");
  private FreebasePropertyHandler handler;

  public LinkLionPropertyCompletion() throws Exception {
    Utils.setUtf8Mode(true);
    // TODO 1. choose DB to process
//    this.dbName = Utils.LL_DB_NAME;
    this.dbName = Utils.GEO_PERFECT_DB_NAME;
    this.con = Utils.openDbConnection(dbName);

    // TODO 2. choose processing mode
    this.processingMode = Utils.MODE_ALL;
    handler = new FreebasePropertyHandler(processingMode);
  }

  /**
   * main - care for db connection
   * @throws SQLException
   */
  public static void main(String[] args) throws Exception {

    LinkLionPropertyCompletion ll = new LinkLionPropertyCompletion();
    System.out.println(ll.processingMode);

    System.out.println("Get nodes with specified properties ..");
    // TODO 3. customize query to restrict working set, if needed
//    ResultSet nodes = ll.getNodesWithoutLabels();
//    ResultSet vertices = ll.getMaliciousDbpResources();
    ResultSet vertices = ll.getAllFreebaseNodes();
    //ll.enrichMissingSourceValues();

    System.out.println("Process nodes one by one ..");
    ll.processResult(vertices);
  }

  /**
   * Process all vertices returned from database for label enrichment.
   * @param vertices SQL result set
   * @throws SQLException
   */
  private void processResult(ResultSet vertices) throws Exception {
    HashMap<Integer, String> retryMap = new HashMap<>();
    int count = 0;
    while (vertices.next()) {
      ++count;
      int id = vertices.getInt(DB_ID_FIELD);
      String url = vertices.getString(DB_URL_FIELD);
      String endpoint = "";
      com.hp.hpl.jena.query.ResultSet properties = null;

      if (repairMode) {
        url = url.replaceAll("(.*)(%2C)(.*)", "$1,$3");
        updateDbProperty(id, DB_URL_FIELD, url);
      }
      // TODO rethink if this is always correct here (especially for the linklion dataset)
      if (url.startsWith(FB_NS)) {
        if (!writeFreebaseProperties(id, url)) {
          retryMap.put(id, url);
        }
      } else {
        if (url.startsWith(DBP_NS)) {
          endpoint = DBP_ENDPOINT;
        } else {
          endpoint = LL_ENDPOINT;
        }
        if (dbName.equals(Utils.LL_DB_NAME)) {
          properties = getPropertiesFromSparql(endpoint, id, url);
        } else if (dbName.equals(Utils.GEO_PERFECT_DB_NAME)) {
          properties = getPropertiesFromSparqlGraph(endpoint, id, url, graph);
        }
      }

      if (properties != null) {
        HashMap<String, Boolean> propsMap = getPropertyErrorMap();
        while (properties.hasNext()) {
          propsMap = parseSolutionLineAndWriteToDb(properties.next(), id, propsMap);
        }
        reportErrors(url, id, endpoint, propsMap);
      }
    }
    System.out.println("Processed " + count + " vertices.");
    retryMissingVertices(retryMap);
  }

  /**
   * Extract source from an URL.
   * @param url instance url
   * @return source string
   */
  private String getSource(String url) {
    String source = "";
    if (url.startsWith("http://")) {
      String regex = "(http://.*?/).*";
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(url);
      if (matcher.find()) {
        source = matcher.group(1);
      }
    }

    return source;
  }

  private HashMap<String, Boolean> getPropertyErrorMap() {
    HashMap<String, Boolean> propsMap = new HashMap<>();
    propsMap.put(LABEL_NAME, Boolean.FALSE);
    propsMap.put(LAT_NAME, Boolean.FALSE);
    propsMap.put(LON_NAME, Boolean.FALSE);
    propsMap.put(TYPE_NAME, Boolean.FALSE);

    return propsMap;
  }

  private void retryMissingVertices(HashMap<Integer, String> retryMap) throws Exception {
    // perhaps no longer needed with implemented google api key usage
    int retryCount = 5;
    System.out.println(retryCount + " retries left.");
    while (!retryMap.isEmpty() && (retryCount > 0)) {
      HashMap<Integer, String> tmpRetryMap = new HashMap<>();
      for (Integer id : retryMap.keySet()) {
        String value = retryMap.get(id);
        if (!writeFreebaseProperties(id, value)) {
          tmpRetryMap.put(id, value);
        }
      }
      retryMap = tmpRetryMap;
      --retryCount;
      System.out.println(retryCount + " retries left.");
    }
    for (Integer id : retryMap.keySet()) {
      System.out.println("Missing properties for id: " + id + " and property: " + retryMap.get(id));
    }
  }

  /**
   * Write all (Freebase) properties for a single vertex to the db.
   * @param id vertex id
   * @param url vertex url
   */
  private Boolean writeFreebaseProperties(int id, String url) throws Exception {
    HashSet<String[]> properties = handler.getPropertiesForURI(url);

    if (!properties.isEmpty()) {
      for (String[] property : properties) {
        parsePropertyAndWriteToDb(id, property);
      }
      return true;
    } else {
      return false;
    }
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
    if (processingMode.equals(Utils.MODE_LAT_LONG_TYPE)) {
      if (!propsMap.get(LAT_NAME)) {
        writeError(id, url, error, LAT_NAME);
      }
      if (!propsMap.get(LON_NAME)) {
        writeError(id, url, error, LON_NAME);
      }
      if (!propsMap.get(TYPE_NAME)) {
        writeError(id, url, error, TYPE_NAME);
      }
    } else if (processingMode.equals(Utils.MODE_TYPE) && !propsMap.get(TYPE_NAME)) {
      writeError(id, url, error, TYPE_NAME); // TODO whats with type detail?
    }
    if (processingMode.equals(Utils.MODE_LABEL) && !propsMap.get(LABEL_NAME)) {
      writeError(id, url, error, LABEL_NAME);
    }
  }

  /**
   * Get property value for vertex and write to DB.
   * @param id node id
   * @param keyValue key value pair from result
   * @throws SQLException
   */
  private void parsePropertyAndWriteToDb(int id, String[] keyValue)
      throws SQLException, XPathExpressionException {
    String key = getDbPropertyName(keyValue[0]);
    writePropertyToDb(id, key, keyValue[1]);
  }

  /**
   * Get property value for vertex and write to DB.
   * @param line SPARQL result line
   * @param id node id
   * @param propsMap error map
   * @throws SQLException
   */
  private HashMap<String, Boolean> parseSolutionLineAndWriteToDb(QuerySolution line,
      int id, HashMap<String, Boolean> propsMap) throws Exception {
    String keyUrl = line.getResource("p").getURI();
    String key = getDbPropertyName(keyUrl);
    String value = getPropertyValue(line);

    String errorKey = writePropertyToDb(id, key, value);
    if (!errorKey.isEmpty()) {
      propsMap.put(errorKey, Boolean.TRUE);
    }
    return propsMap;
  }

  /**
   * Update a single property value for a single vertex. (only working for col name 'url')
   * @param id vertex id
   * @param key property name
   * @param value new value
   */
  private void updateDbProperty(int id, String key, String value) throws SQLException {
    if (!value.isEmpty() && !key.isEmpty()
        && (key.equals(DB_URL_FIELD) || key.equals(DB_ONTID_FIELD))) {
      String update = "UPDATE concept SET ".concat(key).concat(" = ? WHERE id = ?;");

      PreparedStatement updStmt = con.prepareStatement(update);
      updStmt.setString(1, value);
      updStmt.setInt(2, id);

      updStmt.executeUpdate();
      updStmt.close();
      System.out.println("Written for Vertex: " + id + " Property: " +
          key + " Value: " + value);
    }
  }

  /**
   * Delete single attribute value.
   * @param id vertex id
   * @param value value to delete
   * @throws SQLException
   */
  private void deleteSingleValue(int id, String value) throws SQLException {
    if (!value.isEmpty()) {
      String del = "DELETE FROM concept_attributes WHERE attName = 'lon' AND attValue = ? AND id = ?;";

      PreparedStatement stmt = con.prepareStatement(del);
      stmt.setString(1, value);
      stmt.setInt(2, id);

      stmt.executeUpdate();
      stmt.close();
      System.out.println("Deleted for Vertex: " + id + " Value: " + value);
    }
  }

  /**
   * Write single property for single vertex to db. If query is unsuccessful, return key for error processing.
   * @param id vertex id
   * @param key property key
   * @param value property value
   * @return property key, if exception occurs
   * @throws SQLException
   */
  private String writePropertyToDb(int id, String key, String value) throws SQLException {
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
        return key;
      } catch (MySQLIntegrityConstraintViolationException ignore) {
        if (key.equals(TYPE_NAME)) {
          System.err.println(TYPE_NAME + "error");
        }
      } finally {
        insertStmt.close();
      }
    }
    return "";
  }

  /**
   * Source value needed for component check and further analysis.
   * Retrieves all vertices without source and extracts the value from URL.
   * @throws SQLException
   */
  private void enrichMissingSourceValues() throws SQLException {
    ResultSet vertices = getNodesMissingSource();

    while (vertices.next()) {
      int id = vertices.getInt(DB_ID_FIELD);
      String url = vertices.getString(DB_URL_FIELD);

      updateDbProperty(id, DB_ONTID_FIELD, getSource(url));
    }
  }

  private String getDbPropertyName(String propTypeUrl) {
    if (processingMode.equals(Utils.MODE_LAT_LONG_TYPE) || processingMode.equals(Utils.MODE_ALL)) {
      switch (propTypeUrl) {
        case LAT_URL:
        case FB_LATITUDE:
          System.out.println("writeProperty set to: " + LAT_NAME);
          return LAT_NAME;
        case LONG_URL:
        case FB_LONGITUDE:
          return LON_NAME;
        case FB_ELEVATION:
          return ELE_NAME;
      }
    }
    if ((processingMode.equals(Utils.MODE_LABEL) || processingMode.equals(Utils.MODE_ALL)) &&
      (propTypeUrl.equals(RDFS_LABEL) || propTypeUrl.equals(SKOS_LABEL))) {
      return LABEL_NAME;
    }
    if (!processingMode.equals(Utils.MODE_LABEL)) {
      switch (propTypeUrl) {
        case TYPE_URL:
        case FB_TYPE:
        case GN_CLASS_TYPE:
          System.out.println("writeProperty set to: " + TYPE_NAME + " propType: " + propTypeUrl);
          return TYPE_NAME;
        case GN_CODE_TYPE:
          System.out.println("writeProperty set to: " + TYPE_DETAIL_NAME);
          return TYPE_DETAIL_NAME;
      }
    }
    return "";
  }

  /**
   * Get the property value from a single SQL result set line - if it is the
   * label, latitude, longitude or type.
   * @param line SQL query solution
   * @return property value, empty string if not found
   * @throws SQLException
   */
  private String getPropertyValue(QuerySolution line) throws SQLException,
    XPathExpressionException {
    String predicateUri = line.getResource("p").getURI();
    if (line.get("o").isLiteral()) {
      Literal object = line.getLiteral("o");
      String value = object.getString();

      switch (predicateUri) {
      case GN_NAME:
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
      }
    } else if (line.get("o").isResource()) {
      Resource obj = line.getResource("o");
      String objNameSpace = obj.getNameSpace();
      if (objNameSpace == null) {
        return "";
      }
      if (objNameSpace.startsWith(GN_ONTOLOGY)) { // GeoNames special case type
        String name = "#" + obj.getLocalName();
        System.out.println("getPropertyValue().isResource(): " + name);
        if (predicateUri.equals(GN_CLASS_TYPE)) {
          return tr.getInstanceType(name, true);
        } else if (predicateUri.equals(GN_CODE_TYPE)) {
          return tr.getInstanceType(name, false);
        }
      }
      if (objNameSpace.startsWith(DBP_ONTOLOGY)
        || objNameSpace.startsWith(SCHEMA_ONTOLOGY)
        || objNameSpace.startsWith(UMBEL_ONTOLOGY)
        || objNameSpace.startsWith(LGD_ONTOLOGY)) {
        String name = obj.getURI();
        System.out.println("getPropertyValue().isResource(): " + name);
        return name;
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
  private com.hp.hpl.jena.query.ResultSet getPropertiesFromSparql(
    String endpoint, int id, String url) throws SQLException {
    return getPropertiesFromSparqlGraph(endpoint, id, url, "");
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
  private com.hp.hpl.jena.query.ResultSet getPropertiesFromSparqlGraph(
    String endpoint, int id, String url, String graph) throws SQLException {
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


  private ResultSet getMaliciousDbpResources() throws SQLException {
    repairMode = Boolean.TRUE;
    String sql = "SELECT id, url FROM hartung_perfect_geo_links.concept " +
        "where url like '%\\%2C%';";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Get all nodes without coordinates or type
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getAllFreebaseNodes() throws SQLException {
    String sql = "SELECT DISTINCT id, url FROM concept WHERE url like 'http://rdf.freebase.com%';";
    PreparedStatement s = con.prepareStatement(sql);

    return s.executeQuery();
  }

  /**
   * Only needed once per dataset, enrich missing source/ontID_fk fields in db with URLs of vertex.
   * @return SQL result set
   * @throws SQLException
   */
  private ResultSet getNodesMissingSource() throws SQLException {
    String sql = "SELECT DISTINCT id, url FROM concept WHERE ontID_fk IS NULL;";
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
    if (processingMode.equals(Utils.MODE_LAT_LONG_TYPE)) {
      custom = "IN (?, ?, ?);";
    } else {
      custom = "= ?;";
    }
    String sqlErrorNodes = "SELECT id, error_type FROM error_concept WHERE " +
      "error_type " + custom;
    PreparedStatement s = con.prepareStatement(sqlErrorNodes);
    if (processingMode.equals(Utils.MODE_LAT_LONG_TYPE)) {
      s.setString(1, LON_NAME);
      s.setString(2, LAT_NAME);
      s.setString(3, TYPE_NAME);
    } else {
      s.setString(1, LABEL_NAME);
    }

    return s.executeQuery();
  }
}
