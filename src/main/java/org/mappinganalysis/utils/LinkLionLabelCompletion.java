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
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

/**
 * Retrieve additional labels for LinkLion data.
 */
public class LinkLionLabelCompletion {

  private static final Logger LOG =
    Logger.getLogger(LinkLionLabelCompletion.class);
  String geoName = "http://www.geonames.org/ontology#name";
  String skosLabel = "http://www.w3.org/2004/02/skos/core#prefLabel";
  String rdfsLabel = "http://www.w3.org/2000/01/rdf-schema#label";
  String llEndpoint = "http://linklion.org:8890/sparql";
  String dbpEndpoint = "http://dbpedia.org/sparql";
  Connection con = null;

  public LinkLionLabelCompletion() {
    Utils.setUtf8Mode(true);
    this.con = Utils.openDbConnection();
  }

  /**
   * main - care for db connection
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    LinkLionLabelCompletion ll = new LinkLionLabelCompletion();

    System.out.println("Get nodes without label ..");
    ResultSet nodes = ll.getNodesWithoutLabels(ll.con);

    ResultSet errorNodes = ll.getErrorNodes(ll.con);
    HashSet<Integer> errorIDs = getErrorIDs(errorNodes);

    System.out.println("Process nodes one by one ..");
    ll.processResult(ll.con, nodes, errorIDs);
  }

  private static HashSet<Integer> getErrorIDs(ResultSet errorNodes) throws
    SQLException {
    HashSet<Integer> result = new HashSet<>();

    while (errorNodes.next()) {
      result.add(errorNodes.getInt(1));
    }

    return result;
  }

  private void processResult(Connection con, ResultSet nodes, HashSet<Integer>
    errorIDs) throws SQLException {
    int count = 0;
    while (nodes.next()) {
      String url = nodes.getString("url");
      int id = nodes.getInt("id");
      if (errorIDs.contains(id)) {
        break;
      }

      System.out.println(count + " id: " + id + " url " + url);
      ++count;

      com.hp.hpl.jena.query.ResultSet properties;
      String endpoint;
      if (url.startsWith("http://dbpedia.org")) {
        properties = getProperties(dbpEndpoint, id, url);
        endpoint = dbpEndpoint;
      } else {
        properties = getProperties(llEndpoint, id, url);
        endpoint = llEndpoint;
      }

      boolean isLabel = false;
      if (properties != null) {
        while (properties.hasNext()) {
          String label = getLabel(properties.next());
          if (!label.isEmpty()) {
            setLabel(con, id, label);
            isLabel = true;
            break;
          }
        }
        if (!isLabel) {
          String error = "no property found on " + endpoint;
          writeError(id, url, error);
        }
      }
    }
  }

  private String getLabel(QuerySolution line) throws
    SQLException {
    if (line.get("o").isLiteral()) {
      Literal object = line.getLiteral("o");
      Resource predicate = line.getResource("p");
      String uri = predicate.getURI();
      String label = object.getString();

      if (uri.equals(geoName)){
        return label;
      } else if (uri.equals(skosLabel) || uri.equals(rdfsLabel)) {
        if (object.getLanguage().equals("en") ||   // best option
          object.getLanguage().equals("")) {      // second best option
          return label;
        }
      }
    }

    return "";
  }

  private com.hp.hpl.jena.query.ResultSet getProperties(String endpoint, int
    id, String url) throws SQLException {
    String query = "SELECT * WHERE { <" + url + "> ?p ?o } ORDER BY ?p ?o";
    Query jenaQuery = QueryFactory.create(query, Syntax.syntaxARQ);

    com.hp.hpl.jena.query.ResultSet results = null;
    try {
      QueryExecution qExec = QueryExecutionFactory
        .sparqlService(endpoint, jenaQuery);
      results = ResultSetFactory.copyResults(qExec.execSelect());
      qExec.close();
    } catch (Exception e) {
      System.out.println("id: " + id + " url: " + url + " e: " + e.getMessage());
      writeError(id, url, e.getMessage());
    }

    return results;
  }

  private void writeError(int id, String url, String e) throws
    SQLException {
    String error = "INSERT INTO error_concept (id, url, error) " +
      "VALUES (?, ?, ?)";

    PreparedStatement insertStmt = getConnection().prepareStatement(error);
    insertStmt.setInt(1, id);
    insertStmt.setString(2, url);
    insertStmt.setString(3, e);
    insertStmt.executeUpdate();
    insertStmt.close();
  }

  private ResultSet getNodesWithoutLabels(Connection con) throws SQLException {
    String sqlNoLabel = "SELECT c.id, c.url FROM concept AS c LEFT JOIN " +
      "concept_attributes AS a ON c.id = a.id WHERE a.id IS NULL;";
    PreparedStatement s = con.prepareStatement(sqlNoLabel);

    return s.executeQuery();
  }


  private ResultSet getAttributes(Connection con) throws SQLException {
    String sqlErrorNodes = "SELECT id, attValue FROM concept_attributes where" +
      " id = 11638451;";
    PreparedStatement s = con.prepareStatement(sqlErrorNodes);

    return s.executeQuery();
  }

  private ResultSet getErrorNodes(Connection con) throws SQLException {
    String sqlErrorNodes = "SELECT id, url FROM error_concept;";
    PreparedStatement s = con.prepareStatement(sqlErrorNodes);

    return s.executeQuery();
  }

  private static void setLabel(Connection con, int id, String label) throws
    SQLException {
    String insert = "INSERT INTO concept_attributes (id, attName, " +
      "attValue) VALUES (?, ?, ?)";

    PreparedStatement insertStmt = con.prepareStatement(insert);
    insertStmt.setInt(1, id);
    insertStmt.setString(2, "label");
    insertStmt.setString(3, label);
    insertStmt.executeUpdate();
    insertStmt.close();

    System.out.println("Added label " + label + " for id: " + id);
  }

  public Connection getConnection() {
    return con;
  }
}
