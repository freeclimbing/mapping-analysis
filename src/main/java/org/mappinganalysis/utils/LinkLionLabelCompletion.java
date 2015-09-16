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
import org.apache.jena.atlas.web.HttpException;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Retrieve additional labels for LinkLion data.
 */
public class LinkLionLabelCompletion {

  private static final Logger LOG =
    Logger.getLogger(LinkLionLabelCompletion.class);
  String geoName = "http://www.geonames.org/ontology#name";
  String skosLabel = "http://www.w3.org/2004/02/skos/core#prefLabel";
  String rdfsLabel = "http://www.w3.org/2000/01/rdf-schema#label";
  String endpoint = "http://linklion.org:8890/sparql";
  Connection con = null;

  public LinkLionLabelCompletion() {
    this.con = Utils.openDbConnection();
  }

  /**
   * main - care for db connection
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {

    LinkLionLabelCompletion ll = new LinkLionLabelCompletion();
    System.out.println("Get nodes without label ..");
    ResultSet nodes = ll.getNodesWithoutLabels(ll.getConnection());
    System.out.println("Process nodes one by one ..");
    ll.processResult(ll.getConnection(), nodes);
  }

  private void processResult(Connection con, ResultSet nodes) throws
    SQLException {

    while (nodes.next()) {
      int id = nodes.getInt("id");
      String url = nodes.getString("url");
      if (url.startsWith("http://dbpedia.org")) {
        endpoint = "http://dbpedia.org/sparql";
      }

      com.hp.hpl.jena.query.ResultSet result = getProperties(endpoint, id, url);
      while (result.hasNext()) {
        if (isLabel(con, id, result)) {
          break;
        }
      }

      endpoint = "http://linklion.org:8890/sparql";
    }
  }

  private boolean isLabel(Connection con, int id,
    com.hp.hpl.jena.query.ResultSet result) throws SQLException {
    QuerySolution line = result.next();
    try {
      Literal object = line.getLiteral("o");
      Resource predicate = line.getResource("p");
      String uri = predicate.getURI();
      if (uri.equals(geoName) || uri.equals(skosLabel) ||
        uri.equals(rdfsLabel)) {
        String label = object.getString();
        System.out.println("label: " + label);

        setLabel(con, id, label);

        if (object.getLanguage().equals("en") ||   // best option
          object.getLanguage().equals("")) {      // second best option
          LOG.info("english language detected || language empty");
          return true;
        }
      }
    } catch (ClassCastException ignored) {
    }
    return false;
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
    } catch (HttpException e) {
      writeError(id, url, e);
    }
    return results;
  }

  private void writeError(int id, String url, HttpException e) throws
    SQLException {
    String error = "INSERT INTO error_concept (id, url, error) " +
      "VALUES (?, ?, ?)";

    PreparedStatement insertStmt = getConnection().prepareStatement(error);
    insertStmt.setInt(1, id);
    insertStmt.setString(2, url);
    insertStmt.setString(3, e.getMessage());
    insertStmt.executeUpdate();
    insertStmt.close();
  }

  private ResultSet getNodesWithoutLabels(Connection con) throws SQLException {
    String sqlNoLabel = "SELECT c.id, c.url FROM concept AS c LEFT JOIN " +
      "concept_attributes AS a ON c.id = a.id WHERE a.id IS NULL;";
    PreparedStatement s = con.prepareStatement(sqlNoLabel);
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
