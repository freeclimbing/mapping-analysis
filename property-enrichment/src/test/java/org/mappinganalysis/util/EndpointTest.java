package org.mappinganalysis.util;

import com.hp.hpl.jena.query.*;

import java.sql.SQLException;

import org.junit.Test;

public class EndpointTest {

  @Test
  public void testQueryResult() throws SQLException {
    String query = "SELECT *  WHERE { <http://sws.geonames.org/5665733/> ?p ?o . }";
//    String query2 = "SELECT *  WHERE { <http://dbpedia.org/resource/Hiroshima> ?p ?o . } ORDER BY ?p ?o";
    String endpoint = "http://linklion.org:8890/sparql";
//        String endpoint = "http://dbpedia.org/sparql";

    Query jenaQuery = QueryFactory.create(query, Syntax.syntaxARQ);
    com.hp.hpl.jena.query.ResultSet results = null;
    try {
      QueryExecution qExec = QueryExecutionFactory.sparqlService(endpoint, jenaQuery);

      results = ResultSetFactory.copyResults(qExec.execSelect());
      qExec.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (results != null) {
      System.out.println("not null");
      while (results.hasNext()) {
        System.out.println(results.next());
      }
    }
    // todo test for one of these lines, if not working, wdiserv1 could be read only
//    ( ?p = <http://www.geonames.org/ontology#name> ) ( ?o = "Memorial Park" )
//    ( ?p = <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ) ( ?o = "45.29578" )
//    ( ?p = <http://www.w3.org/2003/01/geo/wgs84_pos#long> ) ( ?o = "-108.91375" )
//    ( ?p = <http://www.geonames.org/ontology#countryCode> ) ( ?o = "US" )
//    ( ?p = <http://www.geonames.org/ontology#featureClass> ) ( ?o = <http://www.geonames.org/ontology#L> )
//    ( ?p = <http://www.geonames.org/ontology#featureCode> ) ( ?o = <http://www.geonames.org/ontology#L.PRK> )
//    ( ?p = <http://www.geonames.org/ontology#parentCountry> ) ( ?o = <http://sws.geonames.org/6252001/> )
  }
}
