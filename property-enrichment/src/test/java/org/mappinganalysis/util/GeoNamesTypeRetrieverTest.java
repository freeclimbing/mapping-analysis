package org.mappinganalysis.util;

import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class GeoNamesTypeRetrieverTest {

  @Test
  public void responseHandlerTest() throws ParserConfigurationException,
    SAXException, IOException, XPathExpressionException {
    GeoNamesTypeRetriever handler =
      new GeoNamesTypeRetriever("ontology_v3.1.rdf");

    String correctResult = "country, state, region ...";
    String classExperiment = handler.getInstanceType("#A", true);
    assertEquals(correctResult, classExperiment);

    String codeCorrectResult = "seaplane landing area";
    String codeExperiment = handler.getInstanceType("#H.AIRS", false);
    assertEquals(codeCorrectResult, codeExperiment);
  }
}
