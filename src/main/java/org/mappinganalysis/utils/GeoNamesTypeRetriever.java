package org.mappinganalysis.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.net.URL;
import java.util.Iterator;

/**
 * TypeOntologyRetriever
 */
public class GeoNamesTypeRetriever extends DefaultHandler {

  private Document doc = null;

  /**
   * Constructor - give ontology to parse
   * @param fileName ontology file name
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  public GeoNamesTypeRetriever(String fileName) throws IOException,
    SAXException, ParserConfigurationException {
    BufferedReader reader = getInputStreamReader(fileName);
    doc = getXmlDocument(reader);
  }

  /**
   * Get type name for gn ontology feature or class.
   * @param searchString feature/class name to look for
   * @param isGnClass true if gn:Class is searched
   * @return actual type name
   * @throws XPathExpressionException
   */
  public String getInstanceType(String searchString, Boolean isGnClass) throws
    XPathExpressionException {
    XPath xPath = XPathFactory.newInstance().newXPath();
    xPath.setNamespaceContext(new RDFNamespaceContext());

    String expression;
    if (isGnClass) {
      expression = "//gn:Class[@rdf:about='" + searchString
      + "']/rdfs:comment[@xml:lang='en']";
    } else {
      expression = "//gn:Code[@rdf:about='" + searchString +
        "']/skos:prefLabel[@xml:lang='en']";
    }

    return xPath.compile(expression).evaluate(doc);
  }

  /**
   * Get XML document from file.
   * @return XML document
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   * @param reader file reader
   */
  private Document getXmlDocument(BufferedReader reader) throws
    ParserConfigurationException, SAXException, IOException {
    DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    docFactory.setNamespaceAware(true);
    DocumentBuilder builder = docFactory.newDocumentBuilder();

    if (builder != null) {
      InputSource is = new InputSource(reader);
      return builder.parse(is);
    }
    return null;
  }

  /**
   * Get file from resources.
   * @param fileName file name
   * @return buffered file reader
   * @throws IOException
   */
  private BufferedReader getInputStreamReader(String fileName) throws IOException {
    URL fileUrl = getClass().getClassLoader().getResource(fileName);
    if (fileUrl != null) {
      File file = new File(fileUrl.getFile());
      return Files.asCharSource(file, Charsets.UTF_8).openBufferedStream();

    }
    return null;
  }

  /**
   * Helper class for defining needed geonames namespaces.
   */
  class RDFNamespaceContext implements NamespaceContext {
    @Override
    public String getNamespaceURI(String prefix) {
      if (prefix == null) throw new NullPointerException("Null prefix");
      else if ("rdf".equals(prefix))
        return "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
      else if ("gn".equals(prefix))
        return "http://www.geonames.org/ontology#";
      else if ("rdfs".equals(prefix))
        return "http://www.w3.org/2000/01/rdf-schema#";
      else if ("skos".equals(prefix))
        return "http://www.w3.org/2004/02/skos/core#";
      else if ("xml".equals(prefix))
        return XMLConstants.XML_NS_URI;
      return XMLConstants.XML_NS_URI;
    }

    @Override
    public String getPrefix(String s) {
      return null;
    }

    @Override
    public Iterator getPrefixes(String s) {
      return null;
    }
  }
}