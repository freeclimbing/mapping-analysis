package org.mappinganalysis.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.HashSet;

/**
 * Retrieve HTTP response with RDF content and handle it with SAX parser.
 */
public class RDFPropertyXMLHandler extends DefaultHandler {
  /**
   * All labels which can contain properties.
   */
  private static final String[] LABELS = new String[]{"rdfs:label",
      "skos:prefLabel",
      "gn:name"};
  /**
   * XML property for language
   */
  private static final String LANGUAGE_TAG = "xml:lang";
  /**
   * label not yet found
   */
  private boolean isLabel = false;
  /**
   * label type
   */
  private String labelType;
  /**
   * Set with all propertiesMap as key values for a given URL
   */
  private HashSet<String[]> properties = new HashSet<>();

  /**
   * Get all property values for a given URL
   * @param uri URL to be parsed
   * @return HashMap with all propertiesMap
   * @throws IOException
   * @throws ParserConfigurationException
   * @throws SAXException
   */
  public HashSet<String[]> getLabelsForURI(String uri)
      throws IOException, ParserConfigurationException, SAXException {
    final HttpParams params = new BasicHttpParams();
    HttpConnectionParams.setConnectionTimeout(params, 1800);
    HttpConnectionParams.setSoTimeout(params, 2800);
    DefaultHttpClient client = new DefaultHttpClient(params);
    HttpGet get = new HttpGet();
    get.setURI(URI.create(uri));
    get.setHeader("Accept", "application/rdf+xml");

    HttpResponse response = client.execute(get);
    HttpEntity entity = response.getEntity();
    String result = EntityUtils.toString(entity);

    SAXParserFactory factory = SAXParserFactory.newInstance();
    SAXParser parser = factory.newSAXParser();

    XMLReader reader = parser.getXMLReader();
    reader.setContentHandler(this);

    InputSource inputSource = new InputSource(new StringReader(result));
    reader.parse(inputSource);
    EntityUtils.consume(entity);

    return getProperties();
  }

  /**
   * SAX helper startElement
   * @param uri uri
   * @param localName localName
   * @param qName qName
   * @param attributes attributes
   * @throws SAXException
   */
  public void startElement(String uri, String localName, String qName,
                           Attributes attributes) throws SAXException {
    for (String label : LABELS) {
      if (qName.equalsIgnoreCase(label)) {
        isLabel = true;
        labelType = label;
        int attrLength = attributes.getLength();
        for (int i = 0; i < attrLength; ++i) {
          if (attributes.getQName(i).equalsIgnoreCase(LANGUAGE_TAG)) {
            String lang = " " + attributes.getQName(i) + "=" + attributes
                .getValue(i);
            labelType = labelType.concat(lang);
          }
        }
      }
    }
  }

  /**
   * SAX helper class characters
   * @param ch ch
   * @param start start
   * @param length length
   * @throws SAXException
   */
  public void characters(char[] ch, int start, int length) throws SAXException {
    String s = new String(ch, start, length).trim();
    if (isLabel) {
      String[] element = new String[] {labelType, s};
      properties.add(element);
      isLabel = false;
    }
  }

  /**
   * Get all propertiesMap.
   * @return HashMap with all propertiesMap
   */
  public HashSet<String[]> getProperties() {
    return properties;
  }
}
