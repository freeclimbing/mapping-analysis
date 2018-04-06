package org.mappinganalysis.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.flink.util.StringUtils;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.api.CustomOperation;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.util.*;

/**
 * Custom map for representing properties for vertices. This is needed
 * because a Flink Vertex can only have POJOs as value.
 */
public class ObjectMap
    implements Map<String, Object>, Serializable {
  private static final long serialVersionUID = 42L;
  private static final Logger LOG = Logger.getLogger(ObjectMap.class);

  private String mode;
  private Map<String, Object> map;

  /**
   * Constructor for given object map
   * TODO only used in basic test
   */
  @Deprecated
  public ObjectMap(ObjectMap map) {
    this.map = map;
  }

  /**
   * Constructor for hash map
   * TODO only used in JSONToEntity
   */
  public ObjectMap(HashMap<String, Object> map) {
    this.map = map;
  }

  /**
   * Default constructor
   */
  public ObjectMap() {
    this.map = Maps.newHashMapWithExpectedSize(10);
  }

  /**
   * Constructor containing the current data set mode - music or geo domain
   */
  public ObjectMap(String mode) {
    this.map = Maps.newHashMapWithExpectedSize(10);
    this.mode = mode;
  }

  /**
   * Constructor to apply data domain on creation.
   * @param propertyMap vertex property map
   * @param domain data domain
   */
  public ObjectMap(ObjectMap propertyMap, DataDomain domain) {
    this.map = propertyMap;
    if (domain.equals(DataDomain.GEOGRAPHY)) {
      this.mode = Constants.GEO;
    } else if (domain.equals(DataDomain.MUSIC)) {
      this.mode = Constants.MUSIC;
    } else if (domain.equals(DataDomain.NC)) {
      this.mode = Constants.NC;
    }
  }

  /**
   * Constructor to apply data domain on creation.
   * @param domain data domain
   */
  public ObjectMap(DataDomain domain) {
    this(new ObjectMap(), domain);
  }

  public Map<String, Object> getMap() {
    return map;
  }

  public void setMap(Map<String, Object> map) {
    this.map = map;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public void setMode(DataDomain domain) {
    if (domain.equals(DataDomain.GEOGRAPHY)) {
      this.mode = Constants.GEO;
    } else if (domain.equals(DataDomain.MUSIC)) {
      this.mode = Constants.MUSIC;
    } else if (domain.equals(DataDomain.NC)) {
      this.mode = Constants.NC;
    }
  }

  public String getMode() {
    return mode;
  }

  public ObjectMap runOperation(
      CustomOperation<ObjectMap> operation) {
    operation.setInput(this);
    return operation.createResult();
  }

  public String toString() {
    return "(" + StringUtils.arrayAwareToString(map) + ")";
  }

  /**
   * Check if a specific type is empty.
   * @param type specific type
   * @return true if type has value not available or not found
   */
  public boolean hasTypeNoType(String type) {
    return map.containsKey(type)
        && getTypes(type).contains(Constants.NO_TYPE);
//        && getTypes(type).size() == 1;
  }

  /**
   * Get label property from a single entity. If there is no label, return a placeholder value.
   * @return label
   */
  public String getLabel() {
//    LOG.info("getLabel");
    if (map == null) {
      LOG.info("map null in get label");
      return Constants.NO_LABEL_FOUND;
    }

    if (map.containsKey(Constants.LABEL)) {
      return map.get(Constants.LABEL).toString();
    } else {
      return Constants.NO_LABEL_FOUND;
    }
  }

  public void setIdfLabel(String label) {
    if (label == null) {
      map.put(Constants.IDF_LABEL, Constants.NO_VALUE);
    } else {
      map.put(Constants.IDF_LABEL, label);
    }
  }

  public String getIdfLabel() {
    if (map == null) {
      LOG.info("map null in get idf label");
      return Constants.NO_LABEL_FOUND;
    }

    if (map.containsKey(Constants.IDF_LABEL)) {
      return map.get(Constants.IDF_LABEL).toString();
    } else {
      return Constants.NO_LABEL_FOUND;
    }
  }

  public void setLabel(String label) {
    if (label == null || label.equals(Constants.CSV_NO_VALUE)) {
      map.put(Constants.LABEL, Constants.NO_VALUE);
    } else {
      map.put(Constants.LABEL, label);
    }
  }

  public void setArtistTitleAlbum(String value) {
    map.put(Constants.ARTIST_TITLE_ALBUM, value);
  }

  public String getArtistTitleAlbum() {
    return map.get(Constants.ARTIST_TITLE_ALBUM).toString();
  }

  public HashMap<String, Double> getIDFs() {
    if (map.containsKey(Constants.IDF_VALUES)) {
      return (HashMap<String, Double>) map.get(Constants.IDF_VALUES);
    } else {
      return Maps.newHashMap();
    }
  }

  public void setIDFs(HashMap<String, Double> idfs) {
      map.put(Constants.IDF_VALUES, idfs);
  }

  /**
   * Get the set of type strings for a vertex from the input graph.
   * @return String set of rdf:type values
   * @param type specify type to retrieve, if not available, return "no type available"
   */
  public Set<String> getTypes(String type) {
    if (map.containsKey(type)) {
      Object typeObject = map.get(type);

      if (typeObject instanceof Set) {
        return Sets.newHashSet((Set<String>) typeObject);
      } else {
        return Sets.newHashSet(typeObject.toString());
      }
    } else {
      return Sets.newHashSet(Constants.NO_TYPE);
    }
  }

  public Set<String> getTypesIntern() {
    return getTypes(Constants.TYPE_INTERN);
  }

  public void setTypes(String typeName, Set<String> types) {
    if (types.isEmpty()) {
      map.put(typeName, Sets.newHashSet(Constants.NO_TYPE));
    } else {
      map.put(typeName, types);
    }
  }

  public void addTypes(String typeName, Set<String> types) {
    if (types.contains(Constants.NO_TYPE)) {
      return;
    }

    Set<String> existingTypes = getTypes(typeName);
    if (!existingTypes.contains(Constants.NO_TYPE)) {
      existingTypes.addAll(types);
      setTypes(typeName, existingTypes);
    } else {
      setTypes(typeName, types);
    }
  }

  /**
   * Get the hash cc id from an object map, if not available, return null
   * @return hashCcId
   */
  public Long getHashCcId() {
    if (map.containsKey(Constants.HASH_CC)) {
      return (long) map.get(Constants.HASH_CC);
    } else {
      return null;
    }
  }

  public Long getCcId() {
    if (map.containsKey(Constants.CC_ID)) {
      return (long) map.get(Constants.CC_ID);
    } else {
      return null;
    }
  }

  public void setHashCcId(Long value) {
    map.put(Constants.HASH_CC, value);
  }

  public void setCcId(Long value) {
    map.put(Constants.CC_ID, value);
  }

  public Long getOldHashCcId() {
    if (map.containsKey(Constants.OLD_HASH_CC)) {
      return (long) map.get(Constants.OLD_HASH_CC);
    } else {
      return null;
    }
  }

  public void setOldHashCcId(Long value) {
    map.put(Constants.OLD_HASH_CC, value);
  }

  /**
   * Why not used?
   */
  @Deprecated
  public Boolean getVertexStatus() {
    if (map.containsKey(Constants.VERTEX_STATUS)) {
      return (boolean) map.get(Constants.VERTEX_STATUS);
    } else {
      return Boolean.TRUE;
    }
  }

  /**
   * SimSort only value, check if deprecated
   */
  public void setVertexStatus(Boolean value) {
    map.put(Constants.VERTEX_STATUS, value);
  }

  /**
   * Check if current latitude and longitude of the vertex are valid.
   */
  public boolean hasGeoPropertiesValid() {
    if (Utils.isValidLatitude(getLatitude()) && Utils.isValidLongitude(getLongitude())) {
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  public Double getLatitude() {
    return getGeoValue(Constants.LAT);
  }

  public Double getLongitude() {
    return getGeoValue(Constants.LON);
  }

  private void setLatitude(Double latitude) {
    map.put(Constants.LAT, latitude);
  }

  private void setLongitude(Double longitude) {
    map.put(Constants.LON, longitude);
  }

  public void setGeoProperties(Double latitude, Double longitude) {
    if (Utils.isValidGeoObject(latitude, longitude)) {
      setLatitude(latitude);
      setLongitude(longitude);
    }
  }

  private Double getGeoValue(String latOrLon) {
    Object geoValue = map.get(latOrLon);
    // there are null values for entities without geo coordinates
    if (geoValue == null) {
      return null;
    }
    // old check, perhaps deprecated? only single latitude/longitude allowed
    Preconditions.checkArgument(!(geoValue instanceof Set)
        && !(geoValue instanceof List), "lat or lon instance of Set or List - should not happen: " + geoValue);
    Preconditions.checkArgument(!(geoValue instanceof String), "string value: " + geoValue);

    Double result = Doubles.tryParse(geoValue.toString());

    if (latOrLon.equals(Constants.LAT) && Utils.isValidLatitude(result)
        || latOrLon.equals(Constants.LON) && Utils.isValidLongitude(result)) {
      return result;
    } else {
      return null;
    }
  }

  /**
   * Set geo lat/lon based on source of checked vertex, geonames and dbpedia are ranked higher.
   * @param geoMap property map of vertex
   */
  public void setGeoProperties(HashMap<String, GeoCode> geoMap) {
    if (!geoMap.isEmpty()) {
      if (geoMap.containsKey(Constants.GN_NS)) {
        setLatLon(geoMap.get(Constants.GN_NS));
      } else if (geoMap.containsKey(Constants.DBP_NS)) {
        setLatLon(geoMap.get(Constants.DBP_NS));
      } else {
        GeoCode result = null;
        int smallest = Integer.MAX_VALUE;
        for (Entry<String, GeoCode> entry : geoMap.entrySet()) {
          if (smallest > entry.hashCode()) {
            smallest = entry.hashCode();
            result = entry.getValue();
          }
        }
        setLatLon(result);
      }
    }
  }

  private void setLatLon(GeoCode geocode) {
    map.put(Constants.LAT, geocode.getLat());
    map.put(Constants.LON, geocode.getLon());
  }

  /**
   * Get list of vertex ids which are contained in a cluster.
   */
  public Set<Long> getVerticesList() {
    Object clusteredVertices = map.get(Constants.CL_VERTICES);

    if (clusteredVertices == null) {
      return null;
    } else if (clusteredVertices instanceof Set) {
      return (Set<Long>) clusteredVertices;
    } else {
      return Sets.newHashSet((long) clusteredVertices);
    }
  }

  public int getVerticesCount() {
    return getVerticesList().size();
  }

  public void setClusterVertices(Set<Long> vertexIds) {
    if (!vertexIds.isEmpty()) {
      map.put(Constants.CL_VERTICES, vertexIds);
    }
  }

  /**
   * Get aggregated similarity of vertex property values such as label, geo coordinates, type.
   */
  public Double getEdgeSimilarity() {
    return Doubles.tryParse(map.get(Constants.AGGREGATED_SIM_VALUE).toString());
  }

  /**
   * Set computed aggregated similarity on edge - compared a pair of vertex property
   * values such as label and geo coordinates
   * @param similarity double value of similarity
   */
  public void setEdgeSimilarity(double similarity) {
    map.put(Constants.AGGREGATED_SIM_VALUE, similarity);
  }

  /**
   * Set similarity for geographic distance of 2 entities
   * @param similarity double value
   */
  public void setGeoSimilarity(double similarity) {
    map.put(Constants.SIM_DISTANCE, similarity);
  }

  public void setLanguageSimilarity(double languageSimilarity) {
    map.put(Constants.SIM_LANG, languageSimilarity);
  }

  public void setLengthSimilarity(double lengthSimilarity) {
    map.put(Constants.SIM_LENGTH, lengthSimilarity);
  }

  public void setNumberSimilarity(double numberSimilarity) {
    map.put(Constants.SIM_NUMBER, numberSimilarity);
  }

  public void setYearSimilarity(double yearSimilarity) {
    map.put(Constants.SIM_YEAR, yearSimilarity);
  }

  /**
   * Get aggregated similarity of all edges for vertex
   */
  public Double getVertexSimilarity() {
    return Doubles.tryParse(map.get(Constants.VERTEX_AGG_SIM_VALUE).toString());
  }

  /**
   * Set label similarity for vertex.
   */
  public void setLabelSimilarity(double labelSimilarity) {
    map.put(Constants.SIM_LABEL, labelSimilarity);
  }

  /**
   * Get label similarity for vertex.
   */
  public Double getLabelSimilarity() {
    return Doubles.tryParse(map.get(Constants.SIM_LABEL).toString());
  }

  /**
   * Set computed aggregated similarity on vertex - all incoming or outgoing edge similarities aggregated
   * @param similarity double value of similarity
   */
  public void setVertexSimilarity(double similarity) {
    map.put(Constants.VERTEX_AGG_SIM_VALUE, similarity);
  }

  /**
   * Get source dataset name for vertex. Maps old "ontology" data source values to new ones.
   */
  public String getDataSource() {
    // needed for deprecated "ontology" value
    if (!map.containsKey(Constants.DATA_SOURCE)) {
      if (map.get(Constants.ONTOLOGY) == null) {
        return null;
      }
      setDataSource(map.get(Constants.ONTOLOGY).toString());
      map.remove(Constants.ONTOLOGY);
    }

    return map.get(Constants.DATA_SOURCE).toString();
  }

  public void setDataSource(String source) {
    map.put(Constants.DATA_SOURCE, source);
  }

  /**
   * Get list of source dataset names which are contained in the cluster.
   */
  public Set<String> getDataSourcesList() {
    Object dataSources = map.get(Constants.DATA_SOURCES);

//    if (dataSources == null) {
//      return null;
//    }

    if (dataSources instanceof Set) {
      return (Set<String>) dataSources;
    } else if (dataSources == null) {
      return Sets.newHashSet();
    } else {
      return Sets.newHashSet(dataSources.toString());
    }
  }

  /**
   * set clustered data sources to set of strings
   */
  public void setClusterDataSources(Set<String> sources) {
    if (sources != null && !sources.isEmpty()) {
      map.put(Constants.DATA_SOURCES, sources);
    }
  }

  public boolean hasClusterDataSources() {
    return map.get(Constants.DATA_SOURCES) != null;
  }

  public boolean hasClusterVertices() {
    return map.get(Constants.CL_VERTICES) != null;
  }

  /**
   * Get internal representation of all data sources in a cluster.
   */
  @Deprecated
  public Integer getIntDataSources() {
    Object dataSources = map.get(Constants.DATA_SOURCES);
    Set<String> sources;
    if (dataSources instanceof Set) {
      sources = (Set<String>) dataSources;
    } else if (dataSources == null) {
      sources = Sets.newHashSet(getDataSource());
    } else {
        sources = Sets.newHashSet(dataSources.toString());
    }

    return AbstractionUtils.getSourcesInt(mode, sources);
  }

  public Long getDataSourceEntityCount() {
    if (map.containsKey(Constants.DS_COUNT)) {

      return (long) map.get(Constants.DS_COUNT);
    } else {

      return 1L;
    }
  }

  public void setDataSourceEntityCount(Long count) {
    map.put(Constants.DS_COUNT, count);
  }

  /**
   * Get (comp) types as single int for merge.
   */
  public Integer getIntTypes() {
    Object typeProperty = map.get(Constants.COMP_TYPE);
    Set<String> types;
    if (typeProperty instanceof Set) {
      types = (Set<String>) typeProperty;
    } else {
      types = Sets.newHashSet(typeProperty.toString());
    }

    return AbstractionUtils.getTypesInt(types);
  }

  /**
   * Music dataset getter/setter
   */
  public void setLength(Integer length) {
    map.put(Constants.LENGTH, length);
  }

  public int getLength() {
    if (map.containsKey(Constants.LENGTH)
        && map.get(Constants.LENGTH) != null) {
      Integer result = Ints.tryParse(map.get(Constants.LENGTH).toString());
      if (result != null) {

        return result;
      }
    }

    return Constants.EMPTY_INT;
  }

  public void setYear(Integer year) {
    map.put(Constants.YEAR, year);
  }

  /**
   * Get year for entity. If no year is available, return {@value Constants#NO_VALUE}.
   */
  public Integer getYear() {
    if (map.containsKey(Constants.YEAR) && map.get(Constants.YEAR) != null) {

      return Ints.tryParse(map.get(Constants.YEAR).toString());
    } else {

      return Constants.EMPTY_INT;
    }
  }

  public void setNumber(String number) {
    if (number.equals(Constants.CSV_NO_VALUE)) {
      map.put(Constants.NUMBER, Constants.NO_VALUE);
    } else {
      map.put(Constants.NUMBER, number);
    }
  }

  /**
   * Get song number for entity. If no number is available, return {@value Constants#NO_VALUE}.
   */
  public String getNumber() {
    if (map.containsKey(Constants.NUMBER)) {

      return map.get(Constants.NUMBER).toString();
    } else {

      return Constants.NO_VALUE;
    }
  }

  public void setLanguage(String language) {
    map.put(Constants.LANGUAGE, language);
  }

  /**
   * Get language for entity. If no language is available, return {@value Constants#NO_VALUE}.
   */
  public String getLanguage() {
    if (map.containsKey(Constants.LANGUAGE)) {

      return map.get(Constants.LANGUAGE).toString();
    } else {

      return Constants.NO_VALUE;
    }
  }

  public void setArtist(String artist) {
    if (artist.equals(Constants.CSV_NO_VALUE)) {
      map.put(Constants.ARTIST, Constants.NO_VALUE);
    } else {
      map.put(Constants.ARTIST, artist);
    }
  }

  /**
   * Get artist for entity. If no artist is available, return {@value Constants#NO_VALUE}.
   */
  public String getArtist() {
    if (map.containsKey(Constants.ARTIST)) {

      return map.get(Constants.ARTIST).toString();
    } else {

      return Constants.NO_VALUE;
    }
  }

  public void setAlbum(String album) {
    if (album.equals(Constants.CSV_NO_VALUE)) {
      map.put(Constants.ALBUM, Constants.NO_VALUE);
    } else {
      map.put(Constants.ALBUM, album);
    }
  }

  /**
   * Get album for entity. If no album is available, return {@value Constants#NO_VALUE}.
   */
  public String getAlbum() {
    if (map.containsKey(Constants.ALBUM)) {
      return map.get(Constants.ALBUM).toString();
    } else {
      return Constants.NO_VALUE;
    }
  }

  /**
   * TF/IDF method to get max values from tmp result and add to result
   */
  public void addMinValueToResult(HashMap<String, Double> tmpResult) {
    double minValue = tmpResult.values().stream().min(Double::compare).get();

//    HashMap<String, Double> idfs = getIDFs();

    for(Iterator<Map.Entry<String, Double>> it = tmpResult.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, Double> entry = it.next();

      if (entry.getValue() == minValue) {
        map.put(entry.getKey(), entry.getValue());
//        idfs.put(entry.getKey(), entry.getValue());
        it.remove();
      }
    }
//    setIDFs(idfs);
  }

  /**
   * Based on a blocking strategy and based on data domain
   * set the blocking label for a single instance.
   * @param strategy BlockingStrategy {@see BlockingStrategy}
   */
  public void setBlockingKey(BlockingStrategy strategy) {
//    LOG.info("set blocking key for " + getLabel());
    map.put(Constants.BLOCKING_LABEL,
        Utils.getBlockingKey(strategy, mode, getLabel()));
  }

  /**
   * TODO Blocking key string for IDF?
   */
  public String getBlockingKey() {
    if (this.get(Constants.BLOCKING_LABEL) == null) {
      return Constants.EMPTY_STRING;
    } else {
      return this.get(Constants.BLOCKING_LABEL).toString();
    }
  }

  /**
   * Add a key value pair, if key already exists, a set of values is created or extended.
   * @param key property name
   * @param value property value
   * TODO direct access to properties via getter setter
   */
  @Deprecated
  public void addProperty(String key, Object value) {
    Preconditions.checkNotNull(value, "new lat or lon null: " + map.toString());

    if (map.containsKey(key)) {
      if (value.toString().equals(Constants.NO_TYPE)) {
        return;
      }
      Object oldValue = map.get(key);

      if (oldValue instanceof Set) {
        Set<Object> values = Sets.newHashSet((Set<Object>) oldValue);
        if (values.contains(Constants.NO_TYPE)) {
          values.remove(Constants.NO_TYPE);
        }
        values.add(value);
        map.put(key, values);
      } else {
        if (oldValue.toString().equals(Constants.NO_TYPE)) {
          map.put(key, value);
        } else {
          map.put(key, Sets.newHashSet(oldValue, value));
        }
      }
    } else {
      map.put(key, value);
    }
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    return map.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    return map.containsValue(o);
  }

  @Override
  public Object get(Object o) {
    return map.get(o);
  }

  /**
   * TODO understand and correct
   */
  @Override
  public Object put(String s, Object o) {
//    LOG.info("s: " + s);
//    LOG.info("o: " + o);
//    LOG.info(map);
//    LOG.info(map.toString());
//    LOG.info("l: " + getLabel());
    return map.put(s, o);
  }

  @Override
  public Object remove(Object o) {
    return map.remove(o);
  }

  @Override
  public void putAll(Map<? extends String, ?> inMap) {
    map.putAll(inMap);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<String> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<Object> values() {
    return map.values();
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    return map.entrySet();
  }
}
