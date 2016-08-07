package org.mappinganalysis.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.flink.util.StringUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.util.*;

/**
 * Custom map for representing properties for vertices. This is needed
 * because a Flink Vertex can only have POJOs as value.
 */
public class ObjectMap implements Map<String, Object>, Serializable {
  private static final long serialVersionUID = 1L;

  private Map<String, Object> map = null;

  public ObjectMap(ObjectMap map) {
    this.map = map;
  }

  public ObjectMap(HashMap<String, Object> map) {
    this.map = map;
  }

  public ObjectMap() {
    map = Maps.newHashMap();
  }

  public ObjectMap getMap() {
    return (ObjectMap) map;
  }

  public void setMap(ObjectMap map) {
    this.map = map;
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
    return map.containsKey(type) && getTypes(type).contains(Constants.NO_TYPE);
  }

  /**
   * Get label property from a single entity. If there is no label, return a placeholder value.
   * @return label
   */
  public String getLabel() {
    if (map.containsKey(Constants.LABEL)) {
      return map.get(Constants.LABEL).toString();
    } else {
      return Constants.NO_LABEL_FOUND;
    }
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

  /**
   * Get the hash cc id from an object map - no check if hash is available
   * @return hashCcId
   */
  public Long getHashCcId() {
    return (long) map.get(Constants.HASH_CC);
  }

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

  private Double getGeoValue(String latOrLon) {
    Object geoValue = map.get(latOrLon);
    // there are null values for entities without geo coordinates
    if (geoValue == null) {
      return null;
    }
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

  private void setLatLon(GeoCode geocode) {
    map.put(Constants.LAT, geocode.getLat());
    map.put(Constants.LON, geocode.getLon());
  }

  /**
   * Get list of vertex ids which are contained in a cluster.
   */
  public Set<Long> getVerticesList() {
    Object clusteredVertices = map.get(Constants.CL_VERTICES);

    if (clusteredVertices instanceof Set) {
      return (Set<Long>) clusteredVertices;
    } else {
      return Sets.newHashSet((long) clusteredVertices);
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
   * Get aggregated similarity of all edges for vertex
   */
  public Double getVertexSimilarity() {
    return Doubles.tryParse(map.get(Constants.VERTEX_AGG_SIM_VALUE).toString());
  }

  /**
   * Set computed aggregated similarity on vertex - all incoming or outgoing edge similarities aggregated
   * @param similarity double value of similarity
   */
  public void setVertexSimilarity(double similarity) {
    map.put(Constants.VERTEX_AGG_SIM_VALUE, similarity);
  }

  /**
   * Get source dataset name for vertex.
   */
  public String getOntology() {
    return map.get(Constants.ONTOLOGY).toString();
  }

  /**
   * Get list of source dataset names which are contained in the cluster.
   */
  public Set<String> getOntologiesList() {
    Object ontologies = map.get(Constants.ONTOLOGIES);

    if (ontologies instanceof Set) {
      return (Set<String>) ontologies;
    } else {
      return Sets.newHashSet(ontologies.toString());
    }
  }

  /**
   * Add a key value pair, if key already exists, a set of values is created or extended.
   * @param key property name
   * @param value property value
   */
  public void addProperty(String key, Object value) {

    Preconditions.checkNotNull(value, "new lat or lon null: " + map.toString());

    //Todo here rly needed? write test
    Preconditions.checkArgument(!(key.equals(Constants.LAT) && map.containsKey(Constants.LAT))
        || !(key.equals(Constants.LON) && map.containsKey(Constants.LON)),
        map.get(Constants.LAT) + " - " + map.get(Constants.LON) + " LAT or LON already there, new: "
        + key + ": " + value.toString());


    if (map.containsKey(key)) {
      Object oldValue = map.get(key);
      if (oldValue instanceof Set) {
        Set<Object> values = Sets.newHashSet((Set<Object>) oldValue);
        values.add(value);
        map.put(key, values);
      } else {
        map.put(key, Sets.newHashSet(oldValue, value));
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

  @Override
  public Object put(String s, Object o) {
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
