package org.mappinganalysis.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.flink.util.StringUtils;
import org.mappinganalysis.utils.Utils;

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

  public ObjectMap(Map<String, Object> map) {
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
  public boolean hasNoType(String type) {
    return map.containsKey(type) && map.get(type).equals(Utils.NO_TYPE_AVAILABLE)
        || map.containsKey(type) && map.get(type).equals(Utils.NO_TYPE_FOUND);
  }

  public boolean hasGeoProperties() {
    if (map.containsKey(Utils.LAT) && map.containsKey(Utils.LON)) {
      return Boolean.TRUE;
    } else {
      return Boolean.FALSE;
    }
  }

  public Double getLatitude() {
    return getGeoValue(Utils.LAT);
  }

  public Double getLongitude() {
    return getGeoValue(Utils.LON);
  }

  private Double getGeoValue(String latOrLon) {

    // todo check preconditions, write test
    Object geoValue = map.get(latOrLon);
    if (geoValue == null) {
      return null;
    }
    Preconditions.checkArgument(!(geoValue instanceof Set)
        && !(geoValue instanceof List), "lat or lon instance of Set or List: " + geoValue);
    Preconditions.checkArgument(geoValue instanceof Double, "not double value: " + geoValue);

    return Doubles.tryParse(geoValue.toString());
  }

  /**
   * Set geo lat/lon based on source of checked vertex, geonames and dbpedia are ranked higher.
   * @param geoMap property map of vertex
   */
  public void setGeoProperties(HashMap<String, GeoCode> geoMap) {
      if (geoMap.containsKey(Utils.GN_NS)) {
        setLatLon(geoMap.get(Utils.GN_NS));
      } else if (geoMap.containsKey(Utils.DBP_NS)) {
        setLatLon(geoMap.get(Utils.DBP_NS));
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
    map.put(Utils.LAT, geocode.getLat());
    map.put(Utils.LON, geocode.getLon());
  }

  public Set<Long> getVerticesList() {
    Object clusteredVertices = map.get(Utils.CL_VERTICES);

    if (clusteredVertices instanceof Set) {
      return (Set<Long>) clusteredVertices;
    } else {
      return Sets.newHashSet((long) clusteredVertices);
    }
  }

  public Set<String> getOntologiesList() {
    Object ontologies = map.get(Utils.ONTOLOGIES);

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
    Preconditions.checkArgument(!(key.equals(Utils.LAT) && map.containsKey(Utils.LAT))
        || !(key.equals(Utils.LON) && map.containsKey(Utils.LON)),
        map.get(Utils.LAT) + " - " + map.get(Utils.LON) + " LAT or LON already there, new: "
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
