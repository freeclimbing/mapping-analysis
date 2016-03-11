package org.mappinganalysis.model;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.util.StringUtils;
import org.mappinganalysis.utils.Utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Custom map for representing properties for vertices. This is needed
 * because a Flink Vertex can only have POJOs as value.
 */
public class ObjectMap implements Map<String, Object> {
  public ObjectMap getMap() {
    return (ObjectMap) map;
  }

  public void setMap(ObjectMap map) {
    this.map = map;
  }

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
    // TODO how to handle multiple values in lat/lon correctly?
    Object geoValue = map.get(latOrLon);
    if (geoValue instanceof Set) {
      return (Double) Iterables.get((Set) geoValue, 0);
    } else {
      return (Double) geoValue;
//      return Doubles.tryParse(latOrLon.toString());
    }
  }

  /**
   * Set geo lat/lon based on source of checked vertex, geonames and dbpedia are ranked higher.
   * @param geoMap property map of vertex
   */
  public void setGeoProperties(HashMap<String, GeoCode> geoMap) {
      if (geoMap.containsKey(Utils.GN_NAMESPACE)) {
        setLatLon(geoMap.get(Utils.GN_NAMESPACE));
      } else if (geoMap.containsKey(Utils.DBP_NAMESPACE)) {
        setLatLon(geoMap.get(Utils.DBP_NAMESPACE));
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

  /**
   * Add a key value pair, if key already exists, a set of values is created or extended.
   * @param key property name
   * @param value property value
   */
  public void addProperty(String key, Object value) {
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
