package org.mappinganalysis.model;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.util.StringUtils;
import org.mappinganalysis.utils.Utils;

import java.security.Key;
import java.util.Collection;
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
   * todo check if needed like this
   * @param key
   * @param value
   */
  public void addPropertyToRepresentative(String key, Object value) {
    if (key.equals(Utils.LABEL)) {
      value = Utils.simplify((String) value);
    }
    addProperty(key, value);
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
