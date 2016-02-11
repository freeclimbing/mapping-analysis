package org.mappinganalysis.model;

import com.google.common.collect.Maps;
import org.apache.flink.util.StringUtils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Custom map for representing properties for vertices. This is needed
 * because a Flink Vertex can only have POJOs as value.
 */
public class ObjectMap implements Map<String, Object> {
  Map<String, Object> map = null;

  public ObjectMap(ObjectMap map) {
    this.map = map;
  }

  public ObjectMap(Map<String, Object> map) {
    this.map = map;
  }

  public String toString() {
    return "(" + StringUtils.arrayAwareToString(map) + ")";
  }

  public ObjectMap() {
    map = Maps.newHashMap();
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
