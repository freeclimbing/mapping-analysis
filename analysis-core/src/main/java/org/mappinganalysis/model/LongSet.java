package org.mappinganalysis.model;


import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * tmp helper class
 */
public class LongSet implements Set<Long>, Serializable {
  private static final long serialVersionUID = 23L;

  private Set<Long> set;

  public LongSet() {
    this.set = Sets.newHashSet();
  }

  public LongSet(Long id) {
    this.set = Sets.newHashSet(id);
  }

  public String toString() {
    return Joiner.on(";").join(set);
  }

  private Set<Long> getLongSet() {
    return set;
  }

  @Override
  public int size() {
    return set.size();
  }

  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return set.contains(o);
  }

  @Override
  public Iterator<Long> iterator() {
    return set.iterator();
  }

  /**
   * not working
   */
  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  /**
   * not working
   */
  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  @Override
  public boolean add(Long aLong) {
    return set.add(aLong);
  }

  @Override
  public boolean remove(Object o) {
    return set.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return set.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Long> c) {
    return set.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return set.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return set.removeAll(c);
  }

  @Override
  public void clear() {

  }
}
