package org.mappinganalysis.model.functions;

import com.google.common.collect.Sets;
import org.mappinganalysis.util.Constants;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class CharSet implements Set<char[]>, Serializable {
    private static final long serialVersionUID = 23L;

  private Set<char[]> set;

  public CharSet(Set<char[]> set) {
    this.set = set;
  }

  public CharSet() {
    this.set = Sets.newHashSet();
  }

  @Override
  public int size() {
    return set.size();
  }

  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }

  public String toString() {
    String result = Constants.EMPTY_STRING;
    for (char[] chars : set) {
      result = result.concat(String.valueOf(chars)).concat(", ");
    }
    return result;
  }

  @Override
  public boolean contains(Object o) {
    return false;
  }

  @Override
  public Iterator<char[]> iterator() {
    return set.iterator();
  }

  @Override
  public Object[] toArray() {
    return set.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return set.toArray(a);
  }

  @Override
  public boolean add(char[] chars) {
    return set.add(chars);
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
  public boolean addAll(Collection<? extends char[]> c) {
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
    set.clear();
  }
}
