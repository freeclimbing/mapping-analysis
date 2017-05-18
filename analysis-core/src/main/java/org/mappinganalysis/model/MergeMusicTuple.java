package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple12;
import org.mappinganalysis.model.api.Identifiable;
import org.mappinganalysis.model.api.IntSources;
import org.mappinganalysis.model.api.Labeled;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

import java.util.Set;

/**
 * 0. vertex id - Long
 * 1. label - String
 * 2. album - String
 * 3. artist - String
 * 4. number - String
 * 5. year - Int
 * 6. length - Int
 * 7. lang - String
 * 8. sources - Int
 * 9. clustered elements list - LongSet
 * 10. blocking label - String
 * 11. activity flag - Bool
 */
public class MergeMusicTuple
    extends Tuple12<Long, String, String, String, String, Integer, Integer,
    String, Integer, LongSet, String, Boolean>
    implements Identifiable, Labeled, IntSources, MergeTupleAttributes {

  public MergeMusicTuple() {
    this.f9 = new LongSet();
    this.f11 = true;
  }

  /**
   * Constructor for fake tuples (with fake values)
   */
  public MergeMusicTuple(Long id) {
    super(id,
        Constants.EMPTY_STRING,
        Constants.EMPTY_STRING,
        Constants.EMPTY_STRING,
        Constants.EMPTY_STRING,
        0,
        0,
        Constants.EMPTY_STRING,
        0,
        new LongSet(id),
        Constants.EMPTY_STRING,
        false);
  }

  public String getString(String attribute) {
    switch (attribute) {
      case Constants.LABEL:
        return getLabel();
      case Constants.ALBUM:
        return getAlbum();
      case Constants.ARTIST:
        return getArtist();
      case Constants.NUMBER:
        return getNumber();
      default:
        return null;
    }
  }

  public String getAlbum() {
    return f2;
  }

  public void setAlbum(String album) {
    f2 = album;
  }

  public String getArtist() {
    return f3;
  }

  public void setArtist(String artist) {
    f3 = artist;
  }

  public String getNumber() {
    return f4;
  }

  public void setNumber(String number) {
    f4 = number;
  }

  public int getYear() {
    return f5;
  }

  public void setYear(int year) {
    f5 = year;
  }

  public int getLength() {
    return f6;
  }

  public void setLength(int length) {
    f6 = length;
  }

  public String getLang() {
    return f7;
  }

  public void setLang(String lang) {
    f7 = lang;
  }

  @Override
  public Long getId() {
    return f0;
  }

  @Override
  public void setId(Long id) {
    f0 = id;
  }

  @Override
  public String getLabel() {
    return f1;
  }

  @Override
  public void setLabel(String label) {
    f1 = label;
  }

  @Override
  public Set<Long> getClusteredElements() {
    return f9;
  }

  @Override
  public void addClusteredElements(Set<Long> elements) {
    f9.addAll(elements);
  }

  @Override
  public Integer size() {
    return AbstractionUtils.getSourceCount(f8);
  }

  @Override
  public void setBlockingLabel(String label) {
    f10 = label;
  }

  @Override
  public String getBlockingLabel() {
    return f10;
  }

  @Override
  public boolean isActive() {
    return f11;
  }

  @Override
  public void setActive(Boolean value) {
    f11 = value;
  }

  @Override
  public Integer getIntSources() {
    return f8;
  }

  @Override
  public void setIntSources(Integer intSources) {
    f8 = intSources;
  }
}
