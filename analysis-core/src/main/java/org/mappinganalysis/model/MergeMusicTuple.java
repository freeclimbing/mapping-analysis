package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple12;
import org.mappinganalysis.model.api.Identifiable;
import org.mappinganalysis.model.api.IntSources;
import org.mappinganalysis.model.api.Labeled;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

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

  public void setAttribute(String attribute, MergeMusicTuple value) {
    setAttribute(attribute, value, new MergeMusicTuple(value.getId()));
  }

  public void setAttribute(String attribute, MergeMusicTuple priority, MergeMusicTuple minor) {
    switch (attribute) {
      case Constants.LABEL:
        if (Utils.isSane(priority.getLabel())) {
          setLabel(priority.getLabel());
        } else if (Utils.isSane(minor.getLabel())) {
          setLabel(minor.getLabel());
        } else {
          setLabel(Constants.EMPTY_STRING);
        }
        break;
      case Constants.ALBUM:
        if (Utils.isSane(priority.getAlbum())) {
          setAlbum(priority.getAlbum());
        } else if (Utils.isSane(minor.getAlbum())) {
          setAlbum(minor.getAlbum());
        } else {
          setAlbum(Constants.EMPTY_STRING);
        }
        break;
      case Constants.ARTIST:
        if (Utils.isSane(priority.getArtist())) {
          setArtist(priority.getArtist());
        } else if (Utils.isSane(minor.getArtist())) {
          setArtist(minor.getArtist());
        } else {
          setArtist(Constants.EMPTY_STRING);
        }
        break;
      case Constants.NUMBER:
        if (Utils.isSane(priority.getNumber())) {
          setNumber(priority.getNumber());
        } else if (Utils.isSane(minor.getNumber())) {
          setNumber(minor.getNumber());
        } else {
          setNumber(Constants.EMPTY_STRING);
        }
        break;
      case Constants.BLOCKING_LABEL:
        if (Utils.isSane(priority.getBlockingLabel())) {
          setBlockingLabel(priority.getBlockingLabel());
        } else if (Utils.isSane(minor.getBlockingLabel())) {
          setBlockingLabel(minor.getBlockingLabel());
        } else {
          setBlockingLabel(Constants.EMPTY_STRING);
        }
        break;
      case Constants.YEAR:
        if (Utils.isSaneInt(priority.getYear())) {
          setYear(priority.getYear());
        } else if (Utils.isSaneInt(minor.getYear())) {
          setYear(minor.getYear());
        } else {
          setYear(Constants.EMPTY_INT);
        }
        break;
      case Constants.LENGTH:
        if (Utils.isSaneInt(priority.getLength())) {
          setLength(priority.getLength());
        } else if (Utils.isSaneInt(minor.getLength())) {
          setLength(minor.getLength());
        } else {
          setLength(Constants.EMPTY_INT);
        }
        break;
      default:
        throw new IllegalArgumentException("no attribute like " + attribute);
    }
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

  public Integer getInt(String attribute) {
    switch (attribute) {
      case Constants.LENGTH:
        return getLength();
      case Constants.YEAR:
        return getYear();
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

  public Integer getYear() {
    return f5;
  }

  public void setYear(Integer year) {
    f5 = year;
  }

  public Integer getLength() {
    return f6;
  }

  public void setLength(Integer length) {
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
