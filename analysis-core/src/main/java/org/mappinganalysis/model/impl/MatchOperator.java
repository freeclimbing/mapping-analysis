package org.mappinganalysis.model.impl;

/**
 * Created by markus on 11/14/16.
 */
public abstract class MatchOperator<T> {

  String property;

  public void setProperty(String property) {
    this.property = property;
  }
}
