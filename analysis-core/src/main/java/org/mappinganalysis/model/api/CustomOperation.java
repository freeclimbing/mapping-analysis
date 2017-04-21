package org.mappinganalysis.model.api;

/**
 * Created by markus on 4/13/17.
 */
public interface CustomOperation<T> {

  void setInput(T inputData);

  T createResult();
}
