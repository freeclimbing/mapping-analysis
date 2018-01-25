package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

/**
 * Basic link filter
 */
class BasicLinkFilter extends LinkFilter {
  BasicLinkFilter(List<String> sources, Boolean removeIsolatedVertices,
                  ExecutionEnvironment env) {
    super(new BasicLinkFilterFunction(sources, removeIsolatedVertices, env));
  }
}
