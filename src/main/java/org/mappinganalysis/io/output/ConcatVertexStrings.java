/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.mappinganalysis.io.output;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * concatenates the sorted string representations of graph heads to represent a
 * collection
 */
public class ConcatVertexStrings implements GroupReduceFunction<VertexString, String> {

  @Override
  public void reduce(Iterable<VertexString> vertexStrings,
    Collector<String> collector) throws Exception {

    List<String> vertexStringList = new ArrayList<>();

    for (VertexString vertexString : vertexStrings) {
      String vertexLabel = vertexString.getLabel();
      if (! vertexLabel.equals("")) {
        vertexStringList.add(vertexLabel);
      }
    }

    Collections.sort(vertexStringList);

    collector.collect(StringUtils.join(vertexStringList, "\n"));
  }
}
