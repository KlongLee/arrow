/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.VectorLayout;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.Iterators;

import io.netty.buffer.ArrowBuf;

/**
 * Loads buffers into vectors
 */
public class VectorLoader {
  private final List<FieldVector> fieldVectors;
  private final List<Field> fields;

  /**
   * will create children in root based on schema
   * @param schema the expected schema
   * @param root the root to add vectors to based on schema
   */
  public VectorLoader(Schema schema, FieldVector root) {
    super();
    this.fields = schema.getFields();
    root.initializeChildrenFromFields(fields);
    this.fieldVectors = root.getChildrenFromFields();
    if (this.fieldVectors.size() != fields.size()) {
      throw new IllegalArgumentException("The root vector did not create the right number of children. found " + fieldVectors.size() + " expected " + fields.size());
    }
  }

  /**
   * Loads the record batch in the vectors
   * will not close the record batch
   * @param recordBatch
   */
  public void load(ArrowRecordBatch recordBatch) {
    Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
    Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
    for (int i = 0; i < fields.size(); ++i) {
      Field field = fields.get(i);
      FieldVector fieldVector = fieldVectors.get(i);
      loadBuffers(fieldVector, field, buffers, nodes);
    }
    if (nodes.hasNext() || buffers.hasNext()) {
      throw new IllegalArgumentException("not all nodes and buffers where consumed. nodes: " + Iterators.toString(nodes) + " buffers: " + Iterators.toString(buffers));
    }
  }

  private void loadBuffers(FieldVector vector, Field field, Iterator<ArrowBuf> buffers, Iterator<ArrowFieldNode> nodes) {
    ArrowFieldNode fieldNode = nodes.next();
    List<VectorLayout> typeLayout = field.getTypeLayout().getVectors();
    List<ArrowBuf> ownBuffers = new ArrayList<>(typeLayout.size());
    for (int j = 0; j < typeLayout.size(); j++) {
      ownBuffers.add(buffers.next());
    }
    try {
      vector.loadFieldBuffers(fieldNode, ownBuffers);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not load buffers for field " + field);
    }
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      checkArgument(children.size() == childrenFromFields.size(), "should have as many children as in the schema: found " + childrenFromFields.size() + " expected " + children.size());
      for (int i = 0; i < childrenFromFields.size(); i++) {
        Field child = children.get(i);
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes);
      }
    }
  }
}
