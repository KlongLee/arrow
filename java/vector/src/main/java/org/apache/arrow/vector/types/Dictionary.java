/*******************************************************************************

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
 ******************************************************************************/
package org.apache.arrow.vector.types;

import org.apache.arrow.vector.ValueVector;

public class Dictionary {

    private long id;
    private ValueVector dictionary;
    private boolean ordered;

    public Dictionary(long id, ValueVector dictionary, boolean ordered) {
        this.id = id;
        this.dictionary = dictionary;
        this.ordered = ordered;
    }

    public long getId() {
        return id;
    }

    public ValueVector getDictionary() {
        return dictionary;
    }

    public boolean isOrdered() {
        return ordered;
    }
}
