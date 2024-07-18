/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intuit.data.simplan.spark.core.events;

import java.util.Set;

/**
 * @author Abraham, Thomas - tabraham1
 * Created on 02-Jan-2024 at 12:18PM
 */
public class CountDistinctByColumns {
    Long count;
    Set<String> columns;

    public Long getCount() {
        return count;
    }

    public CountDistinctByColumns setCount(Long count) {
        this.count = count;
        return this;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public CountDistinctByColumns addColumn(String column) {
        if(this.columns == null) {
            this.columns = new java.util.HashSet<>();
        }
        this.columns.add(column);
        return this;
    }

    public CountDistinctByColumns setColumns(Set<String> columns) {
        this.columns = columns;
        return this;
    }
}
