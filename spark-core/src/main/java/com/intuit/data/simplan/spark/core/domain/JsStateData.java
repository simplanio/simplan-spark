/*
 *  Copyright 2024, Intuit Inc
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.intuit.data.simplan.spark.core.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Abraham, Thomas - tabraham1
 * Created on 26-Oct-2024 at 1:39 AM
 */
public class JsStateData implements Serializable {
    Map<String, List<String>> lists;
    Map<String, Long> counters;

    public JsStateData() {
    }
    public JsStateData(List<String> lists,List<String> counters) {
        this.lists = new HashMap<>();
        lists.forEach(list -> this.lists.put(list, new ArrayList<>()));
        this.counters = new HashMap<>();
        counters.forEach(counter -> this.counters.put(counter, 0L));
    }


    public Map<String, List<String>> getLists() {
        return lists;
    }

    public JsStateData setLists(Map<String, List<String>> lists) {
        this.lists = lists;
        return this;
    }

    public Map<String, Long> getCounters() {
        return counters;
    }

    public JsStateData setCounters(Map<String, Long> counters) {
        this.counters = counters;
        return this;
    }

    public boolean updateCounter(String counterKey, Long value) {
        counters.put(counterKey, value);
        return true;
    }
}
