/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.AtomicPositiveInteger;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Round robin load balance.
 *
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";

    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //key表示方法，例如com.alibaba.dubbo.demo.TestService.getRandomNumber.即负载均衡算法细化到每个方法的调用；
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        int length = invokers.size(); // Number of invokers provider的总个数
        int maxWeight = 0; // The maximum weight 最大权重，只有一个临时值，所以设置为0，当遍历invoker时接口的wight肯定大于0，马上就会替换成一个真实的maxWeight的值；
        int minWeight = Integer.MAX_VALUE; // The minimum weight 最小权重，只是一个临时值, 所以设置为Integer类型最大值, 当遍历invokers时接口的weight肯定小于这个数，马上就会替换成一个真实的minWeight的值；
        final LinkedHashMap<Invoker<T>, IntegerWrapper> invokerToWeightMap = new LinkedHashMap<Invoker<T>, IntegerWrapper>();
        int weightSum = 0;
        // 计算最小、最大权重，总的权重和。
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            maxWeight = Math.max(maxWeight, weight); // Choose the maximum weight
            minWeight = Math.min(minWeight, weight); // Choose the minimum weight
            if (weight > 0) {
                invokerToWeightMap.put(invokers.get(i), new IntegerWrapper(weight));
                weightSum += weight;
            }
        }
        //获得AtomicPositiveInteger 对象
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        // 获得当前顺序号，并递增 + 1
        int currentSequence = sequence.getAndIncrement();
        //权重不相等，顺序根据权重分配
        if (maxWeight > 0 && minWeight < maxWeight) {
            int mod = currentSequence % weightSum;//剩余权重
            for (int i = 0; i < maxWeight; i++) {//循环最大权重
                for (Map.Entry<Invoker<T>, IntegerWrapper> each : invokerToWeightMap.entrySet()) {// 循环 Invoker 集合
                    final Invoker<T> k = each.getKey();
                    final IntegerWrapper v = each.getValue();
                    // 剩余权重归 0 ，当前 Invoker 还有剩余权重，返回该 Invoker 对象
                    if (mod == 0 && v.getValue() > 0) {
                        return k;
                    }
                    if (v.getValue() > 0) {
                        //基于mod,来说之前选过的，权重-1.mod--
                        v.decrement();
                        mod--;
                    }
                }
            }
        }
        // Round robin
        return invokers.get(currentSequence % length);
    }

    private static final class IntegerWrapper {
        private int value;

        public IntegerWrapper(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void decrement() {
            this.value--;
        }
    }

}
