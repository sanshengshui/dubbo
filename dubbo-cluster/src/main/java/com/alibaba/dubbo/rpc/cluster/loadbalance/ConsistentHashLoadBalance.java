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

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ConsistentHashLoadBalance
 *
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {

    //接口名，方法名称为key  hash选择器为value的map.每个方法一个选择器。
    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //接口名.方法名称为key
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        //取的invokers对象的hashcode,验证对象变化
        int identityHashCode = System.identityHashCode(invokers);
        //根据key获取hash选择器
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        if (selector == null || selector.identityHashCode != identityHashCode) {
            //选择器为null或者，对象已变化，就创建新选择器放入map中
            selectors.put(key, new ConsistentHashSelector<T>(invokers, invocation.getMethodName(), identityHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        //通过选择器的select方法，返回选中的invoker
        return selector.select(invocation);
    }

    private static final class ConsistentHashSelector<T> {
        //hash 环（值域）中，某些值(所有虚拟节点数)到虚拟节点的映射。
        private final TreeMap<Long, Invoker<T>> virtualInvokers;
        //每个invoker 需要虚拟的节点数
        private final int replicaNumber;

        private final int identityHashCode;

        private final int[] argumentIndex;

        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            //基于红黑树实现的有序map,有序很重要。
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();
            //获取虚拟节点数，默认160个节点，配置例子 <dubbo:parameter key="hash.nodes" value="320" />
            this.replicaNumber = url.getMethodParameter(methodName, "hash.nodes", 160);
            //获取需要hash的参数位置。配置例子<dubbo:parameter key="hash.arguments" value="0,1" /> 默认只hash第一个，0位置参数
            String[] index = Constants.COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, "hash.arguments", "0"));
            argumentIndex = new int[index.length];
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            for (Invoker<T> invoker : invokers) {
                //获取提供者host:port形式地址，以160个虚拟节点为例
                String address = invoker.getUrl().getAddress();
                for (int i = 0; i < replicaNumber / 4; i++) {
                    // 这组虚拟结点得到惟一名称
                    byte[] digest = md5(address + i);//host:port(0,1,2,3.....39),40份
                    //每个再分别，hash 4次，（这样每个机器就虚拟了160份）
                    for (int h = 0; h < 4; h++) {
                        long m = hash(digest, h);
                        //160份虚拟节点，每一份都映射同一个实际节点
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        public Invoker<T> select(Invocation invocation) {
            String key = toKey(invocation.getArguments());
            //对拼接后的参数做MD5指纹摘要
            byte[] digest = md5(key);
            //摘要后，hash计算
            return selectForKey(hash(digest, 0));
        }

        /***
         * 把参数直接拼接。
         * @param args
         * @return
         */
        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        //根据hash值，选择invoker方法的核心方法
        private Invoker<T> selectForKey(long hash) {
            // 得到大于当前 key 的那个子 Map ，然后从中取出第一个 key ，就是大于且离它最近的那个 key
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.tailMap(hash, true).firstEntry();
            // 不存在，则取 virtualInvokers 第一个
            if (entry == null) {
        		entry = virtualInvokers.firstEntry();
        	}
            // 存在，则返回
        	return entry.getValue();
        }

        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }

        private byte[] md5(String value) {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.reset();
            byte[] bytes;
            try {
                bytes = value.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.update(bytes);
            return md5.digest();
        }

    }

}
