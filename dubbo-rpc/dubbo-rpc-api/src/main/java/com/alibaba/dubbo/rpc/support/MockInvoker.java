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
package com.alibaba.dubbo.rpc.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.PojoUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.fastjson.JSON;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final public class MockInvoker<T> implements Invoker<T> {
    /**
     * ProxyFactory$Adaptive对象
     */
    private final static ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    /**
     * mock与Invoker对象的映射缓存
     */
    private final static Map<String, Invoker<?>> mocks = new ConcurrentHashMap<String, Invoker<?>>();
    /**
     * mock与Throwable对象的映射缓存
     */
    private final static Map<String, Throwable> throwables = new ConcurrentHashMap<String, Throwable>();
    /**
     * URL对象
     */
    private final URL url;

    public MockInvoker(URL url) {
        this.url = url;
    }

    public static Object parseMockValue(String mock) throws Exception {
        return parseMockValue(mock, null);
    }

    public static Object parseMockValue(String mock, Type[] returnTypes) throws Exception {
        Object value = null;
        if ("empty".equals(mock)) {
            value = ReflectUtils.getEmptyObject(returnTypes != null && returnTypes.length > 0 ? (Class<?>) returnTypes[0] : null);
        } else if ("null".equals(mock)) {
            value = null;
        } else if ("true".equals(mock)) {
            value = true;
        } else if ("false".equals(mock)) {
            value = false;
        } else if (mock.length() >= 2 && (mock.startsWith("\"") && mock.endsWith("\"")
                || mock.startsWith("\'") && mock.endsWith("\'"))) {
            value = mock.subSequence(1, mock.length() - 1);
        } else if (returnTypes != null && returnTypes.length > 0 && returnTypes[0] == String.class) {
            value = mock;
        } else if (StringUtils.isNumeric(mock)) {
            value = JSON.parse(mock);
        } else if (mock.startsWith("{")) {
            value = JSON.parseObject(mock, Map.class);
        } else if (mock.startsWith("[")) {
            value = JSON.parseObject(mock, List.class);
        } else {
            value = mock;
        }
        if (returnTypes != null && returnTypes.length > 0) {
            value = PojoUtils.realize(value, (Class<?>) returnTypes[0], returnTypes.length > 1 ? returnTypes[1] : null);
        }
        return value;
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        //获得"mock"配置项，方法级 > 类级
        String mock = getUrl().getParameter(invocation.getMethodName() + "." + Constants.MOCK_KEY);
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(this);
        }
        if (StringUtils.isBlank(mock)) {
            mock = getUrl().getParameter(Constants.MOCK_KEY);
        }

        if (StringUtils.isBlank(mock)) {//不允许为空
            throw new RpcException(new IllegalAccessException("mock can not be null. url :" + url));
        }
        //标准化"mock"配置项
        mock = normallizeMock(URL.decode(mock));
        //等于"return",返回值为空的RpcResult对象
        if (Constants.RETURN_PREFIX.trim().equalsIgnoreCase(mock.trim())) {
            RpcResult result = new RpcResult();
            result.setValue(null);
            return result;
            // 以 "return " 开头，返回对应值的 RpcResult 对象
        } else if (mock.startsWith(Constants.RETURN_PREFIX)) {
            mock = mock.substring(Constants.RETURN_PREFIX.length()).trim();
            mock = mock.replace('`', '"');
            try {
                // 解析返回类型
                Type[] returnTypes = RpcUtils.getReturnTypes(invocation);
                // 解析返回值
                Object value = parseMockValue(mock, returnTypes);
                // 创建对应值的 RpcResult 对象，并返回
                return new RpcResult(value);
            } catch (Exception ew) {
                throw new RpcException("mock return invoke error. method :" + invocation.getMethodName() + ", mock:" + mock + ", url: " + url, ew);
            }
            // 以 "throw" 开头，抛出 RpcException 异常
        } else if (mock.startsWith(Constants.THROW_PREFIX)) {
            mock = mock.substring(Constants.THROW_PREFIX.length()).trim();
            mock = mock.replace('`', '"');
            if (StringUtils.isBlank(mock)) {// 禁止为空
                throw new RpcException(" mocked exception for Service degradation. ");
            } else { // user customized class
                // 创建自定义异常
                Throwable t = getThrowable(mock);
                // 抛出业务类型的 RpcException 异常
                throw new RpcException(RpcException.BIZ_EXCEPTION, t);
            }
            // 自定义 Mock 类，执行自定义逻辑
        } else { //impl mock
            try {
                // 创建 Invoker 对象
                Invoker<T> invoker = getInvoker(mock);
                // 执行 Invoker 对象的调用逻辑
                return invoker.invoke(invocation);
            } catch (Throwable t) {
                throw new RpcException("Failed to create mock implemention class " + mock, t);
            }
        }
    }

    private Throwable getThrowable(String throwstr) {
        Throwable throwable = (Throwable) throwables.get(throwstr);
        if (throwable != null) {
            return throwable;
        } else {
            Throwable t = null;
            try {
                Class<?> bizException = ReflectUtils.forName(throwstr);
                Constructor<?> constructor;
                constructor = ReflectUtils.findConstructor(bizException, String.class);
                t = (Throwable) constructor.newInstance(new Object[]{" mocked exception for Service degradation. "});
                if (throwables.size() < 1000) {
                    throwables.put(throwstr, t);
                }
            } catch (Exception e) {
                throw new RpcException("mock throw error :" + throwstr + " argument error.", e);
            }
            return t;
        }
    }

    @SuppressWarnings("unchecked")
    private Invoker<T> getInvoker(String mockService) {
        Invoker<T> invoker = (Invoker<T>) mocks.get(mockService);
        if (invoker != null) {
            return invoker;
        } else {
            Class<T> serviceType = (Class<T>) ReflectUtils.forName(url.getServiceInterface());
            if (ConfigUtils.isDefault(mockService)) {
                mockService = serviceType.getName() + "Mock";
            }

            Class<?> mockClass = ReflectUtils.forName(mockService);
            if (!serviceType.isAssignableFrom(mockClass)) {
                throw new IllegalArgumentException("The mock implemention class " + mockClass.getName() + " not implement interface " + serviceType.getName());
            }

            if (!serviceType.isAssignableFrom(mockClass)) {
                throw new IllegalArgumentException("The mock implemention class " + mockClass.getName() + " not implement interface " + serviceType.getName());
            }
            try {
                T mockObject = (T) mockClass.newInstance();
                invoker = proxyFactory.getInvoker(mockObject, (Class<T>) serviceType, url);
                if (mocks.size() < 10000) {
                    mocks.put(mockService, invoker);
                }
                return invoker;
            } catch (InstantiationException e) {
                throw new IllegalStateException("No such empty constructor \"public " + mockClass.getSimpleName() + "()\" in mock implemention class " + mockClass.getName(), e);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    //mock=fail:throw
    //mock=fail:return
    //mock=xx.Service
    private String normallizeMock(String mock) {
        if (mock == null || mock.trim().length() == 0) {
            return mock;
        } else if (ConfigUtils.isDefault(mock) || "fail".equalsIgnoreCase(mock.trim()) || "force".equalsIgnoreCase(mock.trim())) {
            mock = url.getServiceInterface() + "Mock";
        }
        if (mock.startsWith(Constants.FAIL_PREFIX)) {
            mock = mock.substring(Constants.FAIL_PREFIX.length()).trim();
        } else if (mock.startsWith(Constants.FORCE_PREFIX)) {
            mock = mock.substring(Constants.FORCE_PREFIX.length()).trim();
        }
        return mock;
    }

    @Override
    public URL getUrl() {
        return this.url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
        //do nothing
    }

    @Override
    public Class<T> getInterface() {
        //FIXME
        return null;
    }
}
