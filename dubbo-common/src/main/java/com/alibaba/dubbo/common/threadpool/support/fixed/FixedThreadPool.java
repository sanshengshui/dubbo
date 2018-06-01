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
package com.alibaba.dubbo.common.threadpool.support.fixed;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.threadlocal.NamedInternalThreadFactory;
import com.alibaba.dubbo.common.threadpool.ThreadPool;
import com.alibaba.dubbo.common.threadpool.support.AbortPolicyWithReport;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Creates a thread pool that reuses a fixed number of threads
 *
 * @see java.util.concurrent.Executors#newFixedThreadPool(int)
 */
public class FixedThreadPool implements ThreadPool {

    @Override
    public Executor getExecutor(URL url) {
        //线程名
        String name = url.getParameter(Constants.THREAD_NAME_KEY, Constants.DEFAULT_THREAD_NAME);
        //线程数
        int threads = url.getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
        //队列数
        int queues = url.getParameter(Constants.QUEUES_KEY, Constants.DEFAULT_QUEUES);
        //创建执行器
        return new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
                /**
                 * 根据不同的队列数,使用不同的队列实现:
                 * queues == 0,SynchronousQueue对象。
                 * queues < 0,LinkedBlockingQueue对象。
                 * queues > 0,带队列数的LinkedBlockingQueue对象。
                 */
                queues == 0 ? new SynchronousQueue<Runnable>() :
                        (queues < 0 ? new LinkedBlockingQueue<Runnable>()
                                : new LinkedBlockingQueue<Runnable>(queues)),
                /**
                 * 创建NamedThreadFactory对象，用于生成线程名
                 * 创建AbortPolicyWithReport对象，用于当任务添加到线程池中被拒绝时。
                 */
                new NamedInternalThreadFactory(name, true), new AbortPolicyWithReport(name, url));
    }

}
