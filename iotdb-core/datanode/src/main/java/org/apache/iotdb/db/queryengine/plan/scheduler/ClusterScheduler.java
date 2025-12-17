/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.WAIT_FOR_DISPATCH;

/**
 * QueryScheduler is used to dispatch the fragment instances of a query to target nodes. And it will
 * continue to collect and monitor the query execution before the query is finished.
 *
 * <p>Later, we can add more control logic for a QueryExecution such as retry, kill and so on by
 * this scheduler.
 */
/**
 * ClusterScheduler是IoTDB分布式查询执行引擎的核心调度器实现
 * 负责将查询片段实例分发到目标节点，并监控查询执行状态
 * 支持查询重试、终止等控制逻辑
 */
public class ClusterScheduler implements IScheduler {
  private static final Logger logger = LoggerFactory.getLogger(ClusterScheduler.class);

  // The stateMachine of the QueryExecution owned by this QueryScheduler
  /** 查询执行的状态机，用于管理查询的生命周期状态 */
  private final QueryStateMachine stateMachine;
  
  /** 查询类型（读/写） */
  private final QueryType queryType;
  
  // The fragment instances which should be sent to corresponding Nodes.
  /** 需要发送到对应节点的查询片段实例列表 */
  private final List<FragmentInstance> instances;

  /** 查询片段实例分发器，负责将实例发送到目标节点 */
  private final IFragInstanceDispatcher dispatcher;
  
  /** 查询片段实例状态跟踪器，用于监控实例执行状态 */
  private IFragInstanceStateTracker stateTracker;
  
  /** 查询终止器，用于在需要时终止查询执行 */
  private IQueryTerminator queryTerminator;

  /** 查询执行指标集单例，用于收集查询执行相关性能指标 */
  private static final QueryExecutionMetricSet QUERY_EXECUTION_METRICS =
      QueryExecutionMetricSet.getInstance();

  /**
   * 构造函数，初始化ClusterScheduler实例
   *
   * @param queryContext MPP查询上下文，包含查询ID、执行参数等
   * @param stateMachine 查询状态机，用于管理查询生命周期状态
   * @param instances 需要分发的查询片段实例列表
   * @param queryType 查询类型（读/写）
   * @param executor 通用执行器
   * @param writeOperationExecutor 写操作专用执行器
   * @param scheduledExecutor 定时执行器，用于定期任务
   * @param syncInternalServiceClientManager 同步内部服务客户端管理器
   * @param asyncInternalServiceClientManager 异步内部服务客户端管理器
   */
  public ClusterScheduler(
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      List<FragmentInstance> instances,
      QueryType queryType,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      ScheduledExecutorService scheduledExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    // 初始化核心成员变量
    this.stateMachine = stateMachine;
    this.instances = instances;
    this.queryType = queryType;
    
    // 创建查询片段实例分发器实现
    this.dispatcher =
        new FragmentInstanceDispatcherImpl(
            queryType,
            queryContext,
            executor,
            writeOperationExecutor,
            syncInternalServiceClientManager,
            asyncInternalServiceClientManager);
    
    // 对于读查询，创建状态跟踪器和查询终止器
    if (queryType == QueryType.READ) {
      // 创建固定频率的片段实例状态跟踪器
      this.stateTracker =
          new FixedRateFragInsStateTracker(
              stateMachine,
              scheduledExecutor,
              instances,
              syncInternalServiceClientManager);
      
      // 创建简单查询终止器
      this.queryTerminator =
          new SimpleQueryTerminator(
              scheduledExecutor,
              queryContext,
              instances,
              syncInternalServiceClientManager,
              stateTracker);
    }
  }

  /**
   * 判断是否需要重试失败的查询
   *
   * @param failureStatus 失败状态码
   * @return 是否需要重试
   */
  private boolean needRetry(TSStatus failureStatus) {
    // 只有读查询在特定错误码下需要重试
    return failureStatus != null
        && queryType == QueryType.READ
        && (failureStatus.getCode() == TSStatusCode.DISPATCH_ERROR.getStatusCode()
            // 分发错误
            || failureStatus.getCode()
                == TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode());
                // 并发查询过多错误
  }

  /**
   * 启动查询调度流程
   * 实现IScheduler接口的start方法
   */
  @Override
  public void start() {
    // 将查询状态转换为正在分发状态
    stateMachine.transitionToDispatching();
    
    // 记录开始分发时间，用于性能指标统计
    long startTime = System.nanoTime();
    
    // 异步分发查询片段实例
    Future<FragInstanceDispatchResult> dispatchResultFuture = dispatcher.dispatch(instances);

    // NOTICE: the FragmentInstance may be dispatched to another Host due to consensus redirect.
    // So we need to start the state fetcher after the dispatching stage.
    try {
      // 等待分发结果
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      
      // 检查分发是否成功
      if (!result.isSuccessful()) {
        // 如果失败，判断是否需要重试
        if (needRetry(result.getFailureStatus())) {
          // 转换为等待重试状态
          stateMachine.transitionToPendingRetry(result.getFailureStatus());
        } else {
          // 否则转换为失败状态
          stateMachine.transitionToFailed(result.getFailureStatus());
        }
        return;
      }
    } catch (InterruptedException | ExecutionException e) {
      // If the dispatch request cannot be sent or TException is caught, we will retry this query.
      // 处理分发过程中的中断或执行异常
      if (e instanceof InterruptedException) {
        // 恢复中断状态
        Thread.currentThread().interrupt();
      }
      // 转换为失败状态
      stateMachine.transitionToFailed(e);
      return;
    } finally {
      // 记录分发等待时间指标
      QUERY_EXECUTION_METRICS.recordExecutionCost(WAIT_FOR_DISPATCH, System.nanoTime() - startTime);
    }

    // For the FragmentInstance of WRITE, it will be executed directly when dispatching.
    // 对于写查询，片段实例在分发时已直接执行完成
    if (queryType == QueryType.WRITE) {
      // 转换为已完成状态
      stateMachine.transitionToFinished();
      return;
    }

    // The FragmentInstances has been dispatched successfully to corresponding host, we mark the
    // QueryState to Running
    // 读查询片段实例已成功分发到对应节点，将查询状态转换为运行中
    stateMachine.transitionToRunning();

    // TODO: (xingtanzjr) start the stateFetcher/heartbeat for each fragment instance
    // 启动片段实例状态跟踪器，监控查询执行状态
    this.stateTracker.start();
    logger.debug("state tracker starts");
  }

  /**
   * 停止查询执行
   * 实现IScheduler接口的stop方法
   *
   * @param t 停止查询的原因异常
   */
  @Override
  public void stop(Throwable t) {
    // TODO: It seems that it is unnecessary to check whether they are null or not. Is it a best
    // practice ?
    // 终止分发器
    dispatcher.abort();
    
    // 终止状态跟踪器（仅读查询有）
    if (stateTracker != null) {
      stateTracker.abort();
    }
    
    // TODO: (xingtanzjr) handle the exception when the termination cannot succeed
    // 终止查询（仅读查询有）
    if (queryTerminator != null) {
      queryTerminator.terminate(t);
    }
  }

  /**
   * 获取查询总CPU时间
   * 实现IScheduler接口的方法，当前未实现
   *
   * @return 查询总CPU时间（当前返回null）
   */
  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  /**
   * 获取查询片段信息
   * 实现IScheduler接口的方法，当前未实现
   *
   * @return 查询片段信息（当前返回null）
   */
  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }
}