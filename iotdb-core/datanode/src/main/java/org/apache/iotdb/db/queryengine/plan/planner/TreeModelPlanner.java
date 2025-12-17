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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.scheduler.ClusterScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class TreeModelPlanner implements IPlanner {

  private final Statement statement;

  private final ExecutorService executor;
  private final ExecutorService writeOperationExecutor;
  private final ScheduledExecutorService scheduledExecutor;

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      syncInternalServiceClientManager;

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;

  public TreeModelPlanner(
      Statement statement,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      ScheduledExecutorService scheduledExecutor,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    this.statement = statement;
    this.executor = executor;
    this.writeOperationExecutor = writeOperationExecutor;
    this.scheduledExecutor = scheduledExecutor;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
    this.syncInternalServiceClientManager = syncInternalServiceClientManager;
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
  }

  @Override
  public IAnalysis analyze(MPPQueryContext context) {
    return new Analyzer(context, partitionFetcher, schemaFetcher).analyze(statement);
  }

  @Override
  public LogicalQueryPlan doLogicalPlan(IAnalysis analysis, MPPQueryContext context) {
    LogicalPlanner logicalPlanner = new LogicalPlanner(context);
    return logicalPlanner.plan((Analysis) analysis);
  }

  @Override
  public DistributedQueryPlan doDistributionPlan(IAnalysis analysis, LogicalQueryPlan logicalPlan) {
    DistributionPlanner planner = new DistributionPlanner((Analysis) analysis, logicalPlan);
    return planner.planFragments();
  }

  @Override
  /**
   * 实现IPlanner接口的doSchedule方法，用于创建并启动查询调度器
   * 根据不同的语句类型选择合适的调度器实现
   *
   * @param analysis 查询分析结果，包含查询的元数据和执行信息
   * @param distributedPlan 分布式查询计划，包含查询的执行片段和实例信息
   * @param context MPP查询上下文，包含查询ID、执行参数等
   * @param stateMachine 查询状态机，用于管理查询的执行状态
   * @return 已启动的查询调度器实例
   */
  public IScheduler doSchedule(
      IAnalysis analysis,
      DistributedQueryPlan distributedPlan,
      MPPQueryContext context,
      QueryStateMachine stateMachine) {
    // 声明查询调度器变量
    IScheduler scheduler;

    // 检查是否是管道增强的LoadTsFile语句
    // PipeEnrichedStatement是一种包装语句，用于支持管道功能
    boolean isPipeEnrichedTsFileLoad = 
        statement instanceof PipeEnrichedStatement
            && ((PipeEnrichedStatement) statement).getInnerStatement()
                instanceof LoadTsFileStatement;
    
    // 判断语句类型，选择合适的调度器
    if (statement instanceof LoadTsFileStatement || isPipeEnrichedTsFileLoad) {
      // 如果是LoadTsFile语句或管道增强的LoadTsFile语句，使用LoadTsFileScheduler
      scheduler = 
          new LoadTsFileScheduler(
              distributedPlan, // 分布式查询计划
              context, // 查询上下文
              stateMachine, // 查询状态机
              syncInternalServiceClientManager, // 同步内部服务客户端管理器
              partitionFetcher, // 分区获取器
              isPipeEnrichedTsFileLoad); // 是否是管道增强的LoadTsFile语句
    } else {
      // 否则使用通用的ClusterScheduler
      scheduler = 
          new ClusterScheduler(
              context, // 查询上下文
              stateMachine, // 查询状态机
              distributedPlan.getInstances(), // 查询计划实例列表
              context.getQueryType(), // 查询类型（读/写）
              executor, // 通用执行器
              writeOperationExecutor, // 写操作执行器
              scheduledExecutor, // 定时执行器
              syncInternalServiceClientManager, // 同步内部服务客户端管理器
              asyncInternalServiceClientManager); // 异步内部服务客户端管理器
    }

    // 启动调度器
    scheduler.start();
    // 返回已启动的调度器实例
    return scheduler;
  }

  @Override
  public void invalidatePartitionCache() {
    partitionFetcher.invalidAllCache();
  }

  @Override
  public ScheduledExecutorService getScheduledExecutorService() {
    return scheduledExecutor;
  }

  @Override
  public void setRedirectInfo(
      IAnalysis iAnalysis, TEndPoint localEndPoint, TSStatus tsstatus, TSStatusCode statusCode) {
    Analysis analysis = (Analysis) iAnalysis;

    // Get the inner statement of PipeEnrichedStatement
    Statement statementToRedirect =
        analysis.getStatement() instanceof PipeEnrichedStatement
            ? ((PipeEnrichedStatement) analysis.getStatement()).getInnerStatement()
            : analysis.getStatement();

    if (statementToRedirect instanceof InsertBaseStatement
        && !analysis.isFinishQueryAfterAnalyze()) {
      InsertBaseStatement insertStatement = (InsertBaseStatement) statementToRedirect;
      List<TEndPoint> redirectNodeList = analysis.getRedirectNodeList();
      if (insertStatement instanceof InsertRowsStatement
          || insertStatement instanceof InsertMultiTabletsStatement) {
        // multiple devices
        if (statusCode == TSStatusCode.SUCCESS_STATUS) {
          boolean needRedirect = false;
          List<TSStatus> subStatus = new ArrayList<>();
          for (TEndPoint endPoint : redirectNodeList) {
            // redirect writing only if the redirectEndPoint is not the current node
            if (!localEndPoint.equals(endPoint)) {
              subStatus.add(
                  RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS).setRedirectNode(endPoint));
              needRedirect = true;
            } else {
              subStatus.add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            }
          }
          if (needRedirect) {
            tsstatus.setCode(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
            tsstatus.setSubStatus(subStatus);
          }
        }
      } else {
        // single device
        TEndPoint redirectEndPoint = redirectNodeList.get(0);
        // redirect writing only if the redirectEndPoint is not the current node
        if (!localEndPoint.equals(redirectEndPoint)) {
          tsstatus.setRedirectNode(redirectEndPoint);
        }
      }
    }
  }
}