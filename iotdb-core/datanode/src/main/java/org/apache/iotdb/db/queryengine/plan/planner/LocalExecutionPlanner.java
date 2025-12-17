/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.metric.QueryRelatedResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceType;
import org.apache.iotdb.db.utils.SetThreadName;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Used to plan a fragment instance. One fragment instance could be split into multiple pipelines so
 * that a fragment instance could be run in parallel, and thus we can take full advantages of
 * multi-cores.
 */
public class LocalExecutionPlanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanner.class);
  private static final long ALLOCATE_MEMORY_FOR_OPERATORS;
  private static final long MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;

  static {
    IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
    ALLOCATE_MEMORY_FOR_OPERATORS = CONFIG.getAllocateMemoryForOperators();
    MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD =
        (long)
            ((ALLOCATE_MEMORY_FOR_OPERATORS) * (1.0 - CONFIG.getMaxAllocateMemoryRatioForLoad()));
  }

  /** allocated memory for operator execution */
  private long freeMemoryForOperators = ALLOCATE_MEMORY_FOR_OPERATORS;

  public long getFreeMemoryForOperators() {
    return freeMemoryForOperators;
  }

  public long getFreeMemoryForLoadTsFile() {
    return freeMemoryForOperators - MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
  }

  public static LocalExecutionPlanner getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public List<PipelineDriverFactory> plan(
      PlanNode plan,
      TypeProvider types,
      FragmentInstanceContext instanceContext,
      DataNodeQueryContext dataNodeQueryContext)
      throws MemoryNotEnoughException {
    LocalExecutionPlanContext context =
        new LocalExecutionPlanContext(types, instanceContext, dataNodeQueryContext);

    // Generate pipelines, return the last pipeline data structure
    // TODO Replace operator with operatorFactory to build multiple driver for one pipeline
    Operator root = plan.accept(new OperatorTreeGenerator(), context);

    PipelineMemoryEstimator memoryEstimator =
        context.constructPipelineMemoryEstimator(root, null, plan, -1);
    // set the map to null for gc
    context.invalidateParentPlanNodeIdToMemoryEstimator();

    // check whether current free memory is enough to execute current query
    long estimatedMemorySize = checkMemory(memoryEstimator, instanceContext.getStateMachine());

    context.addPipelineDriverFactory(root, context.getDriverContext(), estimatedMemorySize);

    instanceContext.setSourcePaths(collectSourcePaths(context));
    instanceContext.setDevicePathsToContext(collectDevicePathsToContext(context));
    instanceContext.setQueryDataSourceType(
        getQueryDataSourceType((DataDriverContext) context.getDriverContext()));

    context.getTimePartitions().ifPresent(instanceContext::setTimePartitions);

    // set maxBytes one SourceHandle can reserve after visiting the whole tree
    context.setMaxBytesOneHandleCanReserve();

    return context.getPipelineDriverFactories();
  }

  /**
   * 将查询计划节点转换为可执行的管道驱动工厂列表
   * 
   * 该方法是将逻辑查询计划转换为物理执行计划的核心方法，主要功能包括：
   * 1. 创建本地执行计划上下文
   * 2. 使用OperatorTreeGenerator将PlanNode转换为Operator
   * 3. 估算内存使用量并进行内存检查
   * 4. 创建管道驱动工厂
   * 5. 配置执行参数
   * 
   * 示例查询：SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10
   * 在这个查询中，该方法负责将查询计划转换为具体的执行操作符
   * 
   * 表1：plan方法执行流程
   * | 步骤 | 功能描述 | 关键数据结构变化 | 示例（SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10） |
   * |------|---------|-----------------|----------------------------------------------------------|
   * | 1. 上下文创建 | 创建执行上下文 | FragmentInstanceContext → LocalExecutionPlanContext | 包含查询参数、内存分配等执行环境 |
   * | 2. 操作符生成 | 转换计划节点为操作符 | PlanNode树 → Operator树 | SeriesScanNode → SeriesScanOperator等 |
   * | 3. 内存估算 | 估算执行所需内存 | 构建PipelineMemoryEstimator | 估算扫描、过滤、投影操作的内存需求 |
   * | 4. 内存检查 | 检查内存是否足够 | 验证内存分配 | 确保查询执行不会导致内存不足 |
   * | 5. 管道创建 | 创建管道驱动工厂 | Operator → PipelineDriverFactory | 创建执行管道，支持并行处理 |
   * | 6. 参数配置 | 配置执行参数 | 设置最大字节数等 | 优化数据传输和内存使用 |
   * 
   * 表2：查询计划转换过程中的关键组件
   * | 组件 | 输入类型 | 输出类型 | 功能描述 | 示例转换 |
   * |------|---------|---------|---------|---------|
   * | PlanNode | 逻辑计划节点 | - | 查询计划的逻辑表示 | SeriesScanNode、FilterNode等 |
   * | OperatorTreeGenerator | PlanNode | Operator | 计划节点到操作符的转换器 | 生成SeriesScanOperator等 |
   * | Operator | 物理操作符 | - | 具体的执行单元 | 执行数据扫描、过滤等操作 |
   * | PipelineDriverFactory | Operator | IDriver | 创建执行驱动器的工厂 | 生成并行执行线程 |
   * | LocalExecutionPlanContext | 执行上下文 | - | 管理执行环境和参数 | 包含内存分配、路径信息等 |
   * 
   * 示例执行流程详细说明：
   * 1. 输入：包含SeriesScanNode、FilterNode、ProjectNode、IdentitySinkNode的PlanNode树
   * 2. 转换：通过OperatorTreeGenerator访问者模式，依次调用visitSeriesScan、visitFilter、visitProject、visitIdentitySink
   * 3. 输出：生成SeriesScanOperator、FilterAndProjectOperator、IdentitySinkOperator的操作符树
   * 4. 管道：将操作符树封装为PipelineDriverFactory，支持并行执行
   * 
   * @param plan 查询计划节点树
   * @param instanceContext 片段实例上下文
   * @param schemaRegion 模式区域
   * @return PipelineDriverFactory列表，用于创建执行驱动器
   * @throws MemoryNotEnoughException 当内存不足时抛出异常
   */
  public List<PipelineDriverFactory> plan(
      PlanNode plan, FragmentInstanceContext instanceContext, ISchemaRegion schemaRegion)
      throws MemoryNotEnoughException {
    // 步骤1：创建本地执行计划上下文
    // 关键：将片段实例上下文和模式区域封装为执行环境
    LocalExecutionPlanContext context =
        new LocalExecutionPlanContext(instanceContext, schemaRegion);

    // 步骤2：使用OperatorTreeGenerator将PlanNode树转换为Operator树
    // 关键：通过访问者模式递归遍历PlanNode树，生成对应的Operator
    // 示例：对于SELECT t1 AS ref0查询，依次调用：
    // - visitSeriesScan：生成SeriesScanOperator（扫描root.db0.t1数据）
    // - visitFilter：生成FilterOperator（应用t1 + 2 <= 10过滤条件）
    // - visitProject：生成ProjectOperator（投影t1 AS ref0）
    // - visitIdentitySink：生成IdentitySinkOperator（最终输出）
    Operator root = plan.accept(new OperatorTreeGenerator(), context);

    // 步骤3：构建内存估算器，估算执行所需内存
    // 关键：基于操作符树估算内存使用量，避免内存不足
    PipelineMemoryEstimator memoryEstimator =
        context.constructPipelineMemoryEstimator(root, null, plan, -1);
    
    // 步骤4：清理内存估算相关的映射表，释放资源
    // 关键：避免内存泄漏，优化GC性能
    context.invalidateParentPlanNodeIdToMemoryEstimator();

    // 步骤5：检查内存是否足够执行当前查询
    // 关键：如果内存不足，抛出MemoryNotEnoughException异常
    checkMemory(memoryEstimator, instanceContext.getStateMachine());

    // 步骤6：创建管道驱动工厂
    // 关键：将操作符树封装为可执行的管道
    context.addPipelineDriverFactory(root, context.getDriverContext(), 0);

    // 步骤7：配置执行参数，设置单个SourceHandle可以保留的最大字节数
    // 关键：优化数据传输性能，避免内存过度使用
    context.setMaxBytesOneHandleCanReserve();

    // 步骤8：返回管道驱动工厂列表
    // 关键：FragmentInstanceManager使用这些工厂创建具体的执行驱动器
    return context.getPipelineDriverFactories();
  }

  private long checkMemory(
      final PipelineMemoryEstimator memoryEstimator, FragmentInstanceStateMachine stateMachine)
      throws MemoryNotEnoughException {

    // if it is disabled, just return
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableQueryMemoryEstimation()
        && !IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      return 0;
    }

    long estimatedMemorySize = memoryEstimator.getEstimatedMemoryUsageInBytes();

    QueryRelatedResourceMetricSet.getInstance().updateEstimatedMemory(estimatedMemorySize);

    synchronized (this) {
      if (estimatedMemorySize > freeMemoryForOperators) {
        throw new MemoryNotEnoughException(
            String.format(
                "There is not enough memory to execute current fragment instance, "
                    + "current remaining free memory is %dB, "
                    + "estimated memory usage for current fragment instance is %dB",
                freeMemoryForOperators, estimatedMemorySize));
      } else {
        freeMemoryForOperators -= estimatedMemorySize;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "[ConsumeMemory] consume: {}, current remaining memory: {}",
              estimatedMemorySize,
              freeMemoryForOperators);
        }
      }
    }

    stateMachine.addStateChangeListener(
        newState -> {
          if (newState.isDone()) {
            try (SetThreadName fragmentInstanceName =
                new SetThreadName(stateMachine.getFragmentInstanceId().getFullId())) {
              synchronized (this) {
                this.freeMemoryForOperators += estimatedMemorySize;
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "[ReleaseMemory] release: {}, current remaining memory: {}",
                      estimatedMemorySize,
                      freeMemoryForOperators);
                }
              }
            }
          }
        });
    return estimatedMemorySize;
  }

  private QueryDataSourceType getQueryDataSourceType(DataDriverContext dataDriverContext) {
    return dataDriverContext.getQueryDataSourceType().orElse(QueryDataSourceType.SERIES_SCAN);
  }

  private Map<IDeviceID, DeviceContext> collectDevicePathsToContext(
      LocalExecutionPlanContext context) {
    DataDriverContext dataDriverContext = (DataDriverContext) context.getDriverContext();
    Map<IDeviceID, DeviceContext> deviceContextMap = dataDriverContext.getDeviceIDToContext();
    dataDriverContext.clearDeviceIDToContext();
    return deviceContextMap;
  }

  private List<PartialPath> collectSourcePaths(LocalExecutionPlanContext context) {
    List<PartialPath> sourcePaths = new ArrayList<>();
    context
        .getPipelineDriverFactories()
        .forEach(
            pipeline -> {
              DataDriverContext dataDriverContext = (DataDriverContext) pipeline.getDriverContext();
              sourcePaths.addAll(dataDriverContext.getPaths());
              dataDriverContext.clearPaths();
            });
    return sourcePaths;
  }

  public synchronized boolean forceAllocateFreeMemoryForOperators(long memoryInBytes) {
    if (freeMemoryForOperators - memoryInBytes <= MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD) {
      return false;
    } else {
      freeMemoryForOperators -= memoryInBytes;
      return true;
    }
  }

  public synchronized long tryAllocateFreeMemoryForOperators(long memoryInBytes) {
    if (freeMemoryForOperators - memoryInBytes <= MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD) {
      long result = freeMemoryForOperators - MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
      freeMemoryForOperators = MIN_REST_MEMORY_FOR_QUERY_AFTER_LOAD;
      return result;
    } else {
      freeMemoryForOperators -= memoryInBytes;
      return memoryInBytes;
    }
  }

  public synchronized void reserveFromFreeMemoryForOperators(
      final long memoryInBytes,
      final long reservedBytes,
      final String queryId,
      final String contextHolder) {
    if (memoryInBytes > freeMemoryForOperators) {
      throw new MemoryNotEnoughException(
          String.format(
              "There is not enough memory for Query %s, the contextHolder is %s,"
                  + "current remaining free memory is %dB, "
                  + "already reserved memory for this context in total is %dB, "
                  + "the memory requested this time is %dB",
              queryId, contextHolder, freeMemoryForOperators, reservedBytes, memoryInBytes));
    } else {
      freeMemoryForOperators -= memoryInBytes;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "[ConsumeMemory] consume: {}, current remaining memory: {}",
            memoryInBytes,
            freeMemoryForOperators);
      }
    }
  }

  public synchronized void releaseToFreeMemoryForOperators(final long memoryInBytes) {
    freeMemoryForOperators += memoryInBytes;
  }

  public long getAllocateMemoryForOperators() {
    return ALLOCATE_MEMORY_FOR_OPERATORS;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final LocalExecutionPlanner INSTANCE = new LocalExecutionPlanner();
  }
}