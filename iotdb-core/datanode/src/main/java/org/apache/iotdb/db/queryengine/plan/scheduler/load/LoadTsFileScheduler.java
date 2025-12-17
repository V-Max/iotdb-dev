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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link LoadTsFileScheduler} is used for scheduling {@link LoadSingleTsFileNode} and {@link
 * LoadTsFilePieceNode}. because these two nodes need two phases to finish transfer.
 *
 * <p>for more details please check: <a
 * href="https://apache-iotdb.feishu.cn/docx/doxcnyBYWzek8ksSEU6obZMpYLe">...</a>;
 */
/**
 * LoadTsFileScheduler是IoTDB用于调度TsFile加载操作的专用调度器
 * 负责处理LoadSingleTsFileNode和LoadTsFilePieceNode两种节点的执行
 * 采用两阶段传输机制完成TsFile的加载过程
 */
public class LoadTsFileScheduler implements IScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileScheduler.class);

  /** IoTDB配置实例 */
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  /** TsFile加载成本指标集单例 */
  private static final LoadTsFileCostMetricsSet LOAD_TSFILE_COST_METRICS_SET =
      LoadTsFileCostMetricsSet.getInstance();

  /** 单个调度器的最大内存大小限制（Thrift最大帧大小的四分之一） */
  private static final long SINGLE_SCHEDULER_MAX_MEMORY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize() >> 2;
  
  /** 时间分区槽传输限制 */
  private static final int TRANSMIT_LIMIT =
      CommonDescriptor.getInstance().getConfig().getTTimePartitionSlotTransmitLimit();

  /** 正在加载的文件集合，用于防止重复加载 */
  private static final Set<String> LOADING_FILE_SET = new HashSet<>();

  /** MPP查询上下文，包含查询ID、执行参数等 */
  private final MPPQueryContext queryContext;
  
  /** 查询状态机，用于管理查询的生命周期状态 */
  private final QueryStateMachine stateMachine;
  
  /** TsFile加载分发器实现 */
  private final LoadTsFileDispatcherImpl dispatcher;
  
  /** 数据分区批量获取器 */
  private final DataPartitionBatchFetcher partitionFetcher;
  
  /** 待加载的TsFile节点列表 */
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  
  /** 计划片段ID */
  private final PlanFragmentId fragmentId;
  
  /** 所有副本集集合 */
  private final Set<TRegionReplicaSet> allReplicaSets;
  
  /** 是否由管道生成 */
  private final boolean isGeneratedByPipe;
  
  /** TsFile数据缓存内存块 */
  private final LoadTsFileDataCacheMemoryBlock block;

  /**
   * 构造函数，初始化LoadTsFileScheduler实例
   *
   * @param distributedQueryPlan 分布式查询计划
   * @param queryContext MPP查询上下文，包含查询ID、执行参数等
   * @param stateMachine 查询状态机，用于管理查询的生命周期状态
   * @param internalServiceClientManager 内部服务客户端管理器
   * @param partitionFetcher 分区获取器
   * @param isGeneratedByPipe 是否由管道生成
   */
  public LoadTsFileScheduler(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      IPartitionFetcher partitionFetcher,
      boolean isGeneratedByPipe) {
    // 初始化核心成员变量
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.tsFileNodeList = new ArrayList<>();
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.dispatcher = new LoadTsFileDispatcherImpl(internalServiceClientManager, isGeneratedByPipe);
    this.partitionFetcher = new DataPartitionBatchFetcher(partitionFetcher);
    this.allReplicaSets = new HashSet<>();
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.block = LoadTsFileMemoryManager.getInstance().allocateDataCacheMemoryBlock();

    // 从分布式查询计划中提取所有LoadSingleTsFileNode
    for (FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      tsFileNodeList.add((LoadSingleTsFileNode) fragmentInstance.getFragment().getPlanNodeTree());
    }
  }

  /**
   * 启动TsFile加载调度流程
   * 实现IScheduler接口的start方法
   */
  @Override
  public void start() {
    try {
      // 将查询状态转换为运行中
      stateMachine.transitionToRunning();
      
      // 获取待加载的TsFile节点数量
      int tsFileNodeListSize = tsFileNodeList.size();
      // 标记是否所有TsFile都加载成功
      boolean isLoadSuccess = true;

      // 遍历所有待加载的TsFile节点
      for (int i = 0; i < tsFileNodeListSize; ++i) {
        final LoadSingleTsFileNode node = tsFileNodeList.get(i);
        final String filePath = node.getTsFileResource().getTsFilePath();

        // 标记单个TsFile是否加载成功
        boolean isLoadSingleTsFileSuccess = true;
        // 标记是否需要从加载文件集合中移除该文件
        boolean shouldRemoveFileFromLoadingSet = false;
        
        try {
          // 检查文件是否正在被其他调度器加载
          synchronized (LOADING_FILE_SET) {
            if (LOADING_FILE_SET.contains(filePath)) {
              throw new LoadFileException(
                  String.format("TsFile %s is loading by another scheduler.", filePath));
            }
            // 将文件添加到加载集合中
            LOADING_FILE_SET.add(filePath);
          }
          shouldRemoveFileFromLoadingSet = true;

          // 检查TsFile是否为空
          if (node.isTsFileEmpty()) {
            LOGGER.info("Load skip TsFile {}, because it has no data.", filePath);
          } 
          // 检查是否需要解码TsFile，不需要则本地加载
          else if (!node.needDecodeTsFile(
              slotList ->
                  partitionFetcher.queryDataPartition(
                      slotList,
                      queryContext.getSession().getUserName()))) {
            final long startTime = System.nanoTime();
            try {
              // 本地加载TsFile
              isLoadSingleTsFileSuccess = loadLocally(node);
            } finally {
              // 记录本地加载阶段的时间成本
              LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
                  LoadTsFileCostMetricsSet.LOAD_LOCALLY, System.nanoTime() - startTime);
            }
          } 
          // 需要解码，使用两阶段加载方法（本地或远程）
          else {
            // 生成UUID用于标识这次加载操作
            String uuid = UUID.randomUUID().toString();
            dispatcher.setUuid(uuid);
            allReplicaSets.clear();

            // 执行第一阶段加载
            long startTime = System.nanoTime();
            final boolean isFirstPhaseSuccess;
            try {
              isFirstPhaseSuccess = firstPhase(node);
            } finally {
              // 记录第一阶段的时间成本
              LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
                  LoadTsFileCostMetricsSet.FIRST_PHASE, System.nanoTime() - startTime);
            }

            // 执行第二阶段加载
            startTime = System.nanoTime();
            final boolean isSecondPhaseSuccess;
            try {
              isSecondPhaseSuccess =
                  secondPhase(isFirstPhaseSuccess, uuid, node.getTsFileResource());
            } finally {
              // 记录第二阶段的时间成本
              LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
                  LoadTsFileCostMetricsSet.SECOND_PHASE, System.nanoTime() - startTime);
            }

            // 检查两阶段是否都成功
            if (!isFirstPhaseSuccess || !isSecondPhaseSuccess) {
              isLoadSingleTsFileSuccess = false;
            }
          }

          // 处理单个TsFile加载结果
          if (isLoadSingleTsFileSuccess) {
            // 清理节点资源
            node.clean();
            LOGGER.info(
                "Load TsFile {} Successfully, load process [{}/{}]",
                filePath,
                i + 1,
                tsFileNodeListSize);
          } else {
            isLoadSuccess = false;
            LOGGER.warn(
                "Can not Load TsFile {}, load process [{}/{}]",
                filePath,
                i + 1,
                tsFileNodeListSize);
          }
        } catch (Exception e) {
          isLoadSuccess = false;
          stateMachine.transitionToFailed(e);
          LOGGER.warn("LoadTsFileScheduler loads TsFile {} error", filePath, e);
        } finally {
          // 从加载文件集合中移除该文件
          if (shouldRemoveFileFromLoadingSet) {
            synchronized (LOADING_FILE_SET) {
              LOADING_FILE_SET.remove(filePath);
            }
          }
        }
      }
      
      // 所有TsFile加载完成后，更新查询状态
      if (isLoadSuccess) {
        stateMachine.transitionToFinished();
      }
    } finally {
      // 释放数据缓存内存块
      LoadTsFileMemoryManager.getInstance().releaseDataCacheMemoryBlock();
    }
  }

  /**
   * 执行TsFile加载的第一阶段
   * 主要完成TsFile的解析和数据分区的确定
   * @param node 待加载的单个TsFile节点
   * @return 第一阶段是否执行成功
   */
  private boolean firstPhase(LoadSingleTsFileNode node) {
    // 创建TsFile数据管理器，用于处理解析后的数据
    final TsFileDataManager tsFileDataManager = new TsFileDataManager(this, node, block);
    try {
      // 创建TsFile拆分器，按数据分区拆分TsFile
      new TsFileSplitter(
              node.getTsFileResource().getTsFile(), tsFileDataManager::addOrSendTsFileData)
          .splitTsFileByDataPartition();
      // 发送所有TsFile数据到目标位置
      if (!tsFileDataManager.sendAllTsFileData()) {
        stateMachine.transitionToFailed(new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()));
        return false;
      }
    } catch (IllegalStateException e) {
      // 处理数据分发异常
      stateMachine.transitionToFailed(e);
      LOGGER.warn(
          String.format(
              "Dispatch TsFileData error when parsing TsFile %s.",
              node.getTsFileResource().getTsFile()),
          e);
      return false;
    } catch (Exception e) {
      // 处理其他异常
      stateMachine.transitionToFailed(e);
      LOGGER.warn(
          String.format("Parse or send TsFile %s error.", node.getTsFileResource().getTsFile()), e);
      return false;
    } finally {
      // 清空TsFile数据管理器
      tsFileDataManager.clear();
    }
    return true;
  }

  private boolean dispatchOnePieceNode(
      LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    allReplicaSets.add(replicaSet);
    FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(fragmentId, pieceNode),
            fragmentId.genFragmentInstanceId(),
            null,
            queryContext.getQueryType(),
            queryContext.getTimeOut(),
            queryContext.getSession());
    instance.setExecutorAndHost(new StorageExecutor(replicaSet));
    Future<FragInstanceDispatchResult> dispatchResultFuture =
        dispatcher.dispatch(Collections.singletonList(instance));

    try {
      FragInstanceDispatchResult result =
          dispatchResultFuture.get(
              CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds(), TimeUnit.SECONDS);
      if (!result.isSuccessful()) {
        // TODO: retry.
        LOGGER.warn(
            "Dispatch one piece to ReplicaSet {} error. Result status code {}. "
                + "Result status message {}. Dispatch piece node error:%n{}",
            replicaSet,
            TSStatusCode.representOf(result.getFailureStatus().getCode()).name(),
            result.getFailureStatus().getMessage(),
            pieceNode);
        if (result.getFailureStatus().getSubStatus() != null) {
          for (TSStatus status : result.getFailureStatus().getSubStatus()) {
            LOGGER.warn(
                "Sub status code {}. Sub status message {}.",
                TSStatusCode.representOf(status.getCode()).name(),
                status.getMessage());
          }
        }
        TSStatus status = result.getFailureStatus();
        status.setMessage(
            String.format("Load %s piece error in 1st phase. Because ", pieceNode.getTsFile())
                + status.getMessage());
        stateMachine.transitionToFailed(status); // TODO: record more status
        return false;
      }
    } catch (InterruptedException | ExecutionException | CancellationException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOGGER.warn("Interrupt or Execution error.", e);
      stateMachine.transitionToFailed(e);
      return false;
    } catch (TimeoutException e) {
      dispatchResultFuture.cancel(true);
      LOGGER.warn(
          String.format("Wait for loading %s time out.", LoadTsFilePieceNode.class.getName()), e);
      stateMachine.transitionToFailed(e);
      return false;
    }
    return true;
  }

  /**
   * 执行TsFile加载的第二阶段
   * 主要完成加载命令的分发和执行，根据第一阶段的结果决定执行或回滚操作
   * @param isFirstPhaseSuccess 第一阶段是否执行成功
   * @param uuid 加载操作的唯一标识符
   * @param tsFileResource TsFile资源信息
   * @return 第二阶段是否执行成功
   */
  private boolean secondPhase(
      boolean isFirstPhaseSuccess, String uuid, TsFileResource tsFileResource) {
    // 记录开始分发加载命令的日志
    LOGGER.info("Start dispatching Load command for uuid {}", uuid);
    // 获取TsFile文件对象
    final File tsFile = tsFileResource.getTsFile();
    // 创建加载命令请求，根据第一阶段结果决定是执行还是回滚
    final TLoadCommandReq loadCommandReq =
        new TLoadCommandReq(
            (isFirstPhaseSuccess ? LoadCommand.EXECUTE : LoadCommand.ROLLBACK).ordinal(), uuid);

    try {
      // 设置是否由管道生成的标志
      loadCommandReq.setIsGeneratedByPipe(isGeneratedByPipe);
      // 设置进度索引，用于管道处理
      loadCommandReq.setProgressIndex(assignProgressIndex(tsFileResource));
      // 将加载命令分发到所有相关的副本集
      Future<FragInstanceDispatchResult> dispatchResultFuture =
          dispatcher.dispatchCommand(loadCommandReq, allReplicaSets);

      // 等待命令分发结果
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      // 检查命令分发是否成功
      if (!result.isSuccessful()) {
        // TODO: 实现重试机制
        LOGGER.warn(
            "Dispatch load command {} of TsFile {} error to replicaSets {} error. "
                + "Result status code {}. Result status message {}.",
            loadCommandReq,
            tsFile,
            allReplicaSets,
            TSStatusCode.representOf(result.getFailureStatus().getCode()).name(),
            result.getFailureStatus().getMessage());
        // 获取失败状态，并添加详细信息
        TSStatus status = result.getFailureStatus();
        status.setMessage(
            String.format("Load %s error in 2nd phase. Because ", tsFile) + status.getMessage());
        // 更新查询状态为失败
        stateMachine.transitionToFailed(status);
        return false;
      }
    } catch (IOException e) {
      // 处理进度索引序列化异常
      LOGGER.warn(
          "Serialize Progress Index error, isFirstPhaseSuccess: {}, uuid: {}, tsFile: {}",
          isFirstPhaseSuccess,
          uuid,
          tsFile.getAbsolutePath());
      stateMachine.transitionToFailed(e);
      return false;
    } catch (InterruptedException | ExecutionException e) {
      // 处理中断或执行异常
      if (e instanceof InterruptedException) {
        // 重新设置中断标志
        Thread.currentThread().interrupt();
      }
      LOGGER.warn("Interrupt or Execution error.", e);
      stateMachine.transitionToFailed(e);
      return false;
    }
    // 第二阶段执行成功
    return true;
  }

  private ByteBuffer assignProgressIndex(TsFileResource tsFileResource) throws IOException {
    PipeDataNodeAgent.runtime().assignProgressIndexForTsFileLoad(tsFileResource);

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      tsFileResource.getMaxProgressIndex().serialize(dataOutputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  /**
   * 本地加载TsFile
   * 当TsFile不需要解码时，直接在本地加载
   * @param node 待加载的单个TsFile节点
   * @return 是否加载成功
   * @throws IoTDBException 如果加载过程中发生IoTDB相关异常
   */
  private boolean loadLocally(LoadSingleTsFileNode node) throws IoTDBException {
    // 记录本地加载开始日志
    LOGGER.info("Start load TsFile {} locally.", node.getTsFileResource().getTsFile().getPath());

    // 检查系统是否为只读模式
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new LoadReadOnlyException();
    }

    try {
      // 创建片段实例
      FragmentInstance instance =
          new FragmentInstance(
              new PlanFragment(fragmentId, node),
              fragmentId.genFragmentInstanceId(),
              null,
              queryContext.getQueryType(),
              queryContext.getTimeOut(),
              queryContext.getSession());
      // 设置执行器和主机信息
      instance.setExecutorAndHost(new StorageExecutor(node.getLocalRegionReplicaSet()));
      // 本地分发片段实例
      dispatcher.dispatchLocally(instance);
    } catch (FragmentInstanceDispatchException e) {
      // 处理分发异常
      LOGGER.warn(
          String.format(
              "Dispatch tsFile %s error to local error. Result status code %s. "
                  + "Result status message %s.",
              node.getTsFileResource().getTsFile(),
              TSStatusCode.representOf(e.getFailureStatus().getCode()).name(),
              e.getFailureStatus().getMessage()));
      // 更新查询状态为失败
      stateMachine.transitionToFailed(e.getFailureStatus());
      return false;
    }

    // 收集并记录指标
    DataRegion dataRegion =
        StorageEngine.getInstance()
            .getDataRegion(
                (DataRegionId)
                    ConsensusGroupId.Factory.createFromTConsensusGroupId(
                        node.getLocalRegionReplicaSet().getRegionId()));

    dataRegion
        .getNonSystemDatabaseName()
        .ifPresent(
            databaseName -> {
              // 向IoTDB刷新指标报告加载的TsFile点数
              MemTableFlushTask.recordFlushPointsMetricInternal(
                  node.getWritePointCount(), databaseName, dataRegion.getDataRegionId());

              // 记录加载点数指标
              MetricService.getInstance()
                  .count(
                      node.getWritePointCount(),
                      Metric.QUANTITY.toString(),
                      MetricLevel.CORE,
                      Tag.NAME.toString(),
                      Metric.POINTS_IN.toString(),
                      Tag.DATABASE.toString(),
                      databaseName,
                      Tag.REGION.toString(),
                      dataRegion.getDataRegionId(),
                      Tag.TYPE.toString(),
                      Metric.LOAD_POINT_COUNT.toString());
              // 记录领导者节点的加载点数指标
              MetricService.getInstance()
                  .count(
                      node.getWritePointCount(),
                      Metric.LEADER_QUANTITY.toString(),
                      MetricLevel.CORE,
                      Tag.NAME.toString(),
                      Metric.POINTS_IN.toString(),
                      Tag.DATABASE.toString(),
                      databaseName,
                      Tag.REGION.toString(),
                      dataRegion.getDataRegionId(),
                      Tag.TYPE.toString(),
                      Metric.LOAD_POINT_COUNT.toString());
            });

    // 本地加载成功
    return true;
  }

  @Override
  public void stop(Throwable t) {
    // Do nothing
  }

  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }

  public enum LoadCommand {
    EXECUTE,
    ROLLBACK
  }

  private static class TsFileDataManager {
    private final LoadTsFileScheduler scheduler;
    private final LoadSingleTsFileNode singleTsFileNode;

    private long dataSize;
    private final Map<TRegionReplicaSet, LoadTsFilePieceNode> replicaSet2Piece;
    private final List<ChunkData> nonDirectionalChunkData;
    private final LoadTsFileDataCacheMemoryBlock block;

    public TsFileDataManager(
        LoadTsFileScheduler scheduler,
        LoadSingleTsFileNode singleTsFileNode,
        LoadTsFileDataCacheMemoryBlock block) {
      this.scheduler = scheduler;
      this.singleTsFileNode = singleTsFileNode;
      this.dataSize = 0;
      this.replicaSet2Piece = new HashMap<>();
      this.nonDirectionalChunkData = new ArrayList<>();
      this.block = block;
    }

    private boolean addOrSendTsFileData(TsFileData tsFileData) {
      return tsFileData.isModification()
          ? addOrSendDeletionData(tsFileData)
          : addOrSendChunkData((ChunkData) tsFileData);
    }

    private boolean isMemoryEnough() {
      return dataSize <= SINGLE_SCHEDULER_MAX_MEMORY_SIZE && block.hasEnoughMemory();
    }

    private boolean addOrSendChunkData(ChunkData chunkData) {
      nonDirectionalChunkData.add(chunkData);
      dataSize += chunkData.getDataSize();
      block.addMemoryUsage(chunkData.getDataSize());

      if (!isMemoryEnough()) {
        routeChunkData();

        // start to dispatch from the biggest TsFilePieceNode
        List<TRegionReplicaSet> sortedReplicaSets =
            replicaSet2Piece.keySet().stream()
                .sorted(
                    Comparator.comparingLong(o -> replicaSet2Piece.get(o).getDataSize()).reversed())
                .collect(Collectors.toList());

        for (TRegionReplicaSet sortedReplicaSet : sortedReplicaSets) {
          LoadTsFilePieceNode pieceNode = replicaSet2Piece.get(sortedReplicaSet);
          if (pieceNode.getDataSize() == 0) { // total data size has been reduced to 0
            break;
          }
          if (!scheduler.dispatchOnePieceNode(pieceNode, sortedReplicaSet)) {
            return false;
          }

          dataSize -= pieceNode.getDataSize();
          block.reduceMemoryUsage(pieceNode.getDataSize());
          replicaSet2Piece.put(
              sortedReplicaSet,
              new LoadTsFilePieceNode(
                  singleTsFileNode.getPlanNodeId(),
                  singleTsFileNode
                      .getTsFileResource()
                      .getTsFile())); // can not just remove, because of deletion
          if (isMemoryEnough()) {
            break;
          }
        }
      }

      return true;
    }

    private void routeChunkData() {
      if (nonDirectionalChunkData.isEmpty()) {
        return;
      }

      List<TRegionReplicaSet> replicaSets =
          scheduler.partitionFetcher.queryDataPartition(
              nonDirectionalChunkData.stream()
                  .map(
                      data ->
                          new Pair<>(
                              (IDeviceID) new PlainDeviceID(data.getDevice()),
                              data.getTimePartitionSlot()))
                  .collect(Collectors.toList()),
              scheduler.queryContext.getSession().getUserName());
      IntStream.range(0, nonDirectionalChunkData.size())
          .forEach(
              i ->
                  replicaSet2Piece
                      .computeIfAbsent(
                          replicaSets.get(i),
                          o ->
                              new LoadTsFilePieceNode(
                                  singleTsFileNode.getPlanNodeId(),
                                  singleTsFileNode.getTsFileResource().getTsFile()))
                      .addTsFileData(nonDirectionalChunkData.get(i)));
      nonDirectionalChunkData.clear();
    }

    private boolean addOrSendDeletionData(TsFileData deletionData) {
      routeChunkData(); // ensure chunk data will be added before deletion

      for (Map.Entry<TRegionReplicaSet, LoadTsFilePieceNode> entry : replicaSet2Piece.entrySet()) {
        dataSize += deletionData.getDataSize();
        block.addMemoryUsage(deletionData.getDataSize());
        entry.getValue().addTsFileData(deletionData);
      }
      return true;
    }

    private boolean sendAllTsFileData() {
      routeChunkData();

      for (Map.Entry<TRegionReplicaSet, LoadTsFilePieceNode> entry : replicaSet2Piece.entrySet()) {
        block.reduceMemoryUsage(entry.getValue().getDataSize());
        if (!scheduler.dispatchOnePieceNode(entry.getValue(), entry.getKey())) {
          LOGGER.warn(
              "Dispatch piece node {} of TsFile {} error.",
              entry.getValue(),
              singleTsFileNode.getTsFileResource().getTsFile());
          return false;
        }
      }
      return true;
    }

    private void clear() {
      replicaSet2Piece.clear();
    }
  }

  private static class DataPartitionBatchFetcher {
    private final IPartitionFetcher fetcher;

    public DataPartitionBatchFetcher(IPartitionFetcher fetcher) {
      this.fetcher = fetcher;
    }

    public List<TRegionReplicaSet> queryDataPartition(
        List<Pair<IDeviceID, TTimePartitionSlot>> slotList, String userName) {
      List<TRegionReplicaSet> replicaSets = new ArrayList<>();
      int size = slotList.size();

      for (int i = 0; i < size; i += TRANSMIT_LIMIT) {
        List<Pair<IDeviceID, TTimePartitionSlot>> subSlotList =
            slotList.subList(i, Math.min(size, i + TRANSMIT_LIMIT));
        DataPartition dataPartition =
            fetcher.getOrCreateDataPartition(toQueryParam(subSlotList), userName);
        replicaSets.addAll(
            subSlotList.stream()
                .map(
                    pair ->
                        dataPartition.getDataRegionReplicaSetForWriting(
                            ((PlainDeviceID) pair.left).toStringID(), pair.right))
                .collect(Collectors.toList()));
      }
      return replicaSets;
    }

    private List<DataPartitionQueryParam> toQueryParam(
        List<Pair<IDeviceID, TTimePartitionSlot>> slots) {
      return slots.stream()
          .collect(
              Collectors.groupingBy(
                  Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toSet())))
          .entrySet()
          .stream()
          .map(
              entry ->
                  new DataPartitionQueryParam(
                      ((PlainDeviceID) entry.getKey()).toStringID(),
                      new ArrayList<>(entry.getValue())))
          .collect(Collectors.toList());
    }
  }
}