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
package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.optimization.ColumnInjectionPushDown;
import org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown;
import org.apache.iotdb.db.queryengine.plan.optimization.OrderByExpressionWithLimitChangeToTopK;
import org.apache.iotdb.db.queryengine.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.planner.IFragmentParallelPlaner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.apache.commons.lang3.Validate;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode.LIMIT_VALUE_USE_TOP_K;

public class DistributionPlanner {
  private final Analysis analysis;
  private final MPPQueryContext context;
  private final LogicalQueryPlan logicalPlan;

  private final List<PlanOptimizer> optimizers;

  public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
    this.analysis = analysis;
    this.logicalPlan = logicalPlan;
    this.context = logicalPlan.getContext();

    this.optimizers =
        Arrays.asList(
            new LimitOffsetPushDown(),
            new ColumnInjectionPushDown(),
            new OrderByExpressionWithLimitChangeToTopK());
  }

  /**
   * 作用：根据数据分布重写数据源节点，将逻辑数据源节点转换为物理数据源节点。
   *
   * 示例查询分析：
   *
   * 输入：逻辑查询计划中的 SeriesScanNode（逻辑数据源）
   * 处理：根据数据分布信息，确定数据实际存储位置
   * 输出：SeriesSourceNode（物理数据源），指向具体的数据节点
   * 
   * 数据结构变化：
   * SeriesScanNode → SeriesSourceNode
   * 关键步骤说明：
   *
   * 创建重写器：基于查询分析结果创建 SourceRewriter
   * 遍历逻辑计划：深度优先遍历逻辑查询计划树
   * 重写数据源：将逻辑数据源节点转换为物理数据源节点
   * 验证结果：确保重写后只有一个根节点
   * @return
   */
  public PlanNode rewriteSource() {
    // 创建数据源重写器，基于分析结果
    SourceRewriter rewriter = new SourceRewriter(this.analysis);
    // 遍历逻辑计划根节点，重写数据源节点
    List<PlanNode> planNodeList =
        rewriter.visit(logicalPlan.getRootNode(), new DistributionPlanContext(context));
    // 验证根节点数量
    if (planNodeList.size() != 1) {
      throw new IllegalStateException("root node must return only one");
    } else {
      // 返回重写后的根节点
      return planNodeList.get(0);
    }
}


  /**
   *
   * 作用：在查询计划中添加 ExchangeNode，实现跨节点数据交换。
   *
   * 示例查询分析：
   *
   * 输入：重写数据源后的查询计划
   * 处理：在数据边界处插入 ExchangeNode
   * 输出：包含 ExchangeNode 的查询计划
   * 数据结构变化：
   *
   * 查询计划树 → 查询计划树 + ExchangeNode
   * ExchangeNode 作用：
   *
   * 数据交换节点，连接不同数据节点
   * 实现跨节点数据传递
   * 支持并行查询执行
   */
  public PlanNode addExchangeNode(PlanNode root) {
      // 创建ExchangeNode添加器
      ExchangeNodeAdder adder = new ExchangeNodeAdder(this.analysis);
      // 创建节点分组上下文，确定数据分布
      NodeGroupContext nodeGroupContext =
          new NodeGroupContext(context, analysis.getStatement(), root);
      // 遍历查询计划，添加ExchangeNode
      PlanNode newRoot = adder.visit(root, nodeGroupContext);
      // 调整上游数据流
      adjustUpStream(newRoot, nodeGroupContext);
      return newRoot;
  }

  /**
   * 作用：调整 ExchangeNode 的上游数据流，生成合适的 SinkNode。
   *
   * 示例查询分析：
   *
   * 条件判断：查询不是虚拟数据源，且没有 ORDER BY 子句
   * 结果：needShuffleSinkNode = false，使用 IdentitySinkNode
   * 调用：adjustUpStreamHelper(root, memo, false, context)
   * 
   * 处理逻辑：
   * 条件	          处理方式	适用场景
   * 无ExchangeNode	直接返回	单节点查询
   * 虚拟数据源	简化版调整	虚拟数据源查询
   * 普通查询	完整调整	真实数据源查询
   * 
   * Adjust upStream of exchangeNodes, generate {@link
   * org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode} or {@link
   * org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode} for the children
   * of ExchangeNodes with Same DataRegion.
   */
  private void adjustUpStream(PlanNode root, NodeGroupContext context) {
    // 检查是否存在ExchangeNode
    if (!context.hasExchangeNode) {
      return;
    }

    // 虚拟数据源场景使用简化版调整
    if (analysis.isVirtualSource()) {
      adjustUpStreamHelper(root, context);
      return;
    }

    // 判断是否需要ShuffleSinkNode
    final boolean needShuffleSinkNode =
        analysis.getStatement() instanceof QueryStatement
            && needShuffleSinkNode((QueryStatement) analysis.getStatement(), context);

    // 执行完整的上游数据流调整
    adjustUpStreamHelper(root, new HashMap<>(), needShuffleSinkNode, context);
}

  /**
   * 简化版上游数据流调整辅助方法（用于虚拟数据源场景）
   * 
   * 核心功能：遍历查询计划树，为每个ExchangeNode添加IdentitySinkNode作为子节点，
   * 建立数据交换通道。
   * 
   * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
   * 步骤1：递归遍历子节点 - 深度优先遍历查询计划树
   * 步骤2：发现ExchangeNode - 识别需要调整的数据交换节点
   * 步骤3：创建IdentitySinkNode - 生成数据接收节点
   * 步骤4：建立数据通道 - 添加DownStreamChannelLocation
   * 步骤5：更新ExchangeNode - 设置新的子节点和SinkHandle索引
   * 
   * @param root 当前处理的查询计划节点
   * @param context 节点分组上下文
   */
  private void adjustUpStreamHelper(PlanNode root, NodeGroupContext context) {
    for (PlanNode child : root.getChildren()) {
      adjustUpStreamHelper(child, context);
      if (child instanceof ExchangeNode) {
        ExchangeNode exchangeNode = (ExchangeNode) child;
        MultiChildrenSinkNode newChild =
            new IdentitySinkNode(context.queryContext.getQueryId().genPlanNodeId());
        newChild.addChild(exchangeNode.getChild());
        newChild.addDownStreamChannelLocation(
            new DownStreamChannelLocation(exchangeNode.getPlanNodeId().toString()));
        exchangeNode.setChild(newChild);
        exchangeNode.setIndexOfUpstreamSinkHandle(newChild.getCurrentLastIndex());
      }
    }
  }

  /**
   * 作用：为 ExchangeNode 添加合适的 SinkNode，建立数据交换通道。
   *
   * 示例查询分析：
   *
   * 输入：包含 ExchangeNode 的查询计划树
   * 处理：为每个 ExchangeNode 添加 IdentitySinkNode
   * 输出：ExchangeNode.child 从原节点变为 IdentitySinkNode
   * 数据结构变化：
   *
   *
   * ExchangeNode
   * ├── 原子节点
   * 变为：
   * ExchangeNode
   * ├── IdentitySinkNode
   *     ├── 原子节点
   * 关键步骤说明：
   *
   * 步骤	操作	作用
   * 1	递归遍历	深度优先遍历查询计划树
   * 2	发现ExchangeNode	识别需要调整的数据交换节点
   * 3	获取数据区域	确定子节点所在的数据副本集
   * 4	选择SinkNode类型	根据条件选择IdentitySinkNode或ShuffleSinkNode
   * 5	创建SinkNode	生成数据接收节点
   * 6	建立数据通道	添加DownStreamChannelLocation
   * 7	更新ExchangeNode	设置新的子节点和SinkHandle索引
   * SinkNode 类型对比：
   *
   * 类型	适用场景	数据流特点
   * IdentitySinkNode	普通查询	直接传递数据，不重分布
   * ShuffleSinkNode	排序查询	数据重分布，支持排序操作
   * @param root
   * @param memo
   * @param needShuffleSinkNode
   * @param context
   */
  private void adjustUpStreamHelper(
      PlanNode root,
      Map<TRegionReplicaSet, MultiChildrenSinkNode> memo,
      boolean needShuffleSinkNode,
      NodeGroupContext context) {
      // 递归遍历所有子节点
      for (PlanNode child : root.getChildren()) {
        adjustUpStreamHelper(child, memo, needShuffleSinkNode, context);
        // 发现ExchangeNode时进行处理
        if (child instanceof ExchangeNode) {
          ExchangeNode exchangeNode = (ExchangeNode) child;
          // 获取子节点所在的数据区域
          TRegionReplicaSet regionOfChild =
              context.getNodeDistribution(exchangeNode.getChild().getPlanNodeId()).getRegion();
          // 根据条件选择SinkNode类型
          MultiChildrenSinkNode newChild =
              memo.computeIfAbsent(
                  regionOfChild,
                  tRegionReplicaSet ->
                      needShuffleSinkNode
                          ? new ShuffleSinkNode(context.queryContext.getQueryId().genPlanNodeId())
                          : new IdentitySinkNode(context.queryContext.getQueryId().genPlanNodeId()));
          // 将原子节点添加到新SinkNode
          newChild.addChild(exchangeNode.getChild());
          // 添加数据通道信息
          newChild.addDownStreamChannelLocation(
              new DownStreamChannelLocation(exchangeNode.getPlanNodeId().toString()));
          // 更新ExchangeNode的子节点
          exchangeNode.setChild(newChild);
          // 设置SinkHandle索引
          exchangeNode.setIndexOfUpstreamSinkHandle(newChild.getCurrentLastIndex());
        }
      }
  }

  /**
   * 判断是否需要使用ShuffleSinkNode而不是IdentitySinkNode
   * 
   * 核心功能：根据查询语句的特征判断是否需要使用ShuffleSinkNode进行数据重分布。
   * ShuffleSinkNode主要用于支持排序操作的分布式查询。
   * 
   * 判断条件：
   * 条件1：按设备对齐查询且包含排序组件
   * 条件2：不是TopK查询（TopK查询使用IdentityNode）
   * 条件3：排序基于时间且没有表达式排序
   * 
   * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
   * - 该查询没有ORDER BY子句，所以返回false
   * - 使用IdentitySinkNode进行数据交换
   * 
   * @param queryStatement 查询语句
   * @param nodeGroupContext 节点分组上下文
   * @return true表示需要使用ShuffleSinkNode，false表示使用IdentitySinkNode
   */
  private boolean needShuffleSinkNode(
      QueryStatement queryStatement, NodeGroupContext nodeGroupContext) {
    // 获取排序组件
    OrderByComponent orderByComponent = queryStatement.getOrderByComponent();

    // 检查是否按设备对齐且包含排序
    if (nodeGroupContext.isAlignByDevice() && orderByComponent != null) {

      // TopKNode会使用IdentityNode而不是ShuffleSinkNode
      if (queryStatement.hasLimit()
          && !queryStatement.isOrderByBasedOnDevice()
          && queryStatement.getRowLimit() <= LIMIT_VALUE_USE_TOP_K) {
        return false;
      }

      // 需要ShuffleSinkNode的条件：排序列表不为空，且基于时间排序且没有表达式排序
      return !orderByComponent.getSortItemList().isEmpty()
          && (orderByComponent.isBasedOnTime() && !queryStatement.hasOrderByExpression());
    }

    return false;
  }

  /**
   * 应用分布式优化规则优化查询计划
   * 
   * 核心功能：对添加了ExchangeNode的查询计划应用一系列分布式优化规则，
   * 提升查询执行效率。
   * 
   * 优化器列表：
   * 1. LimitOffsetPushDown：将LIMIT/OFFSET下推到数据源附近
   * 2. ColumnInjectionPushDown：将列注入操作下推到数据源附近
   * 3. OrderByExpressionWithLimitChangeToTopK：将ORDER BY + LIMIT转换为TopK操作
   * 
   * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
   * - 该查询没有LIMIT/OFFSET，所以LimitOffsetPushDown不会生效
   * - 没有列注入操作，ColumnInjectionPushDown不会生效
   * - 没有ORDER BY + LIMIT，TopK转换不会生效
   * - 返回原始的查询计划结构
   * 
   * @param rootWithExchange 添加了ExchangeNode的查询计划根节点
   * @return 优化后的查询计划根节点
   */
  public PlanNode optimize(PlanNode rootWithExchange) {
    // 只对查询语句进行优化
    if (analysis.getStatement() != null && analysis.getStatement().isQuery()) {
      // 依次应用所有优化器
      for (PlanOptimizer optimizer : optimizers) {
        rootWithExchange = optimizer.optimize(rootWithExchange, analysis, context);
      }
    }
    return rootWithExchange;
  }

  /**
   * 将查询计划分割为多个查询片段
   * 
   * 核心功能：将完整的查询计划树分割为多个PlanFragment，每个片段可以独立执行，
   * 片段之间通过ExchangeNode进行数据交换。
   * 
   * 分割逻辑：
   * - 在ExchangeNode处进行分割，ExchangeNode作为片段的边界
   * - ExchangeNode的上游和下游分别属于不同的查询片段
   * - 每个片段可以部署到不同的计算节点上并行执行
   * 
   * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
   * - 如果查询涉及多个数据节点，会在ExchangeNode处分割
   * - 数据源节点和过滤操作可能在一个片段
   * - ExchangeNode和后续操作在另一个片段
   * 
   * @param root 优化后的查询计划根节点
   * @return 包含多个查询片段的SubPlan结构
   */
  public SubPlan splitFragment(PlanNode root) {
    // 创建FragmentBuilder进行片段分割
    FragmentBuilder fragmentBuilder = new FragmentBuilder();
    // 执行片段分割，返回包含根片段的SubPlan
    return fragmentBuilder.splitToSubPlan(root);
  }

  /**
   * 生成完整的分布式查询计划
   * 
   * 核心功能：将逻辑查询计划转换为完整的分布式执行计划，包含所有必要的
   * 数据交换节点、查询片段和片段实例。
   * 
   * 执行流程：
   * 步骤1：重写数据源节点（rewriteSource）
   * 步骤2：添加ExchangeNode（addExchangeNode）
   * 步骤3：优化查询计划（optimize）
   * 步骤4：设置输出列映射（仅查询语句）
   * 步骤5：分割查询片段（splitFragment）
   * 步骤6：标记根片段
   * 步骤7：规划片段实例（planFragmentInstances）
   * 步骤8：为根实例设置Sink（仅读操作）
   * 步骤9：构建分布式查询计划
   * 
   * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
   * - 重写数据源：SeriesScanNode → SeriesSourceNode
   * - 添加ExchangeNode：在数据边界处插入ExchangeNode
   * - 优化：应用分布式优化规则
   * - 分割片段：将查询计划分割为多个PlanFragment
   * - 生成实例：为每个片段创建FragmentInstance
   * - 设置根Sink：为根实例添加IdentitySinkNode
   * 
   * @return 完整的分布式查询计划
   */
  public DistributedQueryPlan planFragments() {
    // 步骤1：重写数据源节点，根据数据分布确定数据读取位置
    PlanNode rootAfterRewrite = rewriteSource();

    // 步骤2：添加ExchangeNode实现跨节点数据交换
    PlanNode rootWithExchange = addExchangeNode(rootAfterRewrite);
    
    // 步骤3：应用分布式优化规则
    PlanNode optimizedRootWithExchange = optimize(rootWithExchange);
    
    // 步骤4：为查询语句设置输出列映射
    if (analysis.getStatement() != null && analysis.getStatement().isQuery()) {
      analysis
          .getRespDatasetHeader()
          .setColumnToTsBlockIndexMap(optimizedRootWithExchange.getOutputColumnNames());
    }
    
    // 步骤5：将查询计划分割为多个PlanFragment
    SubPlan subPlan = splitFragment(optimizedRootWithExchange);
    
    // 步骤6：标记根Fragment为根节点
    subPlan.getPlanFragment().setRoot(true);
    
    // 步骤7：规划片段实例，支持并行执行
    List<FragmentInstance> fragmentInstances = planFragmentInstances(subPlan);

    // 步骤8：仅为读操作设置根实例的Sink
    if (context.getQueryType() == QueryType.READ) {
      setSinkForRootInstance(subPlan, fragmentInstances);
    }
    
    // 步骤9：构建并返回完整的分布式查询计划
    return new DistributedQueryPlan(
        logicalPlan.getContext(), subPlan, subPlan.getPlanFragmentList(), fragmentInstances);
  }

  /**
   * 规划查询片段实例，支持并行执行
   * 
   * 核心功能：将查询片段转换为具体的FragmentInstance，支持并行化执行。
   * 对于可并行的片段，会创建多个实例并分配不同的参数。
   * 
   * 实例化策略：
   * - 读查询：使用SimpleFragmentParallelPlanner，支持数据并行
   * - 写查询：使用WriteFragmentParallelPlanner，支持事务并行
   * 
   * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
   * - 如果是读查询，会为每个数据分区创建独立的FragmentInstance
   * - 每个实例处理不同的数据范围，实现并行查询
   * - 实例之间通过ExchangeNode进行数据交换
   * 
   * @param subPlan 包含查询片段的SubPlan结构
   * @return 查询片段实例列表，每个实例可独立执行
   */
  public List<FragmentInstance> planFragmentInstances(SubPlan subPlan) {
    // 根据查询类型选择并行规划器
    IFragmentParallelPlaner parallelPlaner =
        context.getQueryType() == QueryType.READ
            // 读查询使用简单并行规划器，支持数据并行
            ? new SimpleFragmentParallelPlanner(subPlan, analysis, context)
            // 写查询使用写入并行规划器，支持事务并行
            : new WriteFragmentParallelPlanner(subPlan, analysis, context);
    // 执行并行规划，返回FragmentInstance列表
    return parallelPlaner.parallelPlan();
  }

  /**
   * 为根查询片段实例设置Sink节点
   * 
   * 核心功能：为根查询片段实例设置IdentitySinkNode，用于将查询结果
   * 发送到结果节点（ResultNode）进行最终处理。
   * 
   * 设置流程：
   * 步骤1：从实例列表中查找根片段对应的实例
   * 步骤2：创建IdentitySinkNode，连接到虚拟结果节点
   * 步骤3：设置上游数据流信息
   * 步骤4：更新根片段的计划节点树
   * 
   * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
   * - 根片段实例处理最终的查询结果
   * - IdentitySinkNode将结果发送到结果节点
   * - 结果节点负责将数据返回给客户端
   * 
   * @param subPlan 包含根片段的SubPlan结构
   * @param instances 所有查询片段实例列表
   */
  public void setSinkForRootInstance(SubPlan subPlan, List<FragmentInstance> instances) {
    // 步骤1：查找根片段对应的实例
    FragmentInstance rootInstance = null;
    for (FragmentInstance instance : instances) {
      if (instance.getFragment().getId().equals(subPlan.getPlanFragment().getId())) {
        rootInstance = instance;
        break;
      }
    }
    // 正常情况下根实例不应为null
    if (rootInstance == null) {
      return;
    }

    // 步骤2：创建IdentitySinkNode，连接到虚拟结果节点
    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            context.getQueryId().genPlanNodeId(),
            Collections.singletonList(rootInstance.getFragment().getPlanNodeTree()),
            Collections.singletonList(
                new DownStreamChannelLocation(
                    context.getLocalDataBlockEndpoint(),
                    context.getResultNodeContext().getVirtualFragmentInstanceId().toThrift(),
                    context.getResultNodeContext().getVirtualResultNodeId().getId())));
    
    // 步骤3：设置上游数据流信息
    context
        .getResultNodeContext()
        .setUpStream(
            rootInstance.getHostDataNode().mPPDataExchangeEndPoint,
            rootInstance.getId(),
            sinkNode.getPlanNodeId());
    
    // 步骤4：更新根片段的计划节点树
    rootInstance.getFragment().setPlanNodeTree(sinkNode);
  }

  /**
   * 生成下一个查询片段ID
   * 
   * 核心功能：基于查询ID生成唯一的PlanFragmentId，
   * 确保每个查询片段都有唯一的标识符。
   * 
   * @return 新的查询片段ID
   */
  private PlanFragmentId getNextFragmentId() {
    return this.logicalPlan.getContext().getQueryId().genPlanFragmentId();
  }

  /**
   * 查询片段构建器内部类
   * 
   * 核心功能：负责将查询计划树分割为多个PlanFragment，
   * 在ExchangeNode处进行分割，形成分布式执行结构。
   * 
   * 分割策略：
   * - 在ExchangeNode处进行分割，形成片段边界
   * - ExchangeNode作为当前片段的叶子节点
   * - MultiChildrenSinkNode作为子片段的根节点
   * - 避免重复处理已访问的SinkNode
   */
  private class FragmentBuilder {

    /**
     * 将查询计划分割为SubPlan结构
     * 
     * 核心功能：从查询计划根节点开始，递归分割整个查询计划树，
     * 形成包含父子关系的SubPlan层次结构。
     * 
     * 以查询 SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10 为例：
     * - 如果查询涉及跨节点数据交换，会在ExchangeNode处分割
     * - 形成父片段（包含ExchangeNode）和子片段（包含MultiChildrenSinkNode）
     * - 每个片段可以独立部署到不同节点执行
     * 
     * @param root 查询计划根节点
     * @return 包含片段层次结构的SubPlan
     */
    public SubPlan splitToSubPlan(PlanNode root) {
      // 创建根SubPlan
      SubPlan rootSubPlan = createSubPlan(root);
      // 用于记录已访问的SinkNode，避免重复处理
      Set<PlanNodeId> visitedSinkNode = new HashSet<>();
      // 递归分割查询计划树
      splitToSubPlan(root, rootSubPlan, visitedSinkNode);
      return rootSubPlan;
    }

    /**
     * 递归分割查询计划树
     * 
     * 核心功能：深度优先遍历查询计划树，在ExchangeNode处进行分割，
     * 构建片段之间的父子关系。
     * 
     * 分割逻辑：
     * 1. 跳过WritePlanNode（写操作节点）
     * 2. 遇到ExchangeNode时进行分割
     * 3. 为未访问的MultiChildrenSinkNode创建子片段
     * 4. 递归处理所有子节点
     * 
     * @param root 当前处理的计划节点
     * @param subPlan 当前SubPlan
     * @param visitedSinkNode 已访问的SinkNode集合
     */
    private void splitToSubPlan(PlanNode root, SubPlan subPlan, Set<PlanNodeId> visitedSinkNode) {
      // 跳过WritePlanNode，写操作节点不进行分割
      if (root instanceof WritePlanNode) {
        return;
      }
      
      // 遇到ExchangeNode时进行片段分割
      if (root instanceof ExchangeNode) {
        ExchangeNode exchangeNode = (ExchangeNode) root;
        // 验证ExchangeNode的子节点必须是MultiChildrenSinkNode
        Validate.isTrue(
            exchangeNode.getChild() instanceof MultiChildrenSinkNode,
            "child of ExchangeNode must be MultiChildrenSinkNode");
        MultiChildrenSinkNode sinkNode = (MultiChildrenSinkNode) (exchangeNode.getChild());

        // 切断子树，使ExchangeNode成为当前片段的叶子节点
        exchangeNode.cleanChildren();

        // 如果SinkNode未被访问过，创建子SubPlan
        if (!visitedSinkNode.contains(sinkNode.getPlanNodeId())) {
          visitedSinkNode.add(sinkNode.getPlanNodeId());
          // 为SinkNode创建子片段
          SubPlan childSubPlan = createSubPlan(sinkNode);
          // 递归处理子片段
          splitToSubPlan(sinkNode, childSubPlan, visitedSinkNode);
          // 将子片段添加到当前SubPlan
          subPlan.addChild(childSubPlan);
        }
        return;
      }
      
      // 递归处理所有子节点
      for (PlanNode child : root.getChildren()) {
        splitToSubPlan(child, subPlan, visitedSinkNode);
      }
    }

    /**
     * 创建单个查询片段
     * 
     * 核心功能：基于计划节点创建PlanFragment，
     * 并包装为SubPlan结构。
     * 
     * @param root 片段的根节点
     * @return 包含单个片段的SubPlan
     */
    private SubPlan createSubPlan(PlanNode root) {
      // 创建新的PlanFragment，分配唯一ID
      PlanFragment fragment = new PlanFragment(getNextFragmentId(), root);
      // 包装为SubPlan返回
      return new SubPlan(fragment);
    }
  }
}