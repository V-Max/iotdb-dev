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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanSourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** <b>Optimization phase:</b> Logical plan planning. */
public class PredicatePushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    // 检查是否为查询类型语句，如果不是则直接返回原始计划
    if (analysis.getStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    // 获取查询语句对象
    QueryStatement queryStatement = analysis.getQueryStatement();
    // 如果是last查询或者没有值过滤器，则直接返回原始计划
    if (queryStatement.isLastQuery() || !analysis.hasValueFilter()) {
      return plan;
    }
    // 否则启动重写器进行谓词下推优化
    return plan.accept(
            new Rewriter(), new RewriterContext(analysis, context, queryStatement.isAlignByDevice()));
  }

  // 重写器类，负责遍历查询计划树并执行谓词下推优化
  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      // 默认访问方法，对于无法识别的节点类型抛出异常
      throw new IllegalArgumentException("Unexpected plan node: " + node);
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, RewriterContext context) {
      // 递归访问单个子节点的处理节点
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      // 更新子节点引用
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      // 处理多子节点的情况，创建一个新的子节点列表
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      // 递归访问每个子节点
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      // 更新所有子节点引用
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      // 检查该Filter节点是否来自WHERE子句
      if (!context.isFromWhere(node)) {
        // 不是来自WHERE子句的过滤器，作为普通单节点处理
        return visitSingleChildProcess(node, context);
      }

      // 将此过滤器节点设为待下推的过滤器
      context.setPushDownFilterNode(node);
      // 递归访问其子节点
      PlanNode rewrittenChild = node.getChild().accept(this, context);

      // 检查是否成功执行了谓词下推
      boolean enablePushDown = context.isEnablePushDown();
      // 重置上下文状态
      context.reset();

      if (enablePushDown) {
        // 如果谓词已成功下推，则返回重写后的子节点（移除了当前过滤器）
        return rewrittenChild;
      }
      // 否则返回原始过滤器节点
      return node;
    }

    @Override
    public PlanNode visitFullOuterTimeJoin(FullOuterTimeJoinNode node, RewriterContext context) {
      // 如果没有可继承的谓词，则直接返回
      if (context.hasNotInheritedPredicate()) {
        return node;
      }
      // 如果使用模板构建计划，则只支持下推到aligned scan节点
      if (context.isBuildPlanUseTemplate()) {
        return node;
      }

      // 获取需要下推的谓词表达式
      Expression inheritedPredicate = context.getInheritedPredicate();

      // 将谓词分解为合取项（AND条件）
      List<Expression> conjuncts = PredicateUtils.extractConjuncts(inheritedPredicate);
      List<PlanNode> children = node.getChildren();

      // 分别存储无法下推和合取下推到各个子节点的谓词
      List<Expression> cannotPushDownConjuncts = new ArrayList<>();
      List<List<Expression>> pushDownConjunctsForEachChild = new ArrayList<>();
      // 提取可以下推到每个子节点的谓词
      extractPushDownConjunctsForEachChild(
              conjuncts, children, cannotPushDownConjuncts, pushDownConjunctsForEachChild);

      // 如果所有谓词都不能下推，则直接返回原始节点
      if (cannotPushDownConjuncts.size() == conjuncts.size()) {
        return node;
      }

      // 标记启用了谓词下推
      context.setEnablePushDown(true);

      // 分别存储有谓词和无谓词的子节点
      List<PlanNode> childrenWithPredicate = new ArrayList<>();
      List<PlanNode> childrenWithoutPredicate = new ArrayList<>();
      // 为每个子节点设置下推谓词
      for (int i = 0; i < children.size(); i++) {
        SeriesScanSourceNode child = (SeriesScanSourceNode) children.get(i);
        if (pushDownConjunctsForEachChild.get(i).isEmpty()) {
          // 没有可下推的谓词
          childrenWithoutPredicate.add(child);
        } else {
          // 设置可下推的谓词
          child.setPushDownPredicate(
                  PredicateUtils.combineConjuncts(pushDownConjunctsForEachChild.get(i)));
          childrenWithPredicate.add(child);
        }
      }

      // 为有谓词的子节点构建内连接
      PlanNode left = planInnerTimeJoin(childrenWithPredicate, node.getMergeOrder(), context);
      // 为无谓词的子节点构建全外连接
      PlanNode right = planFullOuterTimeJoin(childrenWithoutPredicate, node.getMergeOrder(), context);

      // 最后将两部分结果进行左外连接
      PlanNode resultNode = planLeftOuterTimeJoin(left, right, node.getMergeOrder(), context);

      // 处理无法下推的谓词
      if (!cannotPushDownConjuncts.isEmpty()) {
        // 为无法下推的谓词创建过滤器
        resultNode = planFilter(
                resultNode,
                PredicateUtils.combineConjuncts(cannotPushDownConjuncts),
                context,
                true);
      } else {
        // 所有谓词都已下推，添加必要的转换和投影节点
        resultNode = planTransform(resultNode, context);
        resultNode = planProject(resultNode, context);
      }
      return resultNode;
    }

    // 提取可以下推到每个子节点的谓词合取项
    private void extractPushDownConjunctsForEachChild(
            List<Expression> conjuncts,
            List<PlanNode> children,
            List<Expression> cannotPushDownConjuncts,
            List<List<Expression>> pushDownConjunctsForEachChild) {
      // 存储每个数据源路径对应的下推谓词
      Map<PartialPath, List<Expression>> pushDownConjunctsMap = new HashMap<>();
      // 遍历所有谓词合取项
      for (Expression conjunct : conjuncts) {
        // 检查谓词是否可以下推到数据源
        if (!PredicateUtils.predicateCanPushDownToSource(conjunct)) {
          // 不能下推的谓词
          cannotPushDownConjuncts.add(conjunct);
          continue;
        }

        // 提取谓词中涉及的数据源路径
        PartialPath extractedSourcePath = PredicateUtils.extractPredicateSourceSymbol(conjunct);
        if (extractedSourcePath == null) {
          // 无法提取数据源路径的谓词不能下推
          cannotPushDownConjuncts.add(conjunct);
        } else {
          // 存储谓词到对应数据源路径的映射
          pushDownConjunctsMap
                  .computeIfAbsent(extractedSourcePath, k -> new ArrayList<>())
                  .add(conjunct);
        }
      }

      // 为每个子节点分配对应的下推谓词
      for (PlanNode child : children) {
        // 确保子节点是SeriesScanSourceNode类型
        checkArgument(
                child instanceof SeriesScanSourceNode, "Unexpected node type: " + child.getClass());
        // 获取子节点对应的分区路径
        PartialPath sourcePath = ((SeriesScanSourceNode) child).getPartitionPath();
        if (sourcePath instanceof MeasurementPath) {
          // 普通测量路径，直接匹配
          pushDownConjunctsForEachChild.add(
                  pushDownConjunctsMap.getOrDefault(sourcePath, Collections.emptyList()));
        } else if (sourcePath instanceof AlignedPath) {
          // 对齐路径，使用设备路径进行匹配
          pushDownConjunctsForEachChild.add(
                  pushDownConjunctsMap.getOrDefault(
                          sourcePath.getDevicePath(), Collections.emptyList()));
        } else {
          // 不支持的路径类型，抛出异常
          throw new IllegalArgumentException("sourcePath must be MeasurementPath or AlignedPath");
        }
      }
    }

    // 构建内时间连接节点
    private PlanNode planInnerTimeJoin(
            List<PlanNode> children, Ordering mergeOrder, RewriterContext context) {
      PlanNode resultNode = null;
      // 根据子节点数量决定如何构建连接
      if (children.size() == 1) {
        // 只有一个子节点，直接返回该子节点
        resultNode = children.get(0);
      } else if (children.size() > 1) {
        // 多个子节点，创建内连接节点
        resultNode = new InnerTimeJoinNode(context.genPlanNodeId(), children, mergeOrder);
      }
      return resultNode;
    }

    // 构建全外时间连接节点
    private PlanNode planFullOuterTimeJoin(
            List<PlanNode> children, Ordering mergeOrder, RewriterContext context) {
      PlanNode resultNode = null;
      // 根据子节点数量决定如何构建连接
      if (children.size() == 1) {
        // 只有一个子节点，直接返回该子节点
        resultNode = children.get(0);
      } else if (children.size() > 1) {
        // 多个子节点，创建全外连接节点
        resultNode = new FullOuterTimeJoinNode(context.genPlanNodeId(), mergeOrder, children);
      }
      return resultNode;
    }

    // 构建左外时间连接节点
    private PlanNode planLeftOuterTimeJoin(
            PlanNode left, PlanNode right, Ordering mergeOrder, RewriterContext context) {
      // 确保至少有一个非空节点
      checkState(left != null || right != null);
      PlanNode resultNode;
      // 根据节点存在情况决定结果
      if (left == null) {
        // 只有右节点
        resultNode = right;
      } else if (right == null) {
        // 只有左节点
        resultNode = left;
      } else {
        // 左右节点都存在，创建左外连接
        resultNode = new LeftOuterTimeJoinNode(context.genPlanNodeId(), mergeOrder, left, right);
      }
      return resultNode;
    }

    // 构建过滤器节点
    private PlanNode planFilter(
            PlanNode child, Expression predicate, RewriterContext context, boolean isFromWhere) {
      // 获取待下推的过滤器节点
      FilterNode pushDownFilterNode = context.getPushDownFilterNode();
      // 创建新的过滤器节点，继承原过滤器的输出表达式和其他属性
      return new FilterNode(
              context.genPlanNodeId(),
              child,
              pushDownFilterNode.getOutputExpressions(),
              predicate,
              pushDownFilterNode.isKeepNull(),
              pushDownFilterNode.getScanOrder(),
              isFromWhere);
    }

    @Override
    public PlanNode visitAlignedSeriesScan(AlignedSeriesScanNode node, RewriterContext context) {
      // 如果没有可继承的谓词，则直接返回
      if (context.hasNotInheritedPredicate()) {
        return node;
      }

      // 非模板模式下，使用通用的SeriesScanSource处理逻辑
      if (!context.isBuildPlanUseTemplate()) {
        return visitSeriesScanSource(node, context);
      }
      // 模板模式下，获取模板信息
      TemplatedInfo templatedInfo = context.getTemplatedInfo();
      checkState(templatedInfo != null, "TemplatedInfo should not be null");

      // 获取要下推的谓词
      Expression inheritedPredicate = context.getInheritedPredicate();
      // 检查是否可以下推谓词到模板化的扫描节点
      if (context.enablePushDownUseTemplate()
              || PredicateUtils.predicateCanPushDownToSource(inheritedPredicate)) {
        // 设置下推谓词
        node.setPushDownPredicate(inheritedPredicate);
        // 更新模板信息中的下推谓词
        if (!templatedInfo.hasPushDownPredicate()) {
          templatedInfo.setPushDownPredicate(inheritedPredicate);
        }
        // 标记模板和普通谓词下推都已启用
        context.setEnablePushDownUseTemplate(true);
        context.setEnablePushDown(true);

        // 添加投影节点
        return planProject(node, context);
      }

      // 无法下推谓词，返回原始节点
      return node;
    }

    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, RewriterContext context) {
      // 如果没有可继承的谓词，则直接返回
      if (context.hasNotInheritedPredicate()) {
        return node;
      }
      // 非模板模式下，使用通用的SeriesScanSource处理逻辑
      if (!context.isBuildPlanUseTemplate()) {
        return visitSeriesScanSource(node, context);
      }
      // 模板模式下，只支持下推到aligned scan节点
      return node;
    }

    @Override
    public PlanNode visitSeriesScanSource(SeriesScanSourceNode node, RewriterContext context) {
      // 获取要下推的谓词
      Expression inheritedPredicate = context.getInheritedPredicate();

      // 将谓词分解为合取项
      List<Expression> conjuncts = PredicateUtils.extractConjuncts(inheritedPredicate);
      // 分别存储可下推和不可下推的谓词合取项
      List<Expression> canPushDownConjuncts = new ArrayList<>();
      List<Expression> cannotPushDownConjuncts = new ArrayList<>();
      // 分类处理每个谓词合取项
      for (Expression conjunct : conjuncts) {
        if (PredicateUtils.predicateCanPushDownToSource(conjunct)) {
          canPushDownConjuncts.add(conjunct);
        } else {
          cannotPushDownConjuncts.add(conjunct);
        }
      }

      // 如果没有可下推的谓词，返回原始节点
      if (canPushDownConjuncts.isEmpty()) {
        return node;
      }

      // 设置可下推的谓词到扫描节点
      node.setPushDownPredicate(PredicateUtils.combineConjuncts(canPushDownConjuncts));
      // 标记谓词下推已启用
      context.setEnablePushDown(true);

      if (cannotPushDownConjuncts.isEmpty()) {
        // 所有谓词都可下推，添加必要的转换和投影节点
        PlanNode resultNode = planTransform(node, context);
        resultNode = planProject(resultNode, context);
        return resultNode;
      } else {
        // 部分谓词无法下推，为这些谓词创建过滤器
        return planFilter(
                node, PredicateUtils.combineConjuncts(cannotPushDownConjuncts), context, true);
      }
    }

    // 创建转换节点
    private PlanNode planTransform(PlanNode resultNode, RewriterContext context) {
      // 获取待下推的过滤器节点
      FilterNode pushDownFilterNode = context.getPushDownFilterNode();
      // 获取输出表达式列表
      Expression[] outputExpressions = pushDownFilterNode.getOutputExpressions();
      // 检查是否需要进行转换
      boolean needTransform = false;
      for (Expression expression : outputExpressions) {
        if (ExpressionAnalyzer.checkIsNeedTransform(expression)) {
          needTransform = true;
          break;
        }
      }

      // 如果不需要转换，直接返回结果节点
      if (!needTransform) {
        return resultNode;
      }
      // 否则创建转换节点
      return new TransformNode(
              context.genPlanNodeId(),
              resultNode,
              outputExpressions,
              pushDownFilterNode.isKeepNull(),
              pushDownFilterNode.getScanOrder());
    }

    /**
     * ProjectNode is used to project the output columns of the child node.
     *
     * <p>There are two cases where ProjectNode is used:
     * <li>Because of the removal of the FilterNode (FilterAndProjectOperator), we need a
     *     ProjectNode to do the projection.
     * <li>For ALIGN_BY_DEVICE query, the ProjectNode is used to ensure the order of the output
     *     columns is consistent with before optimization (required by MergeSortOperator).
     */
    private PlanNode planProject(PlanNode resultNode, RewriterContext context) {
      // 获取待下推的过滤器节点
      FilterNode pushDownFilterNode = context.getPushDownFilterNode();
      // 如果结果节点已经是转换节点，不需要额外的投影（转换节点包含投影功能）
      if (resultNode instanceof TransformNode) {
        return resultNode;
      }

      // 模板模式下，创建不带列名的投影节点
      if (context.isBuildPlanUseTemplate()) {
        return new ProjectNode(context.genPlanNodeId(), resultNode, null);
      }

      // 对于ALIGN_BY_DEVICE查询或输出列数量与子节点不同的情况，创建带列名的投影节点
      if (context.isAlignByDevice()
              || (pushDownFilterNode.getOutputColumnNames().size()
              != pushDownFilterNode.getChild().getOutputColumnNames().size())) {
        return new ProjectNode(
                context.genPlanNodeId(), resultNode, pushDownFilterNode.getOutputColumnNames());
      }
      // 其他情况不需要投影
      return resultNode;
    }
  }

  // 重写器上下文，存储优化过程中的状态和配置信息
  private static class RewriterContext {

    private final QueryId queryId;           // 查询ID
    private final boolean isAlignByDevice;   // 是否按设备对齐查询
    private final boolean isBuildPlanUseTemplate; // 是否使用模板构建计划
    private final TemplatedInfo templatedInfo;   // 模板信息
    private final Function<FilterNode, Boolean> filterNodeFromWhereChecker; // 检查过滤器是否来自WHERE子句的函数

    private FilterNode pushDownFilterNode;  // 当前正在处理的待下推过滤器节点

    private boolean enablePushDown = false;      // 是否启用谓词下推
    private boolean enablePushDownUseTemplate = false; // 是否启用基于模板的谓词下推

    // 构造函数，初始化上下文信息
    private RewriterContext(Analysis analysis, MPPQueryContext context, boolean isAlignByDevice) {
      this.queryId = context.getQueryId();
      this.isAlignByDevice = isAlignByDevice;
      this.isBuildPlanUseTemplate = analysis.allDevicesInOneTemplate();
      this.templatedInfo = context.getTypeProvider().getTemplatedInfo();
      this.filterNodeFromWhereChecker = analysis::fromWhere; // 使用方法引用创建检查器函数
    }

    // 生成新的计划节点ID
    public PlanNodeId genPlanNodeId() {
      return queryId.genPlanNodeId();
    }

    // 获取是否按设备对齐查询的标志
    public boolean isAlignByDevice() {
      return isAlignByDevice;
    }

    // 获取是否使用模板构建计划的标志
    public boolean isBuildPlanUseTemplate() {
      return isBuildPlanUseTemplate;
    }

    // 获取模板信息
    public TemplatedInfo getTemplatedInfo() {
      return templatedInfo;
    }

    // 检查过滤器节点是否来自WHERE子句
    public boolean isFromWhere(FilterNode filterNode) {
      return Boolean.TRUE.equals(filterNodeFromWhereChecker.apply(filterNode));
    }

    // 获取当前待下推的过滤器节点
    public FilterNode getPushDownFilterNode() {
      return pushDownFilterNode;
    }

    // 设置当前待下推的过滤器节点
    public void setPushDownFilterNode(FilterNode pushDownFilterNode) {
      this.pushDownFilterNode = pushDownFilterNode;
    }

    // 检查是否没有可继承的谓词
    public boolean hasNotInheritedPredicate() {
      return pushDownFilterNode == null;
    }

    // 获取要继承的谓词
    public Expression getInheritedPredicate() {
      checkState(pushDownFilterNode != null);
      return pushDownFilterNode.getPredicate();
    }

    // 获取是否启用谓词下推的标志
    public boolean isEnablePushDown() {
      return enablePushDown;
    }

    // 设置是否启用谓词下推
    public void setEnablePushDown(boolean enablePushDown) {
      this.enablePushDown = enablePushDown;
    }

    // 获取是否启用基于模板的谓词下推
    public boolean enablePushDownUseTemplate() {
      return enablePushDownUseTemplate;
    }

    // 设置是否启用基于模板的谓词下推
    public void setEnablePushDownUseTemplate(boolean enablePushDownUseTemplate) {
      this.enablePushDownUseTemplate = enablePushDownUseTemplate;
    }

    // 重置上下文状态
    public void reset() {
      this.pushDownFilterNode = null;
      this.enablePushDown = false;
    }
  }
}
