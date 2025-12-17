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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.TransformToViewExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.DeviceSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.SeriesSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.ShowLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.ENDTIME;

/**
 * LogicalPlanVisitor类实现了StatementVisitor接口，是IoTDB查询引擎中负责将SQL语句转换为逻辑执行计划的关键组件。
 * 该类采用访问者模式，为每种类型的SQL语句提供专门的访问方法，将语法分析后的Statement对象转换为对应的PlanNode对象。
 * 这些PlanNode构成了查询的逻辑执行计划，描述了数据处理的各个步骤。
 * 
 * 主要功能：
 * 1. 将各类SQL语句（查询、插入、DDL、元数据查询等）转换为逻辑计划节点
 * 2. 构建查询执行的操作树，包括扫描、过滤、聚合、排序等操作
 * 3. 处理表结构操作（创建时间序列、修改时间序列等）
 * 4. 处理数据插入操作（单行、多行、批量等）
 * 5. 处理元数据查询和管理操作
 */
public class LogicalPlanVisitor extends StatementVisitor<PlanNode, MPPQueryContext> {

  /**
   * 分析后的查询上下文信息，包含查询相关的元数据信息、路径映射等
   */
  private final Analysis analysis;

  /**
   * 构造函数
   * @param analysis 分析后的查询上下文信息
   */
  public LogicalPlanVisitor(Analysis analysis) {
    this.analysis = analysis;
  }

  /**
   * 访问通用StatementNode节点，对于不支持的语句类型抛出异常
   * @param node StatementNode节点
   * @param context 查询上下文
   * @return 对应的PlanNode
   * @throws UnsupportedOperationException 当遇到不支持的语句类型时抛出
   */
  @Override
  public PlanNode visitNode(StatementNode node, MPPQueryContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  /**
   * 处理EXPLAIN ANALYZE语句，用于分析查询执行计划
   * @param explainAnalyzeStatement EXPLAIN ANALYZE语句对象
   * @param context 查询上下文
   * @return ExplainAnalyzeNode节点，包装了原始查询的执行计划
   */
  @Override
  public PlanNode visitExplainAnalyze(
      ExplainAnalyzeStatement explainAnalyzeStatement, MPPQueryContext context) {
    PlanNode root = visitQuery(explainAnalyzeStatement.getQueryStatement(), context);
    root = 
        new ExplainAnalyzeNode(
            context.getQueryId().genPlanNodeId(),
            root,
            explainAnalyzeStatement.isVerbose(),
            context.getLocalQueryId(),
            context.getTimeOut());
    context.getTypeProvider().setType(ColumnHeaderConstant.EXPLAIN_ANALYZE, TSDataType.TEXT);
    return root;
  }

  /**
   * 处理查询语句，构建逻辑查询计划
   * 该方法负责将SQL查询语句转换为逻辑执行计划，是查询优化的核心入口点
   * 
   * @param queryStatement 查询语句对象，包含SQL查询的所有语法元素
   * @param context 查询上下文，包含查询ID、会话信息等执行环境
   * @return 查询计划的根节点，表示整个查询的逻辑执行树
   */
  @Override
  public PlanNode visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
    // 检查是否所有设备都在同一个模板中，如果是则使用模板化逻辑计划
    if (analysis.allDevicesInOneTemplate()) {
      return new TemplatedLogicalPlan(analysis, queryStatement, context).visitQuery();
    }

    // 创建逻辑计划构建器，用于构建查询执行计划
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // 处理Last查询，用于获取时间序列的最新数据点
    if (queryStatement.isLastQuery()) {
      // 构建Last查询计划
      planBuilder = planBuilder.planLast(analysis, analysis.getTimeseriesOrderingForLastQuery());

      // 处理排序，如果查询有ORDER BY且不仅仅是按时间序列排序
      if (queryStatement.hasOrderBy() && !queryStatement.onlyOrderByTimeseries()) {
        planBuilder = planBuilder.planOrderBy(queryStatement.getSortItemList());
      }

      // 处理分页（OFFSET和LIMIT）
      planBuilder =
          planBuilder
              .planOffset(queryStatement.getRowOffset())
              .planLimit(queryStatement.getRowLimit());
      return planBuilder.getRoot();
    }

    // 处理按设备对齐的查询
    if (queryStatement.isAlignByDevice()) {
      // 创建设备到子计划的映射
      Map<String, PlanNode> deviceToSubPlanMap = new LinkedHashMap<>();
      // 为每个设备构建子查询计划
      for (PartialPath device : analysis.getDeviceList()) {
        String deviceName = device.getFullPath();
        LogicalPlanBuilder subPlanBuilder = new LogicalPlanBuilder(analysis, context);
        // 构建单个设备的查询主体
        subPlanBuilder =
            subPlanBuilder.withNewRoot(
                visitQueryBody(
                    queryStatement,
                    analysis.getDeviceToSourceExpressions().get(deviceName),
                    analysis.getDeviceToSourceTransformExpressions().get(deviceName),
                    analysis.getDeviceToWhereExpression() != null
                        ? analysis.getDeviceToWhereExpression().get(deviceName)
                        : null,
                    analysis.getDeviceToAggregationExpressions().get(deviceName),
                    analysis.getDeviceToGroupByExpression() != null
                        ? analysis.getDeviceToGroupByExpression().get(deviceName)
                        : null,
                    context));
        // 处理排序下推，如果查询需要下推排序操作
        if (queryStatement.needPushDownSort()) {
          subPlanBuilder =
              subPlanBuilder.planOrderBy(
                  analysis.getDeviceToOrderByExpressions().get(deviceName),
                  analysis.getDeviceToSortItems().get(deviceName));
        }
        deviceToSubPlanMap.put(deviceName, subPlanBuilder.getRoot());
      }

      // 转换为ALIGN BY DEVICE视图
      planBuilder =
          planBuilder.planDeviceView(
              deviceToSubPlanMap,
              analysis.getDeviceViewOutputExpressions(),
              analysis.getDeviceViewInputIndexesMap(),
              analysis.getSelectExpressions(),
              queryStatement,
              analysis);
    } else {
      // 处理普通查询（非按设备对齐）
      planBuilder =
          planBuilder.withNewRoot(
              visitQueryBody(
                  queryStatement,
                  analysis.getSourceExpressions(),
                  analysis.getSourceTransformExpressions(),
                  analysis.getWhereExpression(),
                  analysis.getAggregationExpressions(),
                  analysis.getGroupByExpression(),
                  context));
    }

    // 处理聚合查询的HAVING子句和结果转换
    if (queryStatement.isAggregationQuery()) {
      planBuilder =
          planBuilder.planHavingAndTransform(
              analysis.getHavingExpression(),
              analysis.getSelectExpressions(),
              analysis.getOrderByExpressions(),
              queryStatement.isGroupByTime(),
              queryStatement.getResultTimeOrder());
    }

    // 处理排序，如果不需要下推排序操作
    if (!queryStatement.needPushDownSort()) {
      planBuilder = planBuilder.planOrderBy(queryStatement, analysis);
    }

    // 处理其他上游节点：填充（FILL）、分页（OFFSET）
    planBuilder =
        planBuilder
            .planFill(analysis.getFillDescriptor(), queryStatement.getResultTimeOrder())
            .planOffset(queryStatement.getRowOffset());

    // 处理LIMIT分页，如果不使用TopK节点或查询有OFFSET
    if (!analysis.isUseTopKNode() || queryStatement.hasOffset()) {
      planBuilder = planBuilder.planLimit(queryStatement.getRowLimit());
    }

    // 处理模型推理，如果查询包含模型推理
    if (queryStatement.hasModelInference()) {
      planBuilder.planInference(analysis);
    }

    // 处理SELECT INTO查询，将查询结果写入指定路径
    if (queryStatement.isAlignByDevice()) {
      planBuilder = planBuilder.planDeviceViewInto(analysis.getDeviceViewIntoPathDescriptor());
    } else {
      planBuilder = planBuilder.planInto(analysis.getIntoPathDescriptor());
    }

    // 返回查询计划的根节点
    return planBuilder.getRoot();
  }

  /**
   * 处理查询语句的主体部分，根据是否包含聚合表达式构建不同类型的查询计划
   * 该方法负责构建查询的核心逻辑计划，区分原始数据查询和聚合查询两种场景
   * 
   * @param queryStatement 查询语句对象，包含查询的所有语法元素
   * @param sourceExpressions 源表达式集合，包含所有查询涉及的时间序列路径
   * @param sourceTransformExpressions 源转换表达式集合，包含需要在数据源层面进行的转换操作
   * @param whereExpression WHERE条件表达式，用于数据过滤
   * @param aggregationExpressions 聚合表达式集合，如sum、avg等，如果为null表示是原始数据查询
   * @param groupByExpression GROUP BY表达式，用于分组聚合
   * @param context 查询上下文，包含查询ID、会话信息等执行环境
   * @return 查询主体部分的计划节点，表示查询的核心逻辑执行树
   */
  public PlanNode visitQueryBody(
      QueryStatement queryStatement,
      Set<Expression> sourceExpressions,
      Set<Expression> sourceTransformExpressions,
      Expression whereExpression,
      Set<Expression> aggregationExpressions,
      Expression groupByExpression,
      MPPQueryContext context) {
    // 创建逻辑计划构建器，用于构建查询执行计划
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    
    // 判断是否为聚合查询：如果aggregationExpressions为null，则是原始数据查询
    if (aggregationExpressions == null) {
      // 原始数据查询处理逻辑
      // 构建原始数据源计划，然后应用WHERE条件和源转换
      planBuilder =
          planBuilder
              .planRawDataSource(
                  sourceExpressions,                          // 源表达式集合
                  queryStatement.getResultTimeOrder(),        // 结果时间排序方式
                  0,                                          // 偏移量（原始查询不下推offset）
                  pushDownLimitToScanNode(queryStatement, analysis), // 下推的LIMIT值
                  analysis.isLastLevelUseWildcard())          // 是否在最后一级使用通配符
              .planWhereAndSourceTransform(
                  whereExpression,                            // WHERE条件表达式
                  sourceTransformExpressions,                 // 源转换表达式集合
                  queryStatement.isGroupByTime(),             // 是否按时间分组
                  queryStatement.getResultTimeOrder());       // 结果时间排序方式
    } else {
      // 聚合查询处理逻辑
      // 构建原始数据源计划，然后应用WHERE条件和源转换
      planBuilder =
          planBuilder
              .planRawDataSource(
                  sourceExpressions,                          // 源表达式集合
                  queryStatement.getResultTimeOrder(),        // 结果时间排序方式
                  0,                                          // 偏移量（聚合查询不下推offset）
                  0,                                          // 限制值（聚合查询不下推LIMIT）
                  analysis.isLastLevelUseWildcard())          // 是否在最后一级使用通配符
              .planWhereAndSourceTransform(
                  whereExpression,                            // WHERE条件表达式
                  sourceTransformExpressions,                 // 源转换表达式集合
                  queryStatement.isGroupByTime(),             // 是否按时间分组
                  queryStatement.getResultTimeOrder());       // 结果时间排序方式

      // 判断是否需要输出部分聚合结果
      // 在以下情况下需要输出部分聚合结果：
      // 1. 按层级分组（GROUP BY LEVEL）
      // 2. 按标签分组（GROUP BY TAG）
      // 3. 按时间分组且存在时间窗口重叠
      boolean outputPartial =
          queryStatement.isGroupByLevel()                     // 按层级分组
              || queryStatement.isGroupByTag()                // 按标签分组
              || (queryStatement.isGroupByTime()              // 按时间分组
                  && analysis.getGroupByTimeParameter().hasOverlap()); // 时间窗口存在重叠
      
      // 根据是否需要输出部分聚合结果确定聚合步骤
      AggregationStep curStep = outputPartial ? AggregationStep.PARTIAL : AggregationStep.SINGLE;
      
      // 构建原始数据聚合计划
      planBuilder =
          planBuilder.planRawDataAggregation(
              aggregationExpressions,                         // 聚合表达式集合
              groupByExpression,                              // GROUP BY表达式
              analysis.getGroupByTimeParameter(),             // 时间分组参数
              analysis.getGroupByParameter(),                 // 分组参数
              queryStatement.isOutputEndTime(),               // 是否输出结束时间
              curStep,                                        // 当前聚合步骤
              queryStatement.getResultTimeOrder());           // 结果时间排序方式

      // 处理滑动窗口聚合（当按时间分组且存在时间窗口重叠时）
      if (queryStatement.isGroupByTime() && analysis.getGroupByTimeParameter().hasOverlap()) {
        // 确定滑动窗口聚合的步骤
        // 如果是按层级或标签分组，使用中间步骤，否则使用最终步骤
        curStep =
            (queryStatement.isGroupByLevel() || queryStatement.isGroupByTag())
                ? AggregationStep.INTERMEDIATE
                : AggregationStep.FINAL;
        
        // 构建滑动窗口聚合计划
        planBuilder =
            planBuilder.planSlidingWindowAggregation(
                aggregationExpressions,                       // 聚合表达式集合
                analysis.getGroupByTimeParameter(),           // 时间分组参数
                curStep,                                      // 当前聚合步骤
                queryStatement.getResultTimeOrder());         // 结果时间排序方式
      }

      // 处理按层级分组（GROUP BY LEVEL）
      if (queryStatement.isGroupByLevel()) {
        planBuilder =
            planBuilder.planGroupByLevel(
                analysis.getCrossGroupByExpressions(),        // 跨设备分组表达式
                analysis.getGroupByTimeParameter(),           // 时间分组参数
                queryStatement.getResultTimeOrder());         // 结果时间排序方式
      } else if (queryStatement.isGroupByTag()) {
        // 处理按标签分组（GROUP BY TAG）
        planBuilder =
            planBuilder.planGroupByTag(
                analysis.getCrossGroupByExpressions(),        // 跨设备分组表达式
                analysis.getTagKeys(),                        // 标签键集合
                analysis.getTagValuesToGroupedTimeseriesOperands(), // 标签值到时间序列操作数的映射
                analysis.getGroupByTimeParameter(),           // 时间分组参数
                queryStatement.getResultTimeOrder());         // 结果时间排序方式
      }

      // 处理输出结束时间
      // 如果查询需要输出结束时间，需要特殊处理
      if (queryStatement.isOutputEndTime()) {
        // 在类型提供器中设置结束时间的类型为INT64
        context.getTypeProvider().setType(ENDTIME, TSDataType.INT64);
        // 如果是时间分组查询，需要注入结束时间列
        if (queryStatement.isGroupByTime()) {
          planBuilder =
              planBuilder.planEndTimeColumnInject(
                  analysis.getGroupByTimeParameter(),         // 时间分组参数
                  queryStatement.getResultTimeOrder().isAscending()); // 排序方向是否为升序
        }
      }
    }

    // 返回查询主体部分的根节点
    return planBuilder.getRoot();
  }

  /**
   * 判断是否可以将LIMIT子句下推到ScanNode，以优化查询性能
   * @param queryStatement 查询语句对象
   * @param analysis 分析上下文
   * @return 下推的限制值，如果不能下推则返回0
   */
  static long pushDownLimitToScanNode(QueryStatement queryStatement, Analysis analysis) {
    // `order by time|device LIMIT N align by device` and no value filter,
    // can push down limitValue to ScanNode
    if (queryStatement.isAlignByDevice()
        && queryStatement.hasLimit()
        && !analysis.hasValueFilter()
        && (queryStatement.isOrderByBasedOnDevice() || queryStatement.isOrderByBasedOnTime())) {

      // both `offset` and `limit` exist, push `limit+offset` down as limitValue
      if (queryStatement.hasOffset()) {
        return queryStatement.getRowOffset() + queryStatement.getRowLimit();
      }

      // only `limit` exist, push `limit` down as limitValue
      return queryStatement.getRowLimit();
    }

    return 0;
  }

  /**
   * 处理创建时间序列语句
   * @param createTimeSeriesStatement 创建时间序列语句对象
   * @param context 查询上下文
   * @return CreateTimeSeriesNode节点，用于创建单个时间序列
   */
  @Override
  public PlanNode visitCreateTimeseries(
      CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
    return new CreateTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        createTimeSeriesStatement.getPath(),
        createTimeSeriesStatement.getDataType(),
        createTimeSeriesStatement.getEncoding(),
        createTimeSeriesStatement.getCompressor(),
        createTimeSeriesStatement.getProps(),
        createTimeSeriesStatement.getTags(),
        createTimeSeriesStatement.getAttributes(),
        createTimeSeriesStatement.getAlias());
  }

  /**
   * 处理创建对齐时间序列语句
   * @param createAlignedTimeSeriesStatement 创建对齐时间序列语句对象
   * @param context 查询上下文
   * @return 对应的时间序列创建节点
   */
  @Override
  public PlanNode visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, MPPQueryContext context) {
    return new CreateAlignedTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        createAlignedTimeSeriesStatement.getDevicePath(),
        createAlignedTimeSeriesStatement.getMeasurements(),
        createAlignedTimeSeriesStatement.getDataTypes(),
        createAlignedTimeSeriesStatement.getEncodings(),
        createAlignedTimeSeriesStatement.getCompressors(),
        createAlignedTimeSeriesStatement.getAliasList(),
        createAlignedTimeSeriesStatement.getTagsList(),
        createAlignedTimeSeriesStatement.getAttributesList());
  }

  /**
   * 处理内部使用的创建时间序列语句
   * @param internalCreateTimeSeriesStatement 内部创建时间序列语句对象
   * @param context 查询上下文
   * @return InternalCreateTimeSeriesNode节点
   */
  @Override
  public PlanNode visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement,
      MPPQueryContext context) {
    int size = internalCreateTimeSeriesStatement.getMeasurements().size();

    MeasurementGroup measurementGroup = new MeasurementGroup();
    for (int i = 0; i < size; i++) {
      measurementGroup.addMeasurement(
          internalCreateTimeSeriesStatement.getMeasurements().get(i),
          internalCreateTimeSeriesStatement.getTsDataTypes().get(i),
          internalCreateTimeSeriesStatement.getEncodings().get(i),
          internalCreateTimeSeriesStatement.getCompressors().get(i));
    }

    return new InternalCreateTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        internalCreateTimeSeriesStatement.getDevicePath(),
        measurementGroup,
        internalCreateTimeSeriesStatement.isAligned());
  }

  /**
   * 处理创建多个时间序列语句
   * @param createMultiTimeSeriesStatement 创建多个时间序列语句对象
   * @param context 查询上下文
   * @return 对应的多时间序列创建节点
   */
  @Override
  public PlanNode visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, MPPQueryContext context) {
    return new CreateMultiTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        createMultiTimeSeriesStatement.getPaths(),
        createMultiTimeSeriesStatement.getDataTypes(),
        createMultiTimeSeriesStatement.getEncodings(),
        createMultiTimeSeriesStatement.getCompressors(),
        createMultiTimeSeriesStatement.getPropsList(),
        createMultiTimeSeriesStatement.getAliasList(),
        createMultiTimeSeriesStatement.getTagsList(),
        createMultiTimeSeriesStatement.getAttributesList());
  }

  /**
   * 处理内部使用的创建多个时间序列语句
   * @param internalCreateMultiTimeSeriesStatement 内部创建多个时间序列语句对象
   * @param context 查询上下文
   * @return InternalCreateMultiTimeSeriesNode节点
   */
  @Override
  public PlanNode visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement,
      MPPQueryContext context) {
    return new InternalCreateMultiTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        internalCreateMultiTimeSeriesStatement.getDeviceMap());
  }

  /**
   * 处理ALTER TIMESERIES语句
   * @param alterTimeSeriesStatement 修改时间序列语句对象
   * @param context 查询上下文
   * @return AlterTimeSeriesNode节点，用于修改时间序列的标签、属性或别名
   */
  @Override
  public PlanNode visitAlterTimeSeries(
      AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
    return new AlterTimeSeriesNode(
        context.getQueryId().genPlanNodeId(),
        alterTimeSeriesStatement.getPath(),
        alterTimeSeriesStatement.getAlterType(),
        alterTimeSeriesStatement.getAlterMap(),
        alterTimeSeriesStatement.getAlias(),
        alterTimeSeriesStatement.getTagsMap(),
        alterTimeSeriesStatement.getAttributesMap(),
        alterTimeSeriesStatement.isAlterView());
  }

  /**
   * 处理INSERT TABLET语句，用于批量插入数据
   * @param insertTabletStatement 插入表数据语句对象
   * @param context 查询上下文
   * @return InsertTabletNode节点，用于高效批量插入数据
   */
  @Override
  public PlanNode visitInsertTablet(
      InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
    // 将插入语句转换为插入节点
    InsertTabletNode insertNode = new InsertTabletNode(
        context.getQueryId().genPlanNodeId(),
        insertTabletStatement.getDevicePath(),
        insertTabletStatement.isAligned(),
        insertTabletStatement.getMeasurements(),
        insertTabletStatement.getDataTypes(),
        insertTabletStatement.getMeasurementSchemas(),
        insertTabletStatement.getTimes(),
        insertTabletStatement.getBitMaps(),
        insertTabletStatement.getColumns(),
        insertTabletStatement.getRowCount());
    insertNode.setFailedMeasurementNumber(insertTabletStatement.getFailedMeasurementNumber());
    return insertNode;
  }

  /**
   * 处理INSERT ROW语句，用于插入单行数据
   * @param insertRowStatement 插入单行数据语句对象
   * @param context 查询上下文
   * @return InsertRowNode节点，用于插入单行数据
   */
  @Override
  public PlanNode visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
    // 将插入语句转换为插入节点
    InsertRowNode insertNode = new InsertRowNode(
        context.getQueryId().genPlanNodeId(),
        insertRowStatement.getDevicePath(),
        insertRowStatement.isAligned(),
        insertRowStatement.getMeasurements(),
        insertRowStatement.getDataTypes(),
        insertRowStatement.getMeasurementSchemas(),
        insertRowStatement.getTime(),
        insertRowStatement.getValues(),
        insertRowStatement.isNeedInferType());
    insertNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
    return insertNode;
  }

  /**
   * 处理PIPE相关的增强语句
   * @param pipeEnrichedStatement PIPE增强语句对象
   * @param context 查询上下文
   * @return 相应的PIPE增强计划节点
   */
  @Override
  public PlanNode visitPipeEnrichedStatement(
      PipeEnrichedStatement pipeEnrichedStatement, MPPQueryContext context) {
    WritePlanNode node = (WritePlanNode) pipeEnrichedStatement.getInnerStatement().accept(this, context);

    // 根据不同的节点类型返回相应的Pipe增强节点
    if (node instanceof LoadTsFileNode) {
      return node;
    } else if (node instanceof InsertNode) {
      return new PipeEnrichedInsertNode((InsertNode) node);
    } else if (node instanceof DeleteDataNode) {
      return new PipeEnrichedDeleteDataNode((DeleteDataNode) node);
    }

    return new PipeEnrichedWritePlanNode(node);
  }

  /**
   * 处理LOAD FILE语句，用于加载文件
   * @param loadTsFileStatement 加载文件语句对象
   * @param context 查询上下文
   * @return LoadTsFileNode节点，用于处理文件加载操作
   */
  @Override
  public PlanNode visitLoadFile(LoadTsFileStatement loadTsFileStatement, MPPQueryContext context) {
    return new LoadTsFileNode(
        context.getQueryId().genPlanNodeId(), loadTsFileStatement.getResources());
  }

  /**
   * 处理SHOW TIMESERIES语句，用于查询时间序列信息
   * @param showTimeSeriesStatement 显示时间序列语句对象
   * @param context 查询上下文
   * @return 时间序列查询的计划节点
   */
  @Override
  public PlanNode visitShowTimeSeries(
      ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    long limit = showTimeSeriesStatement.getLimit();
    long offset = showTimeSeriesStatement.getOffset();
    // 处理带时间条件的情况
    if (showTimeSeriesStatement.hasTimeCondition()) {
      planBuilder = planBuilder
          .planTimeseriesRegionScan(analysis.getDeviceToTimeseriesSchemas(), false)
          .planLimit(limit)
          .planOffset(offset);
      return planBuilder.getRoot();
    }

    // 判断是否可以将offset和limit下推到源操作符
    boolean canPushDownOffsetLimit = analysis.getSchemaPartitionInfo() != null
        && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1
        && !showTimeSeriesStatement.isOrderByHeat();

    // 处理按热度排序的情况
    if (showTimeSeriesStatement.isOrderByHeat()) {
      limit = 0;
      offset = 0;
    } 
    // 不能下推时调整limit和offset
    else if (!canPushDownOffsetLimit) {
      limit = showTimeSeriesStatement.getLimit() + showTimeSeriesStatement.getOffset();
      offset = 0;
    }
    // 构建时间序列模式查询计划
    planBuilder = planBuilder
        .planTimeSeriesSchemaSource(
            showTimeSeriesStatement.getPathPattern(),
            showTimeSeriesStatement.getSchemaFilter(),
            limit,
            offset,
            showTimeSeriesStatement.isOrderByHeat(),
            showTimeSeriesStatement.isPrefixPath(),
            analysis.getRelatedTemplateInfo(),
            showTimeSeriesStatement.getAuthorityScope())
        .planSchemaQueryMerge(showTimeSeriesStatement.isOrderByHeat());

    // 处理显示最新时间序列（按热度排序）
    if (showTimeSeriesStatement.isOrderByHeat()
        && null != analysis.getDataPartitionInfo()
        && !analysis.getDataPartitionInfo().getDataPartitionMap().isEmpty()) {
      PlanNode lastPlanNode = new LogicalPlanBuilder(analysis, context).planLast(analysis, null).getRoot();
      planBuilder = planBuilder.planSchemaQueryOrderByHeat(lastPlanNode);
    }

    // 根据是否可以下推offset和limit决定是否在顶层应用这些操作
    if (canPushDownOffsetLimit) {
      return planBuilder.getRoot();
    }

    return planBuilder.planOffset(showTimeSeriesStatement.getOffset())
        .planLimit(showTimeSeriesStatement.getLimit())
        .getRoot();
  }

  /**
   * 处理SHOW DEVICES语句
   * @param showDevicesStatement 显示设备语句
   * @param context 查询上下文
   * @return 设备查询的计划节点
   */
  /**
   * 处理SHOW DEVICES语句，用于查询设备信息
   * @param showDevicesStatement 显示设备语句对象
   * @param context 查询上下文
   * @return 设备查询的计划节点
   */
  @Override
  public PlanNode visitShowDevices(
      ShowDevicesStatement showDevicesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // 处理带时间条件的情况
    if (showDevicesStatement.hasTimeCondition()) {
      planBuilder = planBuilder
          .planDeviceRegionScan(analysis.getDevicePathToContextMap(), false)
          .planLimit(showDevicesStatement.getLimit())
          .planOffset(showDevicesStatement.getOffset());
      return planBuilder.getRoot();
    }

    // 判断是否可以将offset和limit下推到源操作符
    boolean canPushDownOffsetLimit = analysis.getSchemaPartitionInfo() != null
        && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1;

    long limit = showDevicesStatement.getLimit();
    long offset = showDevicesStatement.getOffset();
    // 不能下推时调整limit和offset
    if (!canPushDownOffsetLimit) {
      limit = showDevicesStatement.getLimit() + showDevicesStatement.getOffset();
      offset = 0;
    }

    // 构建设备模式查询计划
    planBuilder = planBuilder
        .planDeviceSchemaSource(
            showDevicesStatement.getPathPattern(),
            limit,
            offset,
            showDevicesStatement.isPrefixPath(),
            showDevicesStatement.hasSgCol(),
            showDevicesStatement.getSchemaFilter(),
            showDevicesStatement.getAuthorityScope())
        .planSchemaQueryMerge(false);

    // 根据是否可以下推offset和limit决定是否在顶层应用这些操作
    if (!canPushDownOffsetLimit) {
      return planBuilder.planOffset(showDevicesStatement.getOffset())
          .planLimit(showDevicesStatement.getLimit())
          .getRoot();
    }
    return planBuilder.getRoot();
  }

  /**
   * 处理COUNT DEVICES语句
   * @param countDevicesStatement 统计设备数量语句
   * @param context 查询上下文
   * @return 设备数量统计的计划节点
   */
  /**
   * 处理COUNT DEVICES语句，用于统计设备数量
   * @param countDevicesStatement 统计设备数量语句对象
   * @param context 查询上下文
   * @return 设备数量统计的计划节点
   */
  @Override
  public PlanNode visitCountDevices(
      CountDevicesStatement countDevicesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // 处理带时间条件的情况
    if (countDevicesStatement.hasTimeCondition()) {
      planBuilder = planBuilder.planDeviceRegionScan(analysis.getDevicePathToContextMap(), true);
      return planBuilder.getRoot();
    }

    // 构建设备数量统计计划
    return planBuilder
        .planDevicesCountSource(
            countDevicesStatement.getPathPattern(),
            countDevicesStatement.isPrefixPath(),
            countDevicesStatement.getAuthorityScope())
        .planCountMerge()
        .getRoot();
  }

  /**
   * 处理COUNT TIMESERIES语句
   * @param countTimeSeriesStatement 统计时间序列数量语句
   * @param context 查询上下文
   * @return 时间序列数量统计的计划节点
   */
  /**
   * 处理COUNT TIMESERIES语句，用于统计时间序列数量
   * @param countTimeSeriesStatement 统计时间序列数量语句对象
   * @param context 查询上下文
   * @return 时间序列数量统计的计划节点
   */
  @Override
  public PlanNode visitCountTimeSeries(
      CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // 处理带时间条件的情况
    if (countTimeSeriesStatement.hasTimeCondition()) {
      planBuilder = planBuilder.planTimeseriesRegionScan(analysis.getDeviceToTimeseriesSchemas(), true);
      return planBuilder.getRoot();
    }

    // 构建时间序列数量统计计划
    return planBuilder
        .planTimeSeriesCountSource(
            countTimeSeriesStatement.getPathPattern(),
            countTimeSeriesStatement.isPrefixPath(),
            countTimeSeriesStatement.getSchemaFilter(),
            analysis.getRelatedTemplateInfo(),
            countTimeSeriesStatement.getAuthorityScope())
        .planCountMerge()
        .getRoot();
  }

  /**
   * 处理COUNT LEVEL TIMESERIES语句
   * @param countLevelTimeSeriesStatement 统计特定层级时间序列数量语句
   * @param context 查询上下文
   * @return 层级时间序列数量统计的计划节点
   */
  /**
   * 处理COUNT LEVEL TIMESERIES语句，用于统计特定层级的时间序列数量
   * @param countLevelTimeSeriesStatement 统计特定层级时间序列数量语句对象
   * @param context 查询上下文
   * @return 层级时间序列数量统计的计划节点
   */
  @Override
  public PlanNode visitCountLevelTimeSeries(
      CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    // 构建层级时间序列数量统计计划
    return planBuilder
        .planLevelTimeSeriesCountSource(
            countLevelTimeSeriesStatement.getPathPattern(),
            countLevelTimeSeriesStatement.isPrefixPath(),
            countLevelTimeSeriesStatement.getLevel(),
            countLevelTimeSeriesStatement.getSchemaFilter(),
            analysis.getRelatedTemplateInfo(),
            countLevelTimeSeriesStatement.getAuthorityScope())
        .planCountMerge()
        .getRoot();
  }

  /**
   * 处理COUNT NODES语句
   * @param countStatement 统计节点数量语句
   * @param context 查询上下文
   * @return 节点数量统计的计划节点
   */
  /**
   * 处理COUNT NODES语句，用于统计节点数量
   * @param countStatement 统计节点数量语句对象
   * @param context 查询上下文
   * @return 节点数量统计的计划节点
   */
  @Override
  public PlanNode visitCountNodes(CountNodesStatement countStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    // 构建节点路径统计计划
    return planBuilder
        .planNodePathsSchemaSource(
            countStatement.getPathPattern(),
            countStatement.getLevel(),
            countStatement.getAuthorityScope())
        .planSchemaQueryMerge(false)
        .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
        .planNodePathsCount()
        .getRoot();
  }

  /**
   * 处理INSERT ROWS语句
   * @param insertRowsStatement 插入多行数据语句
   * @param context 查询上下文
   * @return InsertRowsNode节点
   */
  /**
   * 处理INSERT ROWS语句，用于插入多行数据
   * @param insertRowsStatement 插入多行数据语句对象
   * @param context 查询上下文
   * @return InsertRowsNode节点，包含多个InsertRowNode
   */
  @Override
  public PlanNode visitInsertRows(
      InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
    // 创建InsertRowsNode节点，用于处理多行数据插入
    InsertRowsNode insertRowsNode = new InsertRowsNode(context.getQueryId().genPlanNodeId());
    // 为每行创建InsertRowNode
    for (int i = 0; i < insertRowsStatement.getInsertRowStatementList().size(); i++) {
      InsertRowStatement insertRowStatement = insertRowsStatement.getInsertRowStatementList().get(i);
      InsertRowNode insertRowNode = new InsertRowNode(
          insertRowsNode.getPlanNodeId(),
          insertRowStatement.getDevicePath(),
          insertRowStatement.isAligned(),
          insertRowStatement.getMeasurements(),
          insertRowStatement.getDataTypes(),
          insertRowStatement.getMeasurementSchemas(),
          insertRowStatement.getTime(),
          insertRowStatement.getValues(),
          insertRowStatement.isNeedInferType());
      insertRowNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
      insertRowsNode.addOneInsertRowNode(insertRowNode, i);
    }
    return insertRowsNode;
  }


  /**
   * 处理INSERT MULTI TABLETS语句，用于插入多个表的数据
   * @param insertMultiTabletsStatement 插入多表数据语句对象
   * @param context 查询上下文
   * @return InsertMultiTabletsNode节点，包含多个InsertTabletNode
   */
  @Override
  public PlanNode visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
    // 创建InsertMultiTabletsNode节点，用于处理多个表的批量数据插入
    InsertMultiTabletsNode insertMultiTabletsNode = new InsertMultiTabletsNode(context.getQueryId().genPlanNodeId());
    // 为每个表创建InsertTabletNode
    for (int i = 0; i < insertMultiTabletsStatement.getInsertTabletStatementList().size(); i++) {
      InsertTabletStatement insertTabletStatement = insertMultiTabletsStatement.getInsertTabletStatementList().get(i);
      InsertTabletNode insertTabletNode = new InsertTabletNode(
          insertMultiTabletsNode.getPlanNodeId(),
          insertTabletStatement.getDevicePath(),
          insertTabletStatement.isAligned(),
          insertTabletStatement.getMeasurements(),
          insertTabletStatement.getDataTypes(),
          insertTabletStatement.getMeasurementSchemas(),
          insertTabletStatement.getTimes(),
          insertTabletStatement.getBitMaps(),
          insertTabletStatement.getColumns(),
          insertTabletStatement.getRowCount());
      insertTabletNode.setFailedMeasurementNumber(insertTabletStatement.getFailedMeasurementNumber());
      insertMultiTabletsNode.addInsertTabletNode(insertTabletNode, i);
    }
    return insertMultiTabletsNode;
  }

  /**
   * 处理INSERT ROWS OF ONE DEVICE语句
   * @param insertRowsOfOneDeviceStatement 插入同一设备多行数据语句
   * @param context 查询上下文
   * @return InsertRowsOfOneDeviceNode节点
   */
  /**
   * 处理INSERT ROWS OF ONE DEVICE语句，用于向同一设备插入多行数据
   * @param insertRowsOfOneDeviceStatement 插入同一设备多行数据语句对象
   * @param context 查询上下文
   * @return InsertRowsOfOneDeviceNode节点，用于高效处理单设备多行数据插入
   */
  @Override
  public PlanNode visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
    // 创建InsertRowsOfOneDeviceNode节点，优化单设备多行数据插入性能
    InsertRowsOfOneDeviceNode insertRowsOfOneDeviceNode = new InsertRowsOfOneDeviceNode(context.getQueryId().genPlanNodeId());

    List<InsertRowNode> insertRowNodeList = new ArrayList<>();
    List<Integer> insertRowNodeIndexList = new ArrayList<>();
    // 为每行创建InsertRowNode
    for (int i = 0; i < insertRowsOfOneDeviceStatement.getInsertRowStatementList().size(); i++) {
      InsertRowStatement insertRowStatement = insertRowsOfOneDeviceStatement.getInsertRowStatementList().get(i);
      InsertRowNode insertRowNode = new InsertRowNode(
          insertRowsOfOneDeviceNode.getPlanNodeId(),
          insertRowStatement.getDevicePath(),
          insertRowStatement.isAligned(),
          insertRowStatement.getMeasurements(),
          insertRowStatement.getDataTypes(),
          insertRowStatement.getMeasurementSchemas(),
          insertRowStatement.getTime(),
          insertRowStatement.getValues(),
          insertRowStatement.isNeedInferType());
      insertRowNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
      insertRowNodeList.add(insertRowNode);
      insertRowNodeIndexList.add(i);
    }

    insertRowsOfOneDeviceNode.setInsertRowNodeList(insertRowNodeList);
    insertRowsOfOneDeviceNode.setInsertRowNodeIndexList(insertRowNodeIndexList);
    return insertRowsOfOneDeviceNode;
  }

  /**
   * 处理SERIES SCHEMA FETCH语句（内部使用）
   * @param seriesSchemaFetchStatement 系列模式获取语句
   * @param context 查询上下文
   * @return 模式获取的计划节点
   */
  /**
   * 处理SERIES SCHEMA FETCH语句（内部使用），用于获取时间序列模式信息
   * @param seriesSchemaFetchStatement 系列模式获取语句对象
   * @param context 查询上下文
   * @return 模式获取的计划节点
   */
  @Override
  public PlanNode visitSeriesSchemaFetch(
      SeriesSchemaFetchStatement seriesSchemaFetchStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    List<String> storageGroupList = new ArrayList<>(analysis.getSchemaPartitionInfo().getSchemaPartitionMap().keySet());
    return planBuilder
        .planSchemaFetchMerge(storageGroupList)
        .planSeriesSchemaFetchSource(
            storageGroupList,
            seriesSchemaFetchStatement.getPatternTree(),
            seriesSchemaFetchStatement.getTemplateMap(),
            seriesSchemaFetchStatement.isWithTags(),
            seriesSchemaFetchStatement.isWithAttributes(),
            seriesSchemaFetchStatement.isWithTemplate(),
            seriesSchemaFetchStatement.isWithAliasForce())
        .getRoot();
  }

  /**
   * 处理DEVICE SCHEMA FETCH语句（内部使用）
   * @param deviceSchemaFetchStatement 设备模式获取语句
   * @param context 查询上下文
   * @return 模式获取的计划节点
   */
  /**
   * 处理DEVICE SCHEMA FETCH语句（内部使用），用于获取设备模式信息
   * @param deviceSchemaFetchStatement 设备模式获取语句对象
   * @param context 查询上下文
   * @return 模式获取的计划节点
   */
  @Override
  public PlanNode visitDeviceSchemaFetch(
      DeviceSchemaFetchStatement deviceSchemaFetchStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    List<String> storageGroupList = new ArrayList<>(analysis.getSchemaPartitionInfo().getSchemaPartitionMap().keySet());
    return planBuilder
        .planSchemaFetchMerge(storageGroupList)
        .planDeviceSchemaFetchSource(
            storageGroupList,
            deviceSchemaFetchStatement.getPatternTree(),
            deviceSchemaFetchStatement.getAuthorityScope())
        .getRoot();
  }

  /**
   * 处理SHOW CHILD PATHS语句
   * @param showChildPathsStatement 显示子路径语句
   * @param context 查询上下文
   * @return 子路径查询的计划节点
   */
  /**
   * 处理SHOW CHILD PATHS语句，用于查询子路径信息
   * @param showChildPathsStatement 显示子路径语句对象
   * @param context 查询上下文
   * @return 子路径查询的计划节点
   */
  @Override
  public PlanNode visitShowChildPaths(
      ShowChildPathsStatement showChildPathsStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(
            showChildPathsStatement.getPartialPath(),
            -1,
            showChildPathsStatement.getAuthorityScope())
        .planSchemaQueryMerge(false)
        .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
        .getRoot();
  }

  /**
   * 处理SHOW CHILD NODES语句
   * @param showChildNodesStatement 显示子节点语句
   * @param context 查询上下文
   * @return 子节点查询的计划节点
   */
  /**
   * 处理SHOW CHILD NODES语句，用于查询子节点信息
   * @param showChildNodesStatement 显示子节点语句对象
   * @param context 查询上下文
   * @return 子节点查询的计划节点
   */
  @Override
  public PlanNode visitShowChildNodes(
      ShowChildNodesStatement showChildNodesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    return planBuilder
        .planNodePathsSchemaSource(
            showChildNodesStatement.getPartialPath(),
            -1,
            showChildNodesStatement.getAuthorityScope())
        .planSchemaQueryMerge(false)
        .planNodeManagementMemoryMerge(analysis.getMatchedNodes())
        .planNodePathsConvert()
        .getRoot();
  }

  /**
   * 处理DELETE DATA语句
   * @param deleteDataStatement 删除数据语句
   * @param context 查询上下文
   * @return DeleteDataNode节点
   */
  /**
   * 处理DELETE DATA语句，用于删除时间范围内的数据
   * @param deleteDataStatement 删除数据语句对象
   * @param context 查询上下文
   * @return DeleteDataNode节点，用于执行数据删除操作
   */
  @Override
  public PlanNode visitDeleteData(
      DeleteDataStatement deleteDataStatement, MPPQueryContext context) {
    return new DeleteDataNode(
        context.getQueryId().genPlanNodeId(),
        deleteDataStatement.getPathList(),
        deleteDataStatement.getDeleteStartTime(),
        deleteDataStatement.getDeleteEndTime());
  }

  /**
   * 处理ACTIVATE TEMPLATE语句
   * @param activateTemplateStatement 激活模板语句
   * @param context 查询上下文
   * @return ActivateTemplateNode节点
   */
  /**
   * 处理ACTIVATE TEMPLATE语句，用于激活模板
   * @param activateTemplateStatement 激活模板语句对象
   * @param context 查询上下文
   * @return ActivateTemplateNode节点，用于模板激活操作
   */
  @Override
  public PlanNode visitActivateTemplate(
      ActivateTemplateStatement activateTemplateStatement, MPPQueryContext context) {
    return new ActivateTemplateNode(
        context.getQueryId().genPlanNodeId(),
        activateTemplateStatement.getPath(),
        analysis.getTemplateSetInfo().right.get(0).getNodeLength() - 1,
        analysis.getTemplateSetInfo().left.getId());
  }


  /**
   * 处理BATCH ACTIVATE TEMPLATE语句，用于批量激活模板
   * @param batchActivateTemplateStatement 批量激活模板语句对象
   * @param context 查询上下文
   * @return BatchActivateTemplateNode节点，用于批量模板激活操作
   */
  @Override
  public PlanNode visitBatchActivateTemplate(
      BatchActivateTemplateStatement batchActivateTemplateStatement, MPPQueryContext context) {
    Map<PartialPath, Pair<Integer, Integer>> templateActivationMap = new HashMap<>();
    // 构建模板激活映射，将设备路径映射到对应的模板ID和级别
    for (Map.Entry<PartialPath, Pair<Template, PartialPath>> entry : analysis.getDeviceTemplateSetInfoMap().entrySet()) {
      templateActivationMap.put(
          entry.getKey(),
          new Pair<>(entry.getValue().left.getId(), entry.getValue().right.getNodeLength() - 1));
    }
    return new BatchActivateTemplateNode(
        context.getQueryId().genPlanNodeId(), templateActivationMap);
  }

  /**
   * 处理内部使用的BATCH ACTIVATE TEMPLATE语句
   * @param internalBatchActivateTemplateStatement 内部批量激活模板语句
   * @param context 查询上下文
   * @return InternalBatchActivateTemplateNode节点
   */
  /**
   * 处理内部使用的BATCH ACTIVATE TEMPLATE语句，用于内部批量激活模板
   * @param internalBatchActivateTemplateStatement 内部批量激活模板语句对象
   * @param context 查询上下文
   * @return InternalBatchActivateTemplateNode节点，用于内部批量模板激活操作
   */
  @Override
  public PlanNode visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement internalBatchActivateTemplateStatement,
      MPPQueryContext context) {
    Map<PartialPath, Pair<Integer, Integer>> templateActivationMap = new HashMap<>();
    // 构建模板激活映射
    for (Map.Entry<PartialPath, Pair<Template, PartialPath>> entry : internalBatchActivateTemplateStatement.getDeviceMap().entrySet()) {
      templateActivationMap.put(
          entry.getKey(),
          new Pair<>(entry.getValue().left.getId(), entry.getValue().right.getNodeLength() - 1));
    }
    return new InternalBatchActivateTemplateNode(
        context.getQueryId().genPlanNodeId(), templateActivationMap);
  }

  /**
   * 处理SHOW PATHS USING TEMPLATE语句
   * @param showPathsUsingTemplateStatement 显示使用模板的路径语句
   * @param context 查询上下文
   * @return 模板路径查询的计划节点
   */
  /**
   * 处理SHOW PATHS USING TEMPLATE语句，用于查询使用指定模板的路径
   * @param showPathsUsingTemplateStatement 显示使用模板的路径语句对象
   * @param context 查询上下文
   * @return 模板路径查询的计划节点
   */
  @Override
  public PlanNode visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    planBuilder = planBuilder
        .planPathsUsingTemplateSource(
            analysis.getSpecifiedTemplateRelatedPathPatternList(),
            analysis.getTemplateSetInfo().left.getId(),
            showPathsUsingTemplateStatement.getAuthorityScope())
        .planSchemaQueryMerge(false);
    return planBuilder.getRoot();
  }

  /**
   * 处理SHOW QUERIES语句
   * @param showQueriesStatement 显示查询语句
   * @param context 查询上下文
   * @return 查询信息查询的计划节点
   */
  /**
   * 处理SHOW QUERIES语句，用于查询当前运行的查询信息
   * @param showQueriesStatement 显示查询语句对象
   * @param context 查询上下文
   * @return 查询信息查询的计划节点
   */
  @Override
  public PlanNode visitShowQueries(
      ShowQueriesStatement showQueriesStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);
    planBuilder = planBuilder
        .planShowQueries(analysis) // 推送过滤条件
        .planOffset(showQueriesStatement.getRowOffset())
        .planLimit(showQueriesStatement.getRowLimit());
    return planBuilder.getRoot();
  }

  /**
   * 处理CREATE LOGICAL VIEW语句
   * @param createLogicalViewStatement 创建逻辑视图语句
   * @param context 查询上下文
   * @return CreateLogicalViewNode节点
   */
  /**
   * 处理CREATE LOGICAL VIEW语句，用于创建逻辑视图
   * @param createLogicalViewStatement 创建逻辑视图语句对象
   * @param context 查询上下文
   * @return CreateLogicalViewNode节点，用于创建逻辑视图
   */
  @Override
  public PlanNode visitCreateLogicalView(
      CreateLogicalViewStatement createLogicalViewStatement, MPPQueryContext context) {
    List<ViewExpression> viewExpressionList = new ArrayList<>();
    if (createLogicalViewStatement.getViewExpressions() == null) {
      // 将所有Expression转换为ViewExpressions
      TransformToViewExpressionVisitor transformToViewExpressionVisitor = new TransformToViewExpressionVisitor();
      List<Expression> expressionList = createLogicalViewStatement.getSourceExpressionList();
      for (Expression expression : expressionList) {
        viewExpressionList.add(transformToViewExpressionVisitor.process(expression, null));
      }
    } else {
      viewExpressionList = createLogicalViewStatement.getViewExpressions();
    }

    return new CreateLogicalViewNode(
        context.getQueryId().genPlanNodeId(),
        createLogicalViewStatement.getTargetPathList(),
        viewExpressionList);
  }

  /**
   * 处理SHOW LOGICAL VIEW语句
   * @param showLogicalViewStatement 显示逻辑视图语句
   * @param context 查询上下文
   * @return 逻辑视图查询的计划节点
   */
  /**
   * 处理SHOW LOGICAL VIEW语句，用于查询逻辑视图信息
   * @param showLogicalViewStatement 显示逻辑视图语句对象
   * @param context 查询上下文
   * @return 逻辑视图查询的计划节点
   */
  @Override
  public PlanNode visitShowLogicalView(
      ShowLogicalViewStatement showLogicalViewStatement, MPPQueryContext context) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(analysis, context);

    // 判断是否可以将offset和limit下推到源操作符
    boolean canPushDownOffsetLimit = analysis.getSchemaPartitionInfo() != null
        && analysis.getSchemaPartitionInfo().getDistributionInfo().size() == 1;

    long limit = showLogicalViewStatement.getLimit();
    long offset = showLogicalViewStatement.getOffset();
    // 不能下推时调整limit和offset
    if (!canPushDownOffsetLimit) {
      limit = showLogicalViewStatement.getLimit() + showLogicalViewStatement.getOffset();
      offset = 0;
    }
    // 构建逻辑视图模式查询计划
    planBuilder = planBuilder
        .planLogicalViewSchemaSource(
            showLogicalViewStatement.getPathPattern(),
            showLogicalViewStatement.getSchemaFilter(),
            limit,
            offset,
            showLogicalViewStatement.getAuthorityScope())
        .planSchemaQueryMerge(false);

    // 根据是否可以下推offset和limit决定是否在顶层应用这些操作
    if (canPushDownOffsetLimit) {
      return planBuilder.getRoot();
    }

    return planBuilder
        .planOffset(showLogicalViewStatement.getOffset())
        .planLimit(showLogicalViewStatement.getLimit())
        .getRoot();
  }
}