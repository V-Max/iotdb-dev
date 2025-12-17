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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * This rewriter:
 *
 * <p>1. Concat prefix path in SELECT, WHERE, and WITHOUT NULL clause with the suffix path in the
 * FROM clause.
 *
 * <p>2. Construct a {@link PathPatternTree}.
 */
public class ConcatPathRewriter {

  private PathPatternTree patternTree;

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  /**
   * 重写查询语句，将SELECT、WHERE等子句中的路径与FROM子句中的前缀路径进行拼接
   * 并构建路径模式树（PathPatternTree）
   * 
   * @param statement 原始查询语句
   * @param patternTree 路径模式树，用于存储拼接后的完整路径模式
   * @param queryContext 查询上下文，包含查询执行环境信息
   * @return Statement 重写后的查询语句
   * @throws StatementAnalyzeException 语句分析异常
   */
  public Statement rewrite(
      Statement statement, PathPatternTree patternTree, MPPQueryContext queryContext)
      throws StatementAnalyzeException {
    // 步骤1：类型转换和初始化 - 将通用语句转换为查询语句
    QueryStatement queryStatement = (QueryStatement) statement;
    // 设置当前模式树实例
    this.patternTree = patternTree;
    
    // 步骤2：获取FROM子句中的前缀路径列表
    // 例如：FROM root.db0 中的 root.db0 就是前缀路径
    List<PartialPath> prefixPaths = queryStatement.getFromComponent().getPrefixPaths();

    // 步骤3：根据查询类型（按设备对齐或普通查询）采用不同的处理策略
    if (queryStatement.isAlignByDevice()) {
      // 按设备对齐查询的处理逻辑
      
      // 3.1 处理SELECT子句中的表达式
      for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
        // 为SELECT表达式构建路径模式树
        ExpressionAnalyzer.constructPatternTreeFromExpression(
            resultColumn.getExpression(), prefixPaths, patternTree);
      }
      
      // 3.2 处理GROUP BY表达式（如果存在）
      if (queryStatement.hasGroupByExpression()) {
        ExpressionAnalyzer.constructPatternTreeFromExpression(
            queryStatement.getGroupByComponent().getControlColumnExpression(),
            prefixPaths,
            patternTree);
      }
      
      // 3.3 处理ORDER BY表达式（如果存在）
      if (queryStatement.hasOrderByExpression()) {
        for (Expression sortItemExpression : queryStatement.getExpressionSortItemList()) {
          ExpressionAnalyzer.constructPatternTreeFromExpression(
              sortItemExpression, prefixPaths, patternTree);
        }
      }
    } else {
      // 普通查询的处理逻辑
      
      // 3.4 拼接SELECT子句与FROM子句
      List<ResultColumn> resultColumns =
          concatSelectWithFrom(queryStatement.getSelectComponent(), prefixPaths, queryContext);
      // 更新查询语句的SELECT组件
      queryStatement.getSelectComponent().setResultColumns(resultColumns);

      // 3.5 拼接GROUP BY子句与FROM子句（如果存在）
      if (queryStatement.hasGroupByExpression()) {
        queryStatement
            .getGroupByComponent()
            .setControlColumnExpression(
                contactGroupByWithFrom(
                    queryStatement.getGroupByComponent().getControlColumnExpression(),
                    prefixPaths,
                    queryContext));
      }
      
      // 3.6 拼接ORDER BY子句与FROM子句（如果存在）
      if (queryStatement.hasOrderByExpression()) {
        List<Expression> sortItemExpressions = queryStatement.getExpressionSortItemList();
        sortItemExpressions.replaceAll(
            expression -> contactOrderByWithFrom(expression, prefixPaths, queryContext));
      }
    }

    // 步骤4：拼接WHERE子句与FROM子句（如果存在）
    if (queryStatement.getWhereCondition() != null) {
      ExpressionAnalyzer.constructPatternTreeFromExpression(
          queryStatement.getWhereCondition().getPredicate(), prefixPaths, patternTree);
    }

    // 步骤5：拼接HAVING子句与FROM子句（如果存在）
    if (queryStatement.getHavingCondition() != null) {
      ExpressionAnalyzer.constructPatternTreeFromExpression(
          queryStatement.getHavingCondition().getPredicate(), prefixPaths, patternTree);
    }

    // 步骤6：权限检查和模式树构建
    // 构建完整的路径模式树结构
    patternTree.constructTree();
    
    // 步骤7：与授权范围进行交集运算，确保只包含用户有权限访问的路径
    this.patternTree =
        PathPatternTreeUtils.intersectWithFullPathPrefixTree(
            patternTree, queryStatement.getAuthorityScope());
    
    // 返回重写后的查询语句
    return queryStatement;
  }

  /**
   * Concat the prefix path in the SELECT clause and the suffix path in the FROM clause into a full
   * path pattern. And construct pattern tree.
   */
  private List<ResultColumn> concatSelectWithFrom(
      final SelectComponent selectComponent,
      final List<PartialPath> prefixPaths,
      final MPPQueryContext queryContext)
      throws StatementAnalyzeException {
    // resultColumns after concat
    List<ResultColumn> resultColumns = new ArrayList<>();
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      List<Expression> resultExpressions =
          ExpressionAnalyzer.concatExpressionWithSuffixPaths(
              resultColumn.getExpression(), prefixPaths, patternTree, queryContext);
      for (Expression resultExpression : resultExpressions) {
        resultColumns.add(
            new ResultColumn(
                resultExpression, resultColumn.getAlias(), resultColumn.getColumnType()));
      }
    }
    return resultColumns;
  }

  private Expression contactGroupByWithFrom(
      final Expression expression,
      final List<PartialPath> prefixPaths,
      final MPPQueryContext queryContext) {
    List<Expression> resultExpressions =
        ExpressionAnalyzer.concatExpressionWithSuffixPaths(
            expression, prefixPaths, patternTree, queryContext);
    if (resultExpressions.size() != 1) {
      throw new IllegalStateException("Expression in group by should indicate one value");
    }
    return resultExpressions.get(0);
  }

  private Expression contactOrderByWithFrom(
      final Expression expression,
      final List<PartialPath> prefixPaths,
      final MPPQueryContext queryContext) {
    List<Expression> resultExpressions =
        ExpressionAnalyzer.concatExpressionWithSuffixPaths(
            expression, prefixPaths, patternTree, queryContext);
    if (resultExpressions.size() != 1) {
      throw new IllegalStateException("Expression in order by should indicate one value");
    }
    return resultExpressions.get(0);
  }
}