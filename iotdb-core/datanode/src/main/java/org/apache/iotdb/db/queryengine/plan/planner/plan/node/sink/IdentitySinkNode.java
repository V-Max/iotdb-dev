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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink;

import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 身份接收节点，用于在查询计划中标识数据流的最终输出位置
 * 该类继承自MultiChildrenSinkNode，支持多个子节点的数据接收
 * 
 * 示例查询：SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10
 * 在这个查询中，IdentitySinkNode作为查询计划的最终输出节点，负责接收处理后的数据
 * 
 * 关键功能：
 * 1. 标识查询计划的最终输出位置
 * 2. 管理下游通道位置信息
 * 3. 序列化和反序列化节点信息
 * 4. 聚合子节点的输出列名
 * 
 * 表1：IdentitySinkNode在查询计划中的角色
 * | 查询阶段 | 节点类型 | 功能描述 | 示例 |
 * |---------|---------|---------|------|
 * | 数据扫描 | SeriesScanNode | 扫描原始数据 | 扫描root.db0.t1 |
 * | 数据处理 | FilterAndProjectOperator | 过滤和投影 | t1 + 2 <= 10过滤 |
 * | 数据输出 | IdentitySinkNode | 最终输出 | 输出ref0列数据 |
 * 
 * 表2：IdentitySinkNode与其他SinkNode的区别
 * | 特性 | IdentitySinkNode | ExchangeSinkNode | FragmentSinkNode |
 * |------|-----------------|------------------|------------------|
 * | 功能 | 身份标识 | 数据交换 | 片段输出 |
 * | 子节点数 | 多个 | 单个 | 单个 |
 * | 使用场景 | 最终输出 | 分布式查询 | 查询片段 |
 * | 序列化 | 支持 | 支持 | 支持 |
 * 
 * 示例执行流程（SELECT t1 AS ref0 FROM root.db0 WHERE t1 + 2 <= 10）：
 * 1. 查询计划构建：构建包含SeriesScanNode、FilterNode、ProjectNode的计划树
 * 2. 计划优化：优化器将计划转换为执行计划
 * 3. 操作符生成：OperatorTreeGenerator将计划节点转换为操作符
 * 4. 数据流执行：数据从扫描操作符流向过滤操作符，最终到达IdentitySinkNode
 * 5. 结果输出：IdentitySinkNode将处理后的数据输出给客户端
 */
public class IdentitySinkNode extends MultiChildrenSinkNode {

  /**
   * 构造函数：创建身份接收节点
   * 
   * @param id 计划节点ID，用于唯一标识该节点
   * 示例：对于查询SELECT t1 AS ref0 FROM root.db0，节点ID可能是"Sink_1"
   */
  public IdentitySinkNode(PlanNodeId id) {
    super(id);
  }

  /**
   * 获取节点类型
   * 
   * @return 返回IDENTITY_SINK类型标识
   * 示例：返回PlanNodeType.IDENTITY_SINK，表示这是身份接收节点
   */
  @Override
  public PlanNodeType getType() {
    return PlanNodeType.IDENTITY_SINK;
  }

  /**
   * 构造函数：创建包含下游通道位置的身份接收节点
   * 
   * @param id 计划节点ID
   * @param downStreamChannelLocationList 下游通道位置列表
   * 示例：在分布式查询中，指定数据发送的目标节点位置
   */
  public IdentitySinkNode(
      PlanNodeId id, List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id, downStreamChannelLocationList);
  }

  /**
   * 构造函数：创建包含子节点和下游通道位置的身份接收节点
   * 
   * @param id 计划节点ID
   * @param children 子节点列表
   * @param downStreamChannelLocationList 下游通道位置列表
   * 示例：在复杂查询中，包含多个数据源子节点
   */
  public IdentitySinkNode(
      PlanNodeId id,
      List<PlanNode> children,
      List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id, children, downStreamChannelLocationList);
  }

  /**
   * 克隆节点：创建当前节点的副本
   * 
   * @return 新的IdentitySinkNode实例
   * 示例：在查询优化过程中，可能需要复制节点进行不同的优化尝试
   */
  @Override
  public PlanNode clone() {
    return new IdentitySinkNode(getPlanNodeId(), getDownStreamChannelLocationList());
  }

  /**
   * 获取输出列名：聚合所有子节点的输出列名
   * 
   * @return 输出列名列表
   * 示例：对于查询SELECT t1 AS ref0，返回["ref0"]
   * 数据结构变化：将多个子节点的列名列表合并为单个列表
   */
  @Override
  public List<String> getOutputColumnNames() {
    return children.stream()
        .map(PlanNode::getOutputColumnNames)  // 获取每个子节点的输出列名
        .flatMap(List::stream)               // 将多个列表扁平化为单个流
        .collect(Collectors.toList());       // 收集为列表
  }

  /**
   * 接受访问者：实现访问者模式，允许外部访问节点
   * 
   * @param visitor 计划访问者
   * @param context 访问上下文
   * @param <R> 返回类型
   * @param <C> 上下文类型
   * @return 访问结果
   * 示例：OperatorTreeGenerator访问IdentitySinkNode生成对应的操作符
   */
  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitIdentitySink(this, context);
  }

  /**
   * 序列化属性：将节点属性序列化到字节缓冲区
   * 
   * @param byteBuffer 目标字节缓冲区
   * 示例：在网络传输或持久化时，将节点信息序列化为字节流
   * 数据结构变化：对象 → 字节序列
   */
  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.IDENTITY_SINK.serialize(byteBuffer);  // 序列化节点类型
    ReadWriteIOUtils.write(downStreamChannelLocationList.size(), byteBuffer); // 写入下游通道数量
    for (DownStreamChannelLocation downStreamChannelLocation : downStreamChannelLocationList) {
      downStreamChannelLocation.serialize(byteBuffer); // 序列化每个下游通道位置
    }
  }

  /**
   * 序列化属性：将节点属性序列化到输出流
   * 
   * @param stream 目标输出流
   * @throws IOException 序列化异常
   * 示例：将节点信息写入文件或网络流
   */
  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.IDENTITY_SINK.serialize(stream);  // 序列化节点类型
    ReadWriteIOUtils.write(downStreamChannelLocationList.size(), stream); // 写入下游通道数量
    for (DownStreamChannelLocation downStreamChannelLocation : downStreamChannelLocationList) {
      downStreamChannelLocation.serialize(stream); // 序列化每个下游通道位置
    }
  }

  /**
   * 转换为字符串：生成节点的字符串表示
   * 
   * @return 节点字符串表示
   * 示例：返回"IdentitySinkNode-Sink_1"
   */
  @Override
  public String toString() {
    return String.format("IdentitySinkNode-%s", this.getPlanNodeId());
  }

  /**
   * 反序列化：从字节缓冲区创建IdentitySinkNode实例
   * 
   * @param byteBuffer 源字节缓冲区
   * @return 反序列化后的IdentitySinkNode实例
   * 示例：从网络接收或文件读取的字节流重建节点
   * 数据结构变化：字节序列 → 对象
   */
  public static IdentitySinkNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer); // 读取下游通道数量
    List<DownStreamChannelLocation> downStreamChannelLocationList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      downStreamChannelLocationList.add(DownStreamChannelLocation.deserialize(byteBuffer)); // 反序列化每个下游通道
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer); // 反序列化节点ID
    return new IdentitySinkNode(planNodeId, downStreamChannelLocationList); // 创建新实例
  }
}