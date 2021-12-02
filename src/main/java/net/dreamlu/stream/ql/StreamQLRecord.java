package net.dreamlu.stream.ql;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 查询记录,即执行SQL后的一行数据。
 * <p>
 * 名词:
 * <ul>
 *     <li>record: 值当前行数据,可以理解为表中的数据</li>
 *     <li>result: 转换后的新数据,即将输出的数据</li>
 * </ul>
 *
 * @author zhouhao
 * @since 1.0.0
 */
public interface StreamQLRecord {

    /**
     * @return 上下文
     */
    StreamQLContext getContext();

    /**
     * @return 记录名, 通常为表名或者别名
     */
    String getName();

    /**
     * 根据名称获取数据源
     *
     * @param name 名称,通常为表名
     * @return 数据源
     */
    Stream<Object> getDataSource(String name);

    /**
     * 获取指定名称的记录,在同时操作多个表时,可以通过此方法获取指定名称的表记录
     *
     * @param name 名称,通常为表名
     * @return 记录值
     */
    Optional<Object> getRecord(String name);

    /**
     * 获取当前记录
     *
     * @return 当前记录
     */
    Object getRecord();

    /**
     * 将记录设置到结果中
     *
     * @return this
     */
    StreamQLRecord putRecordToResult();

    /**
     * 设置结果
     *
     * @param name  名称
     * @param value 值
     * @return this
     */
    StreamQLRecord setResult(String name, Object value);

    /**
     * 设置多个结果
     *
     * @param values 结果
     * @return this
     */
    StreamQLRecord setResults(Map<String, Object> values);

    /**
     * @return 转为Map
     */
    Map<String, Object> asMap();

    /**
     * 指定名称并将结果转换为新的记录
     *
     * @param name 名称
     * @return 新的记录
     */
    StreamQLRecord resultToRecord(String name);

    /**
     * 添加记录
     *
     * @param name   名称
     * @param record 记录值
     * @return this
     */
    StreamQLRecord addRecord(String name, Object record);

    /**
     * 添加多个记录
     *
     * @param records 记录
     * @return this
     */
    StreamQLRecord addRecords(Map<String, Object> records);

    /**
     * 获取记录信息,key 为记录名称,value为值
     *
     * @param all 是否获取全部记录(多个表的情况)
     * @return 记录
     */
    Map<String, Object> getRecords(boolean all);

    /**
     * 移除记录
     *
     * @param name name
     * @return this
     */
    StreamQLRecord removeRecord(String name);

    /**
     * 创建新的记录值
     *
     * @param name    名称
     * @param row     数据
     * @param context 上下文
     * @return 记录
     */
    static StreamQLRecord newRecord(String name, Object row, StreamQLContext context) {
        if (row instanceof DefaultStreamQLRecord) {
            DefaultStreamQLRecord record = ((DefaultStreamQLRecord) row);
            if (null != name) {
                record.setName(name);
                record.addRecord(name, record.getRecord());
            }
            return record;
        }
        return new DefaultStreamQLRecord(name, row, context);
    }

    StreamQLRecord copy();
}
