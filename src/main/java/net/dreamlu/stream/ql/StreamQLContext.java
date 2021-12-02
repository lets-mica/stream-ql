package net.dreamlu.stream.ql;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * ReactorQL上下文，通过上下文传递参数及数据
 *
 * @author zhouhao
 * @since 1.0.0
 */
public interface StreamQLContext {

    /**
     * 提供一个函数(参数为表名,返回值为数据流).创建上下文.
     *
     * @param supplier 数据源
     * @return this
     */
    static StreamQLContext ofDatasource(Function<String, Stream<?>> supplier) {
        return new DefaultStreamQLContext(supplier);
    }

    /**
     * 根据表名获取数据源
     *
     * @param name 表名
     * @return 数据源
     */
    Stream<Object> getDataSource(String name);

    /**
     * 根据索引获取参数.
     * <pre>
     *     select * from table where name = ?
     * </pre>
     *
     * @param index 索引
     * @return 参数值
     * @see this#bind(int, Object)
     */
    Optional<Object> getParameter(int index);

    /**
     * 根据名称获取参数
     * <pre>
     *     select * from table where name = :name
     * </pre>
     *
     * @param name 参数名
     * @return 参数值
     * @see this#bind(String, Object)
     */
    Optional<Object> getParameter(String name);

    /**
     * @return 全部参数
     */
    Map<String, Object> getParameters();

    /**
     * 绑定参数
     *
     * @param index 索引
     * @param value 值
     * @return this
     */
    StreamQLContext bind(int index, Object value);

    /**
     * 绑定参数
     *
     * @param name  参数名
     * @param value 值
     * @return this
     */
    StreamQLContext bind(String name, Object value);

    /**
     * 绑定参数,自动增加索引
     *
     * @param value 值
     * @return this
     */
    StreamQLContext bind(Object value);

    /**
     * 绑定多个参数,使用key作为参数名
     *
     * @param value map
     * @return this
     */
    default StreamQLContext bindAll(Map<String, Object> value) {
        value.forEach(this::bind);
        return this;
    }

    /**
     * 指定数据源转换器,并转换为新等上下文,数据源转换器用于在创建数据源时,进行自定义的操作
     *
     * @param dataSourceMapper 数据源转换器
     * @return 新上下文
     */
    StreamQLContext transfer(BiFunction<String, Stream<Object>, Stream<Object>> dataSourceMapper);

}
