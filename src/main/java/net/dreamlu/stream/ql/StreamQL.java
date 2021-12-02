package net.dreamlu.stream.ql;

import net.dreamlu.stream.ql.feature.Feature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public interface StreamQL {

    /**
     * 指定SQL,如:
     * <pre>
     *     sql("select * from table where name = ?")
     * </pre>
     *
     * @param sql SQL
     * @return this
     */
    static StreamQL.Builder sql(String sql) {
        return new DefaultMicaQLBuilder(sql);
    }

    /**
     * 使用固定的输入作为数据源,将忽略SQL中指定的表
     *
     * @param stream 数据源
     * @return 输出
     */
    default Stream<Map<String, Object>> execute(Stream<?> stream) {
        return execute((table) -> stream);
    }

    /**
     * 指定数据源执行任务并获取Map结果到输出
     * <pre>
     *     ql.query(table->{
     *
     *         return getTableData(table);
     *
     *     })
     * </pre>
     *
     * @param streamSupplier 数据源
     * @return 输出
     */
    Stream<Map<String, Object>> execute(Function<String, Stream<?>> streamSupplier);

    interface Builder {

        /**
         * 设置特性,用于设置自定义函数等操作
         *
         * @param features 特性
         * @return this
         */
        Builder feature(Feature... features);

        /**
         * 构造 MicaQl,请缓存此结果使用.不要每次都调用build.
         *
         * @return MicaQl
         */
        StreamQL build();
    }

    class DefaultMicaQLBuilder implements StreamQL.Builder {
        private final String sql;
        private final List<Feature> features;

        public DefaultMicaQLBuilder(String sql) {
            this.sql = sql;
            this.features = new ArrayList<>();
        }

        @Override
        public Builder feature(Feature... features) {
            this.features.addAll(Arrays.asList(features));
            return this;
        }

        @Override
        public StreamQL build() {
            DefaultStreamQLMetadata metadata = new DefaultStreamQLMetadata(sql);
            metadata.addFeature(features);
            return new DefaultStreamQL(metadata);
        }
    }

}
