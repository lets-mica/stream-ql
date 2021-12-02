package net.dreamlu.stream.ql;

import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import net.dreamlu.stream.ql.feature.Feature;
import net.dreamlu.stream.ql.feature.FeatureId;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * 元数据,用于管理特性,进行配置等操作
 *
 * @author zhouhao
 * @since 1.0.0
 */
public interface StreamQLMetadata {

    /**
     * 获取特性
     *
     * @param featureId 特性ID
     * @param <T>       特性类型
     * @return 特性
     */
    <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId);

    /**
     * 获取设置
     *
     * @param key key
     * @return 设置内容
     */
    Optional<Object> getSetting(String key);

    /**
     * 自定义设置
     *
     * @param key   key
     * @param value value
     * @return this
     */
    StreamQLMetadata setting(String key, Object value);

    /**
     * 获取原始SQL
     *
     * @return SQL
     */
    SQLSelectQueryBlock getQuery();

    /**
     * 获取 sql 解析起
     *
     * @return sqlVisitor
     */
    MySqlSchemaStatVisitor getSqlVisitor();

    /**
     * 获取特性,如果不存在则抛出异常
     *
     * @param featureId 特性ID
     * @param <T>       特性类型
     * @return 特性
     */
    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId) {
        return getFeatureNow(featureId, featureId::getId);
    }

    /**
     * 获取特性,如果特性不存在则使用指定等错误消息抛出异常
     *
     * @param featureId    特性ID
     * @param errorMessage 错误消息
     * @param <T>          特性类型
     * @return 特性
     */
    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId, Supplier<String> errorMessage) {
        return getFeature(featureId)
                .orElseThrow(() -> new UnsupportedOperationException("unsupported feature: " + errorMessage.get()));
    }


}
