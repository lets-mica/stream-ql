package net.dreamlu.stream.ql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import net.dreamlu.stream.ql.feature.Feature;
import net.dreamlu.stream.ql.feature.FeatureId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultStreamQLMetadata implements StreamQLMetadata {
    private static final Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    private final SQLSelectQueryBlock query;
    private final MySqlSchemaStatVisitor visitor;

    private final Map<String, Feature> features = new ConcurrentHashMap<>(globalFeatures);
    private final Map<String, Object> settings = new ConcurrentHashMap<>();

    DefaultStreamQLMetadata(String sql) {
        SQLSelectStatement selectStatement = getSQLSelectStatement(sql);
        this.query = selectStatement.getSelect().getQueryBlock();
        this.visitor = getSqlVisitor(selectStatement);
    }

    private static SQLSelectStatement getSQLSelectStatement(String sql) {
        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        if (statement instanceof SQLSelectStatement) {
            return (SQLSelectStatement) statement;
        }
        throw new IllegalArgumentException(String.format("Sql [%s] is not select sql!", sql));
    }

    private static MySqlSchemaStatVisitor getSqlVisitor(SQLSelectStatement selectStatement) {
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        selectStatement.accept(visitor);
        return visitor;
    }

    public void addFeature(Feature... features) {
        addFeature(Arrays.asList(features));
    }

    public void addFeature(Collection<Feature> features) {
        for (Feature feature : features) {
            this.features.put(feature.getId().toLowerCase(), feature);
        }
    }

    @Override
    public <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId) {
        return Optional.ofNullable((T) features.get(featureId.getId().toLowerCase()));
    }

    @Override
    public StreamQLMetadata setting(String key, Object value) {
        settings.put(key, value);
        return this;
    }

    @Override
    public Optional<Object> getSetting(String key) {
        return Optional.ofNullable(settings.get(key));
    }

    @Override
    public SQLSelectQueryBlock getQuery() {
        return this.query;
    }

    @Override
    public MySqlSchemaStatVisitor getSqlVisitor() {
        return this.visitor;
    }

}
