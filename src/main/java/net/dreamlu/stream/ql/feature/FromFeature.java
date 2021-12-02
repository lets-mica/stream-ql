package net.dreamlu.stream.ql.feature;

import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import net.dreamlu.stream.ql.StreamQLContext;
import net.dreamlu.stream.ql.StreamQLMetadata;
import net.dreamlu.stream.ql.StreamQLRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * 数据源支持,用于自定义from实现
 *
 * @author zhouhao
 * @since 1.0.0
 */
public interface FromFeature extends Feature {

    static Function<StreamQLContext, Stream<StreamQLRecord>> createFromMapperByFrom(Map<TableStat.Name, TableStat> tables, StreamQLMetadata metadata) {
        List<String> tableList = new ArrayList<>();
        tables.forEach((table, tableStat) -> {
            String tableName = table.getName();
            tableList.add(tableName);
        });
        return ctx -> tableList.stream().flatMap((tableName) ->
            ctx.getDataSource(tableName).map(record -> StreamQLRecord.newRecord(tableName, record, ctx))
        );
    }

    static Function<StreamQLContext, Stream<StreamQLRecord>> createFromMapperByBody(MySqlSchemaStatVisitor visitor, StreamQLMetadata metadata) {
        return createFromMapperByFrom(visitor.getTables(), metadata);
    }

}
