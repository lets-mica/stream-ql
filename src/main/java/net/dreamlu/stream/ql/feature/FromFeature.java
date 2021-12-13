package net.dreamlu.stream.ql.feature;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import net.dreamlu.stream.ql.StreamQLContext;
import net.dreamlu.stream.ql.StreamQLMetadata;
import net.dreamlu.stream.ql.StreamQLRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * 数据源支持,用于自定义from实现
 *
 * @author zhouhao
 * @since 1.0.0
 */
public interface FromFeature extends Feature {

    static Function<StreamQLContext, Stream<StreamQLRecord>> createFromMapperByFrom(SQLTableSource tableSource, StreamQLMetadata metadata) {
        // 1. 普通单表查询
        if (tableSource instanceof SQLExprTableSource) {
            SQLExpr expr = ((SQLExprTableSource) tableSource).getExpr();
            String tableName = getTableName(expr);
            return ctx -> ctx.getDataSource(tableName).map(record -> StreamQLRecord.newRecord(tableName, record, ctx));
        } else if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) tableSource;
            List<String> tableList = new ArrayList<>();
            getJoinTables(tableList, joinTableSource);
            return ctx -> tableList.stream().flatMap(tableName ->
                    ctx.getDataSource(tableName).map(record -> StreamQLRecord.newRecord(tableName, record, ctx))
            );
        }
        // TODO L.cm 解析更多类型的 sql
        throw new IllegalArgumentException("不支持的 sql 语法");
    }

    static Function<StreamQLContext, Stream<StreamQLRecord>> createFromMapperByBody(SQLTableSource tableSource, StreamQLMetadata metadata) {
        return createFromMapperByFrom(tableSource, metadata);
    }

    static void getJoinTables(List<String> tableList, SQLJoinTableSource joinTableSource) {
        SQLJoinTableSource.JoinType joinType = joinTableSource.getJoinType();
        if (SQLJoinTableSource.JoinType.COMMA == joinType) {
            SQLTableSource right = joinTableSource.getRight();
            if (right instanceof SQLExprTableSource) {
                SQLExpr expr = ((SQLExprTableSource) right).getExpr();
                tableList.add(getTableName(expr));
            } else {

            }
            SQLTableSource left = joinTableSource.getLeft();
            if (left instanceof SQLExprTableSource) {
                String tableName = ((SQLExprTableSource) left).getName().getSimpleName();
                tableList.add(tableName);
            } else if (left instanceof SQLJoinTableSource) {
                getJoinTables(tableList, (SQLJoinTableSource) left);
            }
        }
    }

    static String getTableName(SQLExpr expr) {
        if (expr instanceof SQLIdentifierExpr) {
            return ((SQLIdentifierExpr) expr).getName();
        } if (expr instanceof SQLCharExpr) {
            return ((SQLCharExpr) expr).getText();
        }
        return null;
    }

}
