package net.dreamlu.stream.ql.feature;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import net.dreamlu.stream.ql.StreamQLMetadata;
import net.dreamlu.stream.ql.StreamQLRecord;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * 过滤器支持,用来根据表达式创建{@link Predicate}
 *
 * @author zhouhao
 */
public interface FilterFeature extends Feature {

    BiFunction<StreamQLRecord, Object, Boolean> createPredicate(SQLExpr whereExpr, StreamQLMetadata metadata);

    static Optional<BiFunction<StreamQLRecord, Object, Boolean>> createPredicateByExpression(SQLExpr whereExpr, StreamQLMetadata metadata) {
        if (whereExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) whereExpr;
            SQLExpr left = binaryOpExpr.getLeft();
            SQLExpr right = binaryOpExpr.getRight();
            SQLBinaryOperator operator = binaryOpExpr.getOperator();
        }
        return Optional.empty();
    }

    static BiFunction<StreamQLRecord, Object, Boolean> createPredicateNow(SQLExpr whereExpr, StreamQLMetadata metadata) {
        return createPredicateByExpression(whereExpr, metadata).orElseThrow(() -> new UnsupportedOperationException("不支持的条件:" + whereExpr));
    }

}
