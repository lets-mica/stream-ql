package net.dreamlu.stream.ql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import net.dreamlu.stream.ql.feature.FilterFeature;
import net.dreamlu.stream.ql.feature.FromFeature;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

class DefaultStreamQL implements StreamQL {
    private final StreamQLMetadata metadata;

    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> columnMapper;
    //    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> join;
    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> where;
    //    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> groupBy;
//    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> orderBy;
    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> limit;
    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> offset;
    //    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> distinct;
    private Function<StreamQLContext, Stream<StreamQLRecord>> builder;

    public DefaultStreamQL(StreamQLMetadata metadata) {
        this.metadata = metadata;
        prepare();
    }

    @Override
    public Stream<Map<String, Object>> execute(Function<String, Stream<?>> streamSupplier) {
        DefaultStreamQLContext context = new DefaultStreamQLContext(streamSupplier);
        return builder.apply(context).map(StreamQLRecord::asMap);
    }

    protected void prepare() {
        where = createWhere();
        columnMapper = createMapper();
        limit = createLimit();
        offset = createOffset();
//        groupBy = createGroupBy();
//        join = createJoin();
//        orderBy = createOrderBy();
//        distinct = createDistinct();
        Function<StreamQLContext, Stream<StreamQLRecord>> fromMapper = FromFeature.createFromMapperByBody(metadata.getSelect().getFrom(), metadata);
        builder = ctx ->
                limit.apply(
                        offset.apply(
                                columnMapper.apply(
                                        fromMapper.apply(ctx)
                                )
                        )
                );
    }

    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> createWhere() {
        SQLExpr whereExpr = metadata.getSelect().getWhere();
        if (whereExpr == null) {
            return Function.identity();
        }
        BiFunction<StreamQLRecord, Object, Boolean> filter = FilterFeature.createPredicateNow(whereExpr, metadata);
        return flux -> flux.filter(ctx -> filter.apply(ctx, ctx.getRecord()));
    }

    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> createMapper() {
        return stream -> stream.map(StreamQLRecord::putRecordToResult);
    }

    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> createLimit() {
        SQLLimit limit = metadata.getSelect().getLimit();
        if (limit != null) {
            SQLExpr expr = limit.getRowCount();
            if (expr instanceof SQLIntegerExpr) {
                return stream -> stream.limit(((SQLIntegerExpr) expr).getNumber().longValue());
            }
        }
        return Function.identity();
    }

    private Function<Stream<StreamQLRecord>, Stream<StreamQLRecord>> createOffset() {
        SQLLimit limit = metadata.getSelect().getLimit();
        if (limit != null) {
            SQLExpr expr = limit.getOffset();
            if (expr instanceof SQLIntegerExpr) {
                return stream -> stream.skip(((SQLIntegerExpr) expr).getNumber().longValue());
            }
        }
        return Function.identity();
    }

}
