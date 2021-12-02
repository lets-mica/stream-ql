package net.dreamlu.stream.ql.test;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class DruidTest {

    public static void main(String[] args) {
//         String sql = "select *, count(1) as count from emp e inner join org o on e.org_id = o.id where e.id = ? and o.id = 10";
        String sql = "SELECT clientid, count(1), sum(x), json_encode(payload), hex_decode(y) FROM \"$events/session_subscribed\" WHERE topic = 't/#' and qos = 1 group by id HAVING a > 10 limit 1,10\n";
        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        if (!(statement instanceof SQLSelectStatement)) {
            return;
        }
        SQLSelectStatement selectStatement = (SQLSelectStatement) statement;

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        selectStatement.accept(visitor);

        SQLSelect select = selectStatement.getSelect();
        SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
        SQLLimit limit = query.getLimit();

        Predicate<Object> tablePredicate = new Predicate<Object>() {
            @Override
            public boolean test(Object o) {
                return false;
            }
        };

        Function<Object, Object> tableFun = new Function<Object, Object>() {
            @Override
            public Object apply(Object o) {
                return null;
            }
        };

        Map<TableStat.Name, TableStat> tables = visitor.getTables();
        tables.forEach((table, tableStat) -> {
            System.out.println(table.getName());
            if (!tablePredicate.test(table.getName())) {
                return;
            }
        });

        List<TableStat.Condition> conditions = visitor.getConditions();
        conditions.forEach(condition -> {
            TableStat.Column column = condition.getColumn();
            String columnName = column.getName();
            String operator = condition.getOperator();
            List<Object> values = condition.getValues();
        });

        Collection<TableStat.Column> columns = visitor.getColumns();

        Map<String, Object> result = new HashMap<>();
        columns.forEach(column -> {
            if (column.isSelect()) {
                String columnName = column.getName();
                if ("*".equals(columnName)) {
                    // 所有数据
                }
                System.out.println(columnName);
            }
        });

        List<SQLAggregateExpr> aggregateFunctions = visitor.getAggregateFunctions();
        aggregateFunctions.forEach(function -> {
            String methodName = function.getMethodName();
            List<SQLExpr> arguments = function.getArguments();
            System.out.println(function);
            arguments.forEach(argument -> {
                // 可能是子查询
                if (argument instanceof SQLIntegerExpr) {
                    SQLIntegerExpr expr = (SQLIntegerExpr) argument;
                    Number exprNumber = expr.getNumber();
                }
                System.out.println(argument);
            });
        });

        List<SQLMethodInvokeExpr> functions = visitor.getFunctions();
        functions.forEach(function -> {
            String methodName = function.getMethodName();
            List<SQLExpr> arguments = function.getArguments();
            System.out.println(function);
        });

        System.out.println(statement);
    }
}
