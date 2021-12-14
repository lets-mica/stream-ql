package net.dreamlu.stream.ql.test;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;

import java.util.List;

public class DruidTest {

    public static void main(String[] args) {
//         String sql = "select *, count(1) as count from emp e inner join org o on e.org_id = o.id where e.id = ? and o.id = 10";
        String sql = "SELECT clientid, count(1), sum(x), json_encode(payload), hex_decode(y) FROM \"$events/session_subscribed\" WHERE topic = 't/#' and qos = 1 group by id HAVING a > 10 limit 1,10\n";
//        String sql = "select t1.*, t2.* from '/test/1' t1, '/test/2' t2";
//        String sql = "select t1.* from '/test/1' t1";
        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        if (!(statement instanceof SQLSelectStatement)) {
            return;
        }
        SQLSelectStatement selectStatement = (SQLSelectStatement) statement;

        SQLSelect select = selectStatement.getSelect();
        SQLSelectQueryBlock query = (SQLSelectQueryBlock) select.getQuery();
        SQLLimit limit = query.getLimit();

        SQLTableSource from = query.getFrom();
        List<SQLSelectItem> selectList = query.getSelectList();

        System.out.println(statement);
    }
}
