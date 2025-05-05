package eddbase.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlCreateView extends SqlDdl {

    public final SqlIdentifier name;
    public final SqlNodeList params;
    public final SqlNode query;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE VIEW", SqlKind.OTHER_DDL);

    public SqlCreateView(SqlParserPos pos, SqlIdentifier name, SqlNodeList params, SqlNode query) {
        super(OPERATOR, pos);
        this.name = requireNonNull(name, "view name must not be null");
        this.params = requireNonNull(params, "view params must not be null");
        this.query = requireNonNull(query, "view query must not be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, params, query);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("VIEW");
        name.unparse(writer, leftPrec, rightPrec);

        List<SqlNode> paramList = params.getList();
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 1; i < paramList.size(); i += 2) {
            writer.sep(",");
            paramList.get(i - 1).unparse(writer, leftPrec, rightPrec);
            writer.keyword("=");
            paramList.get(i).unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
        writer.keyword("AS");
        query.unparse(writer, leftPrec, rightPrec);
    }
}
