package eddbase.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlCreateStream extends SqlDdl {

    public final SqlIdentifier name;
    public final SqlIdentifier sourceTable;
    public final SqlNodeList params;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE STREAM", SqlKind.OTHER_DDL);

    public SqlCreateStream(SqlParserPos pos, SqlIdentifier name, SqlIdentifier sourceTable, SqlNodeList params) {
        super(OPERATOR, pos);
        this.name = requireNonNull(name, "stream name must not be null");
        this.sourceTable = requireNonNull(sourceTable, "stream source must not be null");
        this.params = requireNonNull(params, "stream params must not be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, sourceTable, params);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("STREAM");
        name.unparse(writer, leftPrec, rightPrec);
        writer.keyword("FROM");
        sourceTable.unparse(writer, leftPrec, rightPrec);

        List<SqlNode> paramList = params.getList();
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 1; i < paramList.size(); i += 2) {
            writer.sep(",");
            paramList.get(i - 1).unparse(writer, leftPrec, rightPrec);
            writer.keyword("=");
            paramList.get(i).unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }

}
