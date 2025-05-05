package eddbase.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlCreateCatalog extends SqlDdl {
    public final SqlIdentifier name;
    public final SqlNodeList params;

    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("CREATE CATALOG", SqlKind.OTHER_DDL);

    public SqlCreateCatalog(SqlParserPos pos, SqlIdentifier name, SqlNodeList params) {
        super(OPERATOR, pos);
        this.name = requireNonNull(name, "catalog name must not be null");
        this.params = requireNonNull(params, "catalog params must not be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, params);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("CATALOG");
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
    }
}
