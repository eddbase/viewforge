<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

SqlDdl SqlCreateCatalog() :
{
    final Span s;
    final SqlIdentifier id;
    final SqlNodeList params;
}
{
    <CREATE> { s = span(); }
    <CATALOG> id = SimpleIdentifier()
    (
        LOOKAHEAD(2)
        params = ParenthesizedKeyValueOptionCommaList()
      |
        [ <LPAREN> <RPAREN> ] { params = SqlNodeList.EMPTY; }
    )
    {
        return new SqlCreateCatalog(s.end(this), id, params);
    }
}

SqlDdl SqlCreateStream() :
{
    final Span s;
    final SqlIdentifier id;
    final SqlIdentifier source;
    final SqlNodeList params;
}
{
    <CREATE> { s = span(); }
    <STREAM> id = SimpleIdentifier()
    <FROM> source = CompoundIdentifier()
    (
        LOOKAHEAD(2)
        params = ParenthesizedKeyValueOptionCommaList()
      |
        [ <LPAREN> <RPAREN> ] { params = SqlNodeList.EMPTY; }
    )
    {
        return new SqlCreateStream(s.end(this), id, source, params);
    }
}

SqlDdl SqlCreateView() :
{
    final Span s;
    final SqlIdentifier id;
    final SqlNodeList params;
    final SqlNode query;
}
{
    <CREATE> { s = span(); }
    <VIEW> id = SimpleIdentifier()
    (
        LOOKAHEAD(2)
        params = ParenthesizedKeyValueOptionCommaList()
      |
        [ <LPAREN> <RPAREN> ] { params = SqlNodeList.EMPTY; }
    )
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(s.end(this), id, params, query);
    }
}

<#--

  Add implementations of additional parser statements, literals or
  data types.

  Example of SqlShowTables() implementation:
  SqlNode SqlShowTables()
  {
    ...local variables...
  }
  {
    <SHOW> <TABLES>
    ...
    {
      return SqlShowTables(...)
    }
  }
-->
