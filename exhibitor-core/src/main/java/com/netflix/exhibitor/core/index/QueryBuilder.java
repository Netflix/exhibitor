/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.exhibitor.core.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import java.util.Collection;
import java.util.Date;

public class QueryBuilder
{
    private final Collection<Query> queries;
    
    public static QueryBuilder      builder()
    {
        return new QueryBuilder();
    }

    public enum Type
    {
        AND,
        OR
    }

    public Query            build(Type type)
    {
        Preconditions.checkArgument(queries.size() > 0, "Query is empty");

        if ( queries.size() == 1 )
        {
            return queries.iterator().next();
        }

        BooleanQuery        query = new BooleanQuery();
        for ( Query q : queries )
        {
            query.add(q, (type == Type.AND) ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD);
        }

        return query;
    }
    
    public QueryBuilder pathExact(String path)
    {
        Term            term = new Term(FieldNames.PATH, path);
        queries.add(new TermQuery(term));
        return this;
    }

    public QueryBuilder pathPrefix(String pathPrefix)
    {
        Term            term = new Term(FieldNames.PATH, pathPrefix);
        queries.add(new PrefixQuery(term));
        return this;
    }

    public QueryBuilder     versionRange(int startVersion, int endVersion)
    {
        NumericRangeQuery<Integer> query = NumericRangeQuery.newIntRange(FieldNames.VERSION, startVersion, endVersion, true, false);
        queries.add(query);
        return this;
    }

    public QueryBuilder     dateRange(Date startDate, Date endDate)
    {
        NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange(FieldNames.DATE, startDate.getTime(), endDate.getTime(), true, false);
        queries.add(query);
        return this;
    }

    public QueryBuilder operationType(int type)
    {
        Term            term = new Term(FieldNames.TYPE, Integer.toString(type));
        queries.add(new TermQuery(term));
        return this;
    }

    private QueryBuilder()
    {
        queries = Lists.newArrayList();
    }
}
