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

package com.netflix.exhibitor.core.config;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EncodedConfigParser
{
    private final List<FieldValue> fieldValues;

    public static class FieldValue
    {
        private final String    field;
        private final String    value;

        public FieldValue(String field, String value)
        {
            this.field = field;
            this.value = value;
        }

        public String getField()
        {
            return field;
        }

        public String getValue()
        {
            return value;
        }

        @SuppressWarnings("RedundantIfStatement")
        @Override
        public boolean equals(Object o)
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            FieldValue that = (FieldValue)o;

            if ( !field.equals(that.field) )
            {
                return false;
            }
            if ( !value.equals(that.value) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = field.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }
    }
    
    public EncodedConfigParser(String encodedValue)
    {
        if ( encodedValue == null )
        {
            encodedValue = "";
        }

        ImmutableList.Builder<FieldValue> builder = ImmutableList.builder();

        Set<String>     usedKeys = Sets.newHashSet();
        String[]        parts = encodedValue.split("&");
        try
        {
            for ( String part : parts )
            {
                String[]      subParts = part.split("=");
                if ( subParts.length == 2 )
                {
                    usedKeys.add(subParts[0]);
                    builder.add(new FieldValue(URLDecoder.decode(subParts[0], "UTF-8"), URLDecoder.decode(subParts[1], "UTF-8")));
                }
            }
        }
        catch ( UnsupportedEncodingException e )
        {
            // should never get here
            throw new Error(e);
        }

        fieldValues = builder.build();
    }

    /**
     * @param fieldValues runtime values
     */
    public EncodedConfigParser(List<FieldValue> fieldValues)
    {
        this(fieldValues, false);
    }

    /**
     * @param fieldValues runtime values
     * @param sort if true, sort the values first
     */
    public EncodedConfigParser(List<FieldValue> fieldValues, boolean sort)
    {
        List<FieldValue>        localFieldValues;
        if ( sort )
        {
            localFieldValues = Lists.newArrayList(fieldValues);
            Collections.sort
            (
                localFieldValues,
                new Comparator<FieldValue>()
                {
                    @Override
                    public int compare(FieldValue o1, FieldValue o2)
                    {
                        return o1.getField().compareTo(o2.getField());
                    }
                }
            );
        }
        else
        {
            localFieldValues = fieldValues;
        }

        this.fieldValues = ImmutableList.copyOf(localFieldValues);
    }

    /**
     * Return the values
     *
     * @return values
     */
    public List<FieldValue> getFieldValues()
    {
        return fieldValues;
    }

    /**
     * Return a sorted map of the fields/values
     *
     * @return map
     */
    public Map<String, String> getSortedMap()
    {
        ImmutableSortedMap.Builder<String, String> builder = ImmutableSortedMap.naturalOrder();
        for ( FieldValue fv : fieldValues )
        {
            builder.put(fv.getField(), fv.getValue());
        }

        return builder.build();
    }

    /**
     * Return the value for the given field or null
     *
     * @param field field to check
     * @return value or null
     */
    public String getValue(final String field)
    {
        FieldValue fieldValue = Iterables.find
        (
            fieldValues,
            new Predicate<FieldValue>()
            {
                @Override
                public boolean apply(FieldValue fv)
                {
                    return fv.getField().equals(field);
                }
            },
            null
        );
        return (fieldValue != null) ? fieldValue.getValue() : null;
    }

    /**
     * @return storable value
     */
    public String   toEncoded()
    {
        StringBuilder       str = new StringBuilder();
        try
        {
            for ( FieldValue fv : fieldValues )
            {
                if ( str.length() > 0 )
                {
                    str.append("&");
                }
                str.append(URLEncoder.encode(fv.getField(), "UTF8"));
                str.append("=");
                str.append(URLEncoder.encode(fv.getValue(), "UTF8"));
            }
        }
        catch ( UnsupportedEncodingException e )
        {
            // should never get here
            throw new Error(e);
        }

        return str.toString();
    }
}
