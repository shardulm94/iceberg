package org.apache.iceberg.transforms;

import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DatePartition implements Transform<Long, String> {
    private final DateFormat format;
    public DatePartition() {
        format = new SimpleDateFormat("yyyy-MM-dd-00");
        format.setTimeZone(TimeZone.getTimeZone("PST"));
    }

    @Override
    public String apply(Long value) {
        return format.format(new Date(value));
    }

    @Override
    public boolean canTransform(Type type) {
        return type.typeId() == Type.TypeID.INTEGER ||
                type.typeId() == Type.TypeID.DATE ||
                type.typeId() == Type.TypeID.LONG;
    }

    @Override
    public Type getResultType(Type sourceType) {
        return Types.StringType.get();
    }

    @Override
    public UnboundPredicate<String> project(String fieldName, BoundPredicate<Long> predicate) {
        return ProjectionUtil.truncateLong(fieldName, predicate, this);
    }

    @Override
    public UnboundPredicate<String> projectStrict(String name, BoundPredicate<Long> predicate) {
        return null;
    }

    @Override
    public String toString() {
        return "datepartition";
    }
}
