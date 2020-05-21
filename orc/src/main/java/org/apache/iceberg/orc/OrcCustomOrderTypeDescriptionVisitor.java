package org.apache.iceberg.orc;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.function.Supplier;
import org.apache.orc.TypeDescription;

abstract class OrcCustomOrderTypeDescriptionVisitor<T> {
  public static <T> T visit(TypeDescription type, OrcCustomOrderTypeDescriptionVisitor<T> visitor) {
    switch (type.getCategory()) {
      case STRUCT:
        List<TypeDescription> fields = type.getChildren();
        List<String> names = type.getFieldNames();
        List<Supplier<T>> fieldResults = Lists.newArrayListWithExpectedSize(fields.size());
        for (TypeDescription field : fields) {
          fieldResults.add(new OrcCustomOrderTypeDescriptionVisitor.VisitFuture<>(field, visitor));
        }

        return visitor.struct(type, names, Iterables.transform(fieldResults, Supplier::get));

      case UNION:
        List<TypeDescription> options = type.getChildren();
        List<Supplier<T>> optionResults = Lists.newArrayListWithExpectedSize(options.size());
        for (TypeDescription option : options) {
          optionResults.add(new OrcCustomOrderTypeDescriptionVisitor.VisitFuture<>(option, visitor));
        }

        return visitor.union(type, Iterables.transform(optionResults, Supplier::get));

      case LIST:
        return visitor.list(type,
            new OrcCustomOrderTypeDescriptionVisitor.VisitFuture<>(type.getChildren().get(0), visitor));

      case MAP:
        return visitor.map(type,
            new OrcCustomOrderTypeDescriptionVisitor.VisitFuture<>(type.getChildren().get(0), visitor),
            new OrcCustomOrderTypeDescriptionVisitor.VisitFuture<>(type.getChildren().get(1), visitor));

      default:
        return visitor.primitive(type);
    }
  }

  public T struct(TypeDescription struct, List<String> names, Iterable<T> fields) {
    return null;
  }

  public T union(TypeDescription union, Iterable<T> options) {
    return null;
  }

  public T list(TypeDescription list, Supplier<T> element) {
    return null;
  }

  public T map(TypeDescription map, Supplier<T> key, Supplier<T> value) {
    return null;
  }

  public T primitive(TypeDescription primitive) {
    return null;
  }

  private static class VisitFuture<T> implements Supplier<T> {
    private final TypeDescription type;
    private final OrcCustomOrderTypeDescriptionVisitor<T> visitor;

    private VisitFuture(TypeDescription type, OrcCustomOrderTypeDescriptionVisitor<T> visitor) {
      this.type = type;
      this.visitor = visitor;
    }

    @Override
    public T get() {
      return visit(type, visitor);
    }
  }
}
