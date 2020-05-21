package org.apache.iceberg.orc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.orc.TypeDescription;


class BuildOrcProjection extends OrcCustomOrderTypeDescriptionVisitor<TypeDescription> {

  private Type current;

  BuildOrcProjection(Schema expectedSchema) {
    this.current = expectedSchema.asStruct();
  }

  @Override
  public TypeDescription struct(TypeDescription struct, List<String> names, Iterable<TypeDescription> fieldsIterable) {
    Preconditions.checkArgument(
        current.isNestedType() && current.asNestedType().isStructType(),
        "Cannot project non-struct: %s", current);

    Types.StructType iStruct = current.asNestedType().asStructType();

    List<TypeDescription> fieldTypes = struct.getChildren();
    Iterator<TypeDescription> iter = fieldsIterable.iterator();
    Map<Integer, Pair<String, TypeDescription>> updateMap = Maps.newHashMap();
    for (int i = 0; i < fieldTypes.size(); i += 1) {
      int fieldId = ORCSchemaUtil.getIcebergId(fieldTypes.get(i));
      Types.NestedField expectedField = iStruct.field(fieldId);

      // if the field isn't present, it was not selected
      if (expectedField == null) {
        this.current = ORCSchemaUtil.convertOrcToIceberg(fieldTypes.get(i), "F", new TypeUtil.NextID() {
          @Override
          public int get() {
            return 0;
          }
        }).type();
        try {
          iter.next();
        } finally {
          this.current = iStruct;
        }
        continue;
      }

      this.current = expectedField.type();
      try {
        updateMap.put(fieldId, Pair.of(names.get(i), iter.next()));
      } finally {
        this.current = iStruct;
      }
    }

    // construct the schema using the expected order
    List<Pair<String, TypeDescription>> updatedFields = Lists.newArrayListWithExpectedSize(iStruct.fields().size());
    List<Types.NestedField> expectedFields = iStruct.fields();
    for (Types.NestedField field : expectedFields) {
      Pair<String, TypeDescription> orcField = updateMap.get(field.fieldId());

      if (orcField != null) {
        updatedFields.add(orcField);
      } else {
        Preconditions.checkArgument(field.isOptional(), "Missing required field: %s", field.name());
        // Create a field that will be defaulted to null. We assign a unique suffix to the field
        // to make sure that even if records in the file have the field it is not projected.
        String updatedName = field.name() + "_r" + field.fieldId();
        updatedFields.add(
            Pair.of(updatedName, ORCSchemaUtil.convert(field.fieldId(), field.type(), field.isRequired())));
      }
    }

    // TODO: create copy utility
    TypeDescription toReturn = TypeDescription.createStruct();
    for (Pair<String, TypeDescription> f : updatedFields) {
      toReturn.addField(f.first(), f.second());
    }
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, struct.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, struct.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
    return toReturn;
  }

  @Override
  public TypeDescription union(TypeDescription union, Iterable<TypeDescription> options) {
    throw new UnsupportedOperationException("Union type not supported");
  }

  @Override
  public TypeDescription list(TypeDescription list, Supplier<TypeDescription> element) {
    Preconditions.checkArgument(current.isListType(),
        "Incompatible projected type: %s", current);
    Types.ListType iList = current.asNestedType().asListType();
    this.current = iList.elementType();
    try {
      TypeDescription elementType = element.get();

      // element was changed, create a new array
      if (elementType != list.getChildren().get(0)) {
        TypeDescription toReturn = TypeDescription.createList(elementType);
        toReturn.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, list.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
        toReturn.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, list.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
        return toReturn;
      }

      return list;

    } finally {
      this.current = iList;
    }
  }

  @Override
  public TypeDescription map(TypeDescription map, Supplier<TypeDescription> key, Supplier<TypeDescription> value) {
    Preconditions.checkArgument(current.isNestedType() && current.asNestedType().isMapType(),
        "Incompatible projected type: %s", current);
    Types.MapType asMapType = current.asNestedType().asMapType();
    Preconditions.checkArgument(asMapType.keyType() == Types.StringType.get(),
        "Incompatible projected type: key type %s is not string", asMapType.keyType());
    TypeDescription keyType;
    TypeDescription valueType;
    this.current = asMapType.keyType();
    try {
      keyType = key.get();
    } finally {
      this.current = asMapType;
    }
    this.current = asMapType.valueType();
    try {
      valueType = value.get();
    } finally {
      this.current = asMapType;
    }
    TypeDescription toReturn = TypeDescription.createMap(keyType, valueType);
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, map.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, map.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
    return toReturn;
  }

  @Override
  public TypeDescription primitive(TypeDescription primitive) {
    // check for type promotion
    TypeDescription promotedType = null;
    switch (primitive.getCategory()) {
      case INT:
        if (current.typeId() == Type.TypeID.LONG) {
          promotedType = TypeDescription.createLong();
        }
        break;
      case FLOAT:
        if (current.typeId() == Type.TypeID.DOUBLE) {
          promotedType = TypeDescription.createDouble();
        }
        break;
      case DECIMAL:
        if (current.typeId() == Type.TypeID.DECIMAL) {
          Types.DecimalType newDecimal = (Types.DecimalType) current;
          if (newDecimal.scale() == primitive.getScale() && newDecimal.precision() > primitive.getPrecision()) {
            promotedType = TypeDescription.createDecimal()
                .withScale(newDecimal.scale())
                .withPrecision(newDecimal.precision());
          }
        }
        break;
    }
    if (promotedType == null) {
      Preconditions.checkArgument(ORCSchemaUtil.isSameType(primitive, current),
          "Cannot promote %s type to %s", primitive.getCategory(), current.typeId().name());
      return primitive;
    } else {
      promotedType.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
      promotedType.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
      return promotedType;
    }
  }
}
