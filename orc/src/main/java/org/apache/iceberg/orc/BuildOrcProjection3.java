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


class BuildOrcProjection3 extends OrcSchemaWithTypeVisitor<TypeDescription> {

  @Override
  public TypeDescription record(Types.StructType iStruct, TypeDescription record, List<String> names,
      List<TypeDescription> fields) {
    List<TypeDescription> fieldTypes = record.getChildren();

    Map<Integer, Pair<String, TypeDescription>> updateMap = Maps.newHashMap();
    for (int i = 0; i < fieldTypes.size(); i += 1) {
      int fieldId = ORCSchemaUtil.getIcebergId(fieldTypes.get(i));
      Types.NestedField expectedField = iStruct.field(fieldId);

      // if the field isn't present, it was not selected
      if (expectedField == null) {
        continue;
      }

      updateMap.put(fieldId, Pair.of(names.get(i), fieldTypes.get(i)));
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
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, record.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, record.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
    return toReturn;
  }

  @Override
  public TypeDescription array(Types.ListType iList, TypeDescription array, TypeDescription element) {
    TypeDescription elementType = element;

    // element was changed, create a new array
    if (elementType != array.getChildren().get(0)) {
      TypeDescription toReturn = TypeDescription.createList(elementType);
      toReturn.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, array.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
      toReturn.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, array.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
      return toReturn;
    }

    return array;
  }

  @Override
  public TypeDescription map(Types.MapType iMap, TypeDescription map, TypeDescription key, TypeDescription value) {
    TypeDescription toReturn = TypeDescription.createMap(key, value);
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, map.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
    toReturn.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, map.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
    return toReturn;
  }

  @Override
  public TypeDescription primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
    if (iPrimitive == null ) return primitive;
    // check for type promotion
    TypeDescription promotedType = null;
    switch (primitive.getCategory()) {
      case INT:
        if (iPrimitive.typeId() == Type.TypeID.LONG) {
          promotedType = TypeDescription.createLong();
        }
        break;
      case FLOAT:
        if (iPrimitive.typeId() == Type.TypeID.DOUBLE) {
          promotedType = TypeDescription.createDouble();
        }
        break;
      case DECIMAL:
        if (iPrimitive.typeId() == Type.TypeID.DECIMAL) {
          Types.DecimalType newDecimal = (Types.DecimalType) iPrimitive;
          if (newDecimal.scale() == primitive.getScale() && newDecimal.precision() > primitive.getPrecision()) {
            promotedType = TypeDescription.createDecimal()
                .withScale(newDecimal.scale())
                .withPrecision(newDecimal.precision());
          }
        }
        break;
    }
    if (promotedType == null) {
      Preconditions.checkArgument(ORCSchemaUtil.isSameType(primitive, iPrimitive),
          "Cannot promote %s type to %s", primitive.getCategory(), iPrimitive.typeId().name());
      return primitive;
    } else {
      promotedType.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE));
      promotedType.setAttribute(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE, primitive.getAttributeValue(ORCSchemaUtil.ICEBERG_REQUIRED_ATTRIBUTE));
      return promotedType;
    }
  }
}
