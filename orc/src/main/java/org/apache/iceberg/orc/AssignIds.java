package org.apache.iceberg.orc;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.orc.TypeDescription;


class AssignIds extends ORCSchemaUtil.OrcTypeDescriptionVisitor<TypeDescription> {

  private final NameMapping nameMapping;
  private final Deque<String> fieldNames;

  AssignIds(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
    this.fieldNames = new LinkedList<>();
  }

  @Override
  public void beforeField(String fieldName, TypeDescription field) {
    fieldNames.addLast(fieldName);
  }

  @Override
  public void afterField(String fieldName, TypeDescription field) {
    fieldNames.removeLast();
  }

  @Override
  public TypeDescription struct(TypeDescription struct, List<TypeDescription> fieldResults) {
    List<String> names = struct.getFieldNames();
    List<TypeDescription> fieldResultsWithIds = Lists.newArrayListWithExpectedSize(names.size());
    for (int i = 0; i < names.size(); i++) {
      TypeDescription fieldResult = fieldResults.get(i);
      if (fieldResult == null) {
        continue;
      }

      if (ORCSchemaUtil.hasFieldId(fieldResult)) {
        fieldResultsWithIds.add(fieldResult);
      } else {
        Integer fieldId = ORCSchemaUtil.fieldId(fieldResult, nameMapping, names.get(i), fieldNames);
        if (fieldId == null) {
          continue;
        }
        fieldResultsWithIds.add(typeWithId(fieldResult, fieldId));
      }
    }
    // TODO: if pruned, names and fieldResultsWithIds don't match
    return copyStruct(struct, names, fieldResultsWithIds);
  }

  @Override
  public TypeDescription list(TypeDescription list, TypeDescription elementResult) {
    TypeDescription elementResultWithId;
    if (ORCSchemaUtil.hasFieldId(elementResult)) {
      elementResultWithId = elementResult;
    } else {
      Integer fieldId = ORCSchemaUtil.fieldId(elementResult, nameMapping, "element", fieldNames);
      if (fieldId == null) {
        return null;
      }
      elementResultWithId = typeWithId(elementResult, fieldId);
    }
    return copyList(list, elementResultWithId);
  }

  @Override
  public TypeDescription map(TypeDescription map, TypeDescription keyResult, TypeDescription valueResult) {
    TypeDescription keyResultWithId;
    if (ORCSchemaUtil.hasFieldId(keyResult)) {
      keyResultWithId = keyResult;
    } else {
      Integer fieldId = ORCSchemaUtil.fieldId(keyResult, nameMapping, "key", fieldNames);
      if (fieldId == null) {
        return null;
      }
      keyResultWithId = typeWithId(keyResult, fieldId);
    }

    TypeDescription valueResultWithId;
    if (ORCSchemaUtil.hasFieldId(valueResult)) {
      valueResultWithId = valueResult;
    } else {
      Integer fieldId = ORCSchemaUtil.fieldId(valueResult, nameMapping, "value", fieldNames);
      if (fieldId == null) {
        return null;
      }
      valueResultWithId = typeWithId(valueResult, fieldId);
    }
    return copyMap(map, keyResultWithId, valueResultWithId);
  }

  @Override
  public TypeDescription primitive(TypeDescription primitive) {
    return primitive;
  }

  private TypeDescription typeWithId(TypeDescription type, int fieldId) {
    TypeDescription newType = type.clone();
    newType.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, String.valueOf(fieldId));
    return newType;
  }

  private TypeDescription copyStruct(TypeDescription struct, List<String> newNames, List<TypeDescription> newTypes) {
    TypeDescription newStruct = TypeDescription.createStruct();
    for (int i = 0; i < newNames.size(); i++) {
      newStruct.addField(newNames.get(i), newTypes.get(i));
    }
    copyAttributes(struct, newStruct);
    return newStruct;
  }

  private TypeDescription copyList(TypeDescription list, TypeDescription elementType) {
    TypeDescription newList = TypeDescription.createList(elementType);
    copyAttributes(list, newList);
    return newList;
  }

  private TypeDescription copyMap(TypeDescription map, TypeDescription keyType, TypeDescription valueType) {
    TypeDescription newMap = TypeDescription.createMap(keyType, valueType);
    copyAttributes(map, newMap);
    return newMap;
  }

  private void copyAttributes(TypeDescription from, TypeDescription to) {
    for (String attribute: from.getAttributeNames()) {
      to.setAttribute(attribute, from.getAttributeValue(attribute));
    }
  }
}
