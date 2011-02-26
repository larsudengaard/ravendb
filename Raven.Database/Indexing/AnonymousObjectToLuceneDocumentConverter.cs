//-----------------------------------------------------------------------
// <copyright file="AnonymousObjectToLuceneDocumentConverter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using Lucene.Net.Documents;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Extensions;
using Raven.Database.Linq;

namespace Raven.Database.Indexing
{
    public class AnonymousObjectToLuceneDocumentConverter
    {
        private readonly IndexDefinition indexDefinition;
        private const string NullValueMarker = "NULL_VALUE";
        private List<int> multipleItemsSameFieldCount = new List<int>();
        private readonly Dictionary<object, Field> fieldsCache = new Dictionary<object, Field>();
        private readonly Dictionary<object, NumericField> numericFieldsCache = new Dictionary<object, NumericField>();

        public AnonymousObjectToLuceneDocumentConverter(IndexDefinition indexDefinition)
        {
            this.indexDefinition = indexDefinition;
        }

        public IEnumerable<AbstractField> Index(object val,
            PropertyDescriptorCollection properties,
            Field.Store defaultStorage)
        {
            return (from property in properties.Cast<PropertyDescriptor>()
                    let name = property.Name
                    where name != "__document_id"
                    let value = property.GetValue(val)
                    from field in CreateFields(name, value, defaultStorage)
                    select field);
        }

        public IEnumerable<AbstractField> Index(JObject document,
            Field.Store defaultStorage)
        {
            return (from property in document.Cast<JProperty>()
                    let name = property.Name
                    where name != "__document_id"
                    let value = GetPropertyValue(property)
                    from field in CreateFields(name, value, defaultStorage)
                    select field);
        }

        private object GetPropertyValue(JProperty property)
        {
            switch (property.Value.Type)
            {
                case JTokenType.Array:
                case JTokenType.Object:
                    return property.Value.ToString(Formatting.None);
                default:
                    return property.Value.Value<object>();
            }
        }

        /// <summary>
        /// This method generate the fields for indexing documents in lucene from the values.
        /// Given a name and a value, it has the following behavior:
        /// * If the value is enumerable, index all the items in the enumerable under the same field name
        /// * If the value is null, create a single field with the supplied name with the unanalyzed value 'NULL_VALUE'
        /// * If the value is string or was set to not analyzed, create a single field with the supplied name
        /// * If the value is date, create a single field with millisecond precision with the supplied name
        /// * If the value is numeric (int, long, double, decimal, or float) will create two fields:
        ///		1. with the supplied name, containing the numeric value as an unanalyzed string - useful for direct queries
        ///		2. with the name: name +'_Range', containing the numeric value in a form that allows range queries
        /// </summary>
        private IEnumerable<AbstractField> CreateFields(string name,
            object value,
            Field.Store defaultStorage)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Field must be not null, not empty and cannot contain whitespace", "name");
          
            if (char.IsLetter(name[0]) == false &&
                name[0] != '_')
            {
                name = "_" + name;
            }

            if (value == null)
            {
                yield return
                    CreateFieldWithCaching(name, NullValueMarker, Field.Index.NOT_ANALYZED,
                                           indexDefinition.GetStorage(name, defaultStorage));
                yield break;
            }
            if (value is DynamicNullObject)
            {
                if (((DynamicNullObject) value).IsExplicitNull)
                {
                    yield return
                        CreateFieldWithCaching(name, NullValueMarker, Field.Index.NOT_ANALYZED,
                                               indexDefinition.GetStorage(name, defaultStorage));
                }
                yield break;
            }

            if (value is AbstractField)
            {
                yield return (AbstractField) value;
                yield break;
            }


            var itemsToIndex = value as IEnumerable;
            if (itemsToIndex != null && ShouldTreatAsEnumerable(itemsToIndex))
            {
                string name1 = name + "_IsArray";
                Field.Store defaultStorage1 = Field.Store.YES;
                yield return
                    CreateFieldWithCaching(name1, "true", Field.Index.NOT_ANALYZED_NO_NORMS,
                                           indexDefinition.GetStorage(name1, defaultStorage1));

                int count = 1;
                foreach (var itemToIndex in itemsToIndex)
                {
                    multipleItemsSameFieldCount.Add(count++);
                    foreach (var field in CreateFields(name, itemToIndex, defaultStorage))
                    {
                        yield return field;
                    }
                    multipleItemsSameFieldCount.RemoveAt(multipleItemsSameFieldCount.Count - 1);
                }
                yield break;
            }

            if (indexDefinition.GetIndex(name, null) == Field.Index.NOT_ANALYZED) // explicitly not analyzed
            {
                yield return CreateFieldWithCaching(name, value.ToString(),
                                                    indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED),
                                                    indexDefinition.GetStorage(name, defaultStorage));
                yield break;
            }
            if (value is string)
            {
                var index = indexDefinition.GetIndex(name, Field.Index.ANALYZED);
                yield return
                    CreateFieldWithCaching(name, value.ToString(), index,
                                           indexDefinition.GetStorage(name, defaultStorage));
                yield break;
            }

            if (value is DateTime)
            {
                yield return CreateFieldWithCaching(name,
                                                    DateTools.DateToString((DateTime) value,
                                                                           DateTools.Resolution.MILLISECOND),
                                                    indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED),
                                                    indexDefinition.GetStorage(name, defaultStorage));
            }
            else if (value is bool)
            {
                yield return CreateFieldWithCaching(name,
                                                    ((bool) value) ? "true" : "false",
                                                    indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED),
                                                    indexDefinition.GetStorage(name, defaultStorage));
            }
            else if (value is IConvertible) // we need this to store numbers in invariant format, so JSON could read them
            {
                var convert = ((IConvertible) value);
                yield return CreateFieldWithCaching(name, convert.ToString(CultureInfo.InvariantCulture),
                                                    indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED),
                                                    indexDefinition.GetStorage(name, defaultStorage));
            }
            else if (value is DynamicJsonObject)
            {
                var inner = ((DynamicJsonObject) value).Inner;
                string convertToJsonMarker = name + "_ConvertToJson";
                yield return CreateFieldWithCaching(convertToJsonMarker, "true",
                                                    Field.Index.NOT_ANALYZED_NO_NORMS,
                                                    indexDefinition.GetStorage(convertToJsonMarker, Field.Store.YES))
                    ;
                yield return CreateFieldWithCaching(name, inner.ToString(),
                                                    indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED),
                                                    indexDefinition.GetStorage(name, defaultStorage));
            }
            else
            {
                yield return
                    CreateFieldWithCaching(name + "_ConvertToJson", "true", Field.Index.NOT_ANALYZED_NO_NORMS,
                                           Field.Store.YES);
                yield return CreateFieldWithCaching(name, JToken.FromObject(value).ToString(),
                                                    indexDefinition.GetIndex(name, Field.Index.NOT_ANALYZED),
                                                    indexDefinition.GetStorage(name, defaultStorage));
            }


            var numericFieldWithCaching = CreateNumericFieldWithCaching(name, value, defaultStorage);
            if (numericFieldWithCaching != null)
                yield return numericFieldWithCaching;
        }

        private NumericField CreateNumericFieldWithCaching(string name, object value, Field.Store defaultStorage)
        {
            var fieldName = name + "_Range";
            var storage = indexDefinition.GetStorage(name, defaultStorage);
            var cacheKey = new
            {
                fieldName, 
                storage,
                multipleItemsSameFieldCountSum = multipleItemsSameFieldCount.Sum()
            };
            NumericField numericField;
            if(numericFieldsCache.TryGetValue(cacheKey, out numericField) == false)
            {
                numericFieldsCache[cacheKey] = numericField = new NumericField(fieldName, storage, true);
            }
            if (value is int)
            {
                return indexDefinition.GetSortOption(name) == SortOptions.Long ? 
                    numericField.SetLongValue((int)value) : 
                    numericField.SetIntValue((int)value);
            }
            if (value is long)
            {
                return numericField.SetLongValue((long)value);
            }
            if (value is decimal)
            {
                return numericField
                   .SetDoubleValue((double)(decimal)value);
            }
            if (value is float)
            {
                return indexDefinition.GetSortOption(name) == SortOptions.Double ? 
                    numericField.SetDoubleValue((float)value) : 
                    numericField.SetFloatValue((float)value);
            }
            if (value is double)
            {
                return numericField.SetDoubleValue((double)value);
            }
            return null;
        }

        private Field CreateFieldWithCaching(string name, string value, Field.Index index, Field.Store store)
        {
            var cacheKey = new
            {
                name, 
                index, 
                store,
                multipleItemsSameFieldCountSum = multipleItemsSameFieldCount.Sum()
            };
            Field field;
            if(fieldsCache.TryGetValue(cacheKey, out field)==false)
            {
                fieldsCache[cacheKey] = field = new Field(name, value, store, index);
            }
            field.SetValue(value);
            return field;
        }

        private static bool ShouldTreatAsEnumerable(IEnumerable itemsToIndex)
        {
            if (itemsToIndex == null)
                return false;

            if (itemsToIndex is DynamicJsonObject)
                return false;

            if (itemsToIndex is string)
                return false;

            if (itemsToIndex is JObject)
                return false;

            if (itemsToIndex is IDictionary)
                return false;

            return true;
        }
    }
}
