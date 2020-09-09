# spark-positional
Saprk relation for positional file parsing

- features:
- It will be based on Spark fixedWidth parser having capabilities to parse fixed width columns and extra facilities
- Allow missing fields
- extract data if even lesser byte position.

Note: Most of the code are copied from spark FixedWidth parser 

You can set the following fixedwidth-specific options to deal with fixedwidth files:

encoding (default UTF-8): decodes the data files by the given encoding type.
quote (default "): sets a single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string. 
escape (default \): sets a single character used for escaping quotes inside an already quoted value.
charToEscapeQuoteEscaping (default escape or \0): sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise.
comment (default empty string): sets a single character used for skipping lines beginning with this character. By default, it is disabled.
header (default false): uses the first line as names of columns.
enforceSchema (default true): If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in FixedWidth files will be ignored. If the option is set to false, the schema will be validated against all headers in FixedWidth files in the case when the header option is set to true. Field names in the schema and column names in FixedWidth headers are checked by their positions taking into account spark.sql.caseSensitive. Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results.
inferSchema (default false): infers the input schema automatically from data. It requires one extra pass over the data.
samplingRatio (default is 1.0): defines fraction of rows used for schema inferring.
ignoreLeadingWhiteSpace (default false): a flag indicating whether or not leading whitespaces from values being read should be skipped.
ignoreTrailingWhiteSpace (default false): a flag indicating whether or not trailing whitespaces from values being read should be skipped.
nullValue (default empty string): sets the string representation of a null value. Since 2.0.1, this applies to all supported types including the string type.
emptyValue (default empty string): sets the string representation of an empty value.
nanValue (default NaN): sets the string representation of a non-number" value.
positiveInf (default Inf): sets the string representation of a positive infinity value.
negativeInf (default -Inf): sets the string representation of a negative infinity value.
dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSXXX): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
maxColumns (default 20480): defines a hard limit of how many columns a record can have.
maxCharsPerColumn (default -1): defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
extension (default txt) : define output file extension to use while writing
mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes.
PERMISSIVE : when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets other fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. A record with less/more tokens than schema is not a corrupted record to FixedWidth. When it meets a record having fewer tokens than the length of the schema, sets null to extra fields. When the record has more tokens than the length of the schema, it drops extra tokens.
DROPMALFORMED : ignores the whole corrupted records.
FAILFAST : throws an exception when it meets corrupted records.
columnNameOfCorruptRecord (default is the value specified in spark.sql.columnNameOfCorruptRecord): allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.
multiLine (default false): Not Supported yet
fieldLengths : comma separated lengths of each fields to parse or output file
fieldSchema : Json Array with field details as 
[
	{
		"name":"",
		"length":0,
		"startPosition":0,
		"endPosition":0,
		"padding":"\u0000",
		"alignment":"left|centre|right"
	}
]
Above schema lengths given more preferences that start & end position.

Note: Fieldlengths Or fieldSchema should be use ,if both found fieldSchema will given preferances
