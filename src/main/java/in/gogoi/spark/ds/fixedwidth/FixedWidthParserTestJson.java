package in.gogoi.spark.ds.fixedwidth;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
/**
 * @author lieuranjan
 * Created on 12/9/20
 */
public class FixedWidthParserTestJson {
    public static void main(String[] args) {
        /*FixedWidthFields fixedWidthFields = new FixedWidthFields();
        fixedWidthFields.addField("seq", 4, FieldAlignment.RIGHT, '0');
        fixedWidthFields.addField("name_first", 10, FieldAlignment.LEFT);
        fixedWidthFields.addField("name_last", 10, FieldAlignment.LEFT);
        fixedWidthFields.addField("age", 2, FieldAlignment.RIGHT);
        fixedWidthFields.addField("street", 20, FieldAlignment.LEFT);
        fixedWidthFields.addField("city", 10, FieldAlignment.LEFT);
        fixedWidthFields.addField("state", 10, FieldAlignment.LEFT);
        fixedWidthFields.addField("zip", 10, FieldAlignment.CENTER);
        fixedWidthFields.addField("dollar", 20, FieldAlignment.RIGHT);
        fixedWidthFields.addField("pick", 6, FieldAlignment.LEFT);
        fixedWidthFields.addField("date", 10, FieldAlignment.CENTER);*/
        val json= "[{\"name\":\"seq\",\"length\":4,\"alignment\":\"left\"}," +
                "{\"name\":\"name_first\",\"length\":10}," +
                "{\"name\":\"name_last\",\"length\":10}," +
                "{\"name\":\"age\",\"length\":2}," +
                "{\"name\":\"street\",\"length\":20}," +
                "{\"name\":\"city\",\"length\":10}," +
                "{\"name\":\"state\",\"length\":10}," +
                "{\"name\":\"dollar\",\"length\":20}," +
                "{\"name\":\"pick\",\"length\":6}," +
                "{\"name\":\"date\",\"length\":10}" +
                "]\"";

       // String lengths=String.join(",", Arrays.stream(fixedWidthFields.getFieldLengths()).mapToObj(x->String.valueOf(x)).collect(Collectors.toList()));
       // System.out.println(lengths);
        SparkSession sparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate();
        Dataset<Row> input=sparkSession
                .read()
                .format("org.apache.spark.sql.fixedwidth.FixedWidthFileFormat")
                //.option("padding"," ")
                .option("fieldSchema",json)
                .option("header",false)
                .option("ignoreLeadingWhiteSpaceInRead",true)
                .option("ignoreTrailingWhiteSpaceInRead",true)
                .load("src/test/resources/test-data/sample_fixed_width_file.txt");
        input.show();

        input.write()
                .option("header",false)
                .option("fieldSchema",json)
                .option("extension",".DAT")
                .format("org.apache.spark.sql.fixedwidth.FixedWidthFileFormat")
                .mode(SaveMode.Overwrite)
                .save("data/fixed/dat");

    }
}
