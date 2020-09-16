package in.gogoi.spark.ds.fixedwidth;

import com.univocity.parsers.fixed.FieldAlignment;
import com.univocity.parsers.fixed.FixedWidthFields;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.stream.Collectors;
/**
 * @author lieuranjan
 * Created on 12/9/20
 */
public class FixedWidthParserTest {
    public static void main(String[] args) {
        FixedWidthFields fixedWidthFields = new FixedWidthFields();
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
        fixedWidthFields.addField("date", 10, FieldAlignment.CENTER);

        String lengths=String.join(",", Arrays.stream(fixedWidthFields.getFieldLengths()).mapToObj(x->String.valueOf(x)).collect(Collectors.toList()));
        System.out.println(lengths);
        SparkSession sparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate();
        Dataset<Row> input=sparkSession
                .read()
                .format("org.apache.spark.sql.fixedwidth.FixedWidthFileFormat")
                //.option("padding"," ")
                .option("fieldLengths",lengths)
                .option("header",false)
                .option("multiline",false)
                .option("ignoreLeadingWhiteSpaceInRead",true)
                .option("ignoreTrailingWhiteSpaceInRead",true)
                .load("src/test/resources/test-data/sample_fixed_width_file.txt");
        input.show();

        input.write()
                .option("header",false)
                .option("fieldLengths",lengths)
                .option("extension",".DAT")
                .format("org.apache.spark.sql.fixedwidth.FixedWidthFileFormat")
                .mode(SaveMode.Overwrite)
                .save("data/fixed/dat");

    }
}
