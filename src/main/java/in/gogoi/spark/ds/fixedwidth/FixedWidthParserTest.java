package in.gogoi.spark.ds.fixedwidth;

import com.univocity.parsers.fixed.FieldAlignment;
import com.univocity.parsers.fixed.FixedWidthFields;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.stream.Collectors;

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
        fixedWidthFields.addField("float", 30, FieldAlignment.RIGHT, '0');
        fixedWidthFields.addField("credt_card", 30, FieldAlignment.RIGHT, '0');
        fixedWidthFields.addField("natural", 30, FieldAlignment.RIGHT, '0');

        String lengths=String.join(",", Arrays.stream(fixedWidthFields.getFieldLengths()).mapToObj(x->String.valueOf(x)).collect(Collectors.toList()));
        System.out.println(lengths);
        SparkSession sparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate();
        Dataset<Row> input=sparkSession
                .read()
                .format("org.apache.spark.sql.fixedwidth.FixedWidthFileFormat")
                //.option("padding"," ")
                .option("fieldLengths",lengths)
                .option("header",true)
                .option("ignoreLeadingWhiteSpaceInRead",true)
                .option("ignoreTrailingWhiteSpaceInRead",true)
                .load("data/input/fixed_width.dat");
        input.show();

        input.write()
                .option("header",true)
                .option("fieldLengths",lengths)
                .option("extension",".DAT")
                .format("in.gogoi.ds.fixedwidth.FixedWidthFileFormat")
                .mode(SaveMode.Overwrite)
                .save("data/fixed/dat");

    }
}
