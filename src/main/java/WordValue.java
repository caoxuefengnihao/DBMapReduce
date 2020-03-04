import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WordValue implements Writable, DBWritable {

    String word = "";
    String value = "";

    public WordValue() {
    }

    public WordValue(String word, String value) {
        this.word = word;
        this.value = value;
    }

    public String getWord() {
        return word;
    }

    public String getValue() {
        return value;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "WordValue{" +
                "word='" + word + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeUTF(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       word = dataInput.readUTF();
       value = dataInput.readUTF();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,word);
        preparedStatement.setString(2,value);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        word = resultSet.getString(1);
        value = resultSet.getString(2);
    }
}
