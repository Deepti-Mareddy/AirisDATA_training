// ORM class for table 'Rental_data'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Mon Jul 18 10:10:12 IST 2022
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Rental_data extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public Rental_data with_id(Integer id) {
    this.id = id;
    return this;
  }
  private String property_type;
  public String get_property_type() {
    return property_type;
  }
  public void set_property_type(String property_type) {
    this.property_type = property_type;
  }
  public Rental_data with_property_type(String property_type) {
    this.property_type = property_type;
    return this;
  }
  private String room_type;
  public String get_room_type() {
    return room_type;
  }
  public void set_room_type(String room_type) {
    this.room_type = room_type;
  }
  public Rental_data with_room_type(String room_type) {
    this.room_type = room_type;
    return this;
  }
  private Float bathrooms;
  public Float get_bathrooms() {
    return bathrooms;
  }
  public void set_bathrooms(Float bathrooms) {
    this.bathrooms = bathrooms;
  }
  public Rental_data with_bathrooms(Float bathrooms) {
    this.bathrooms = bathrooms;
    return this;
  }
  private Integer bedrooms;
  public Integer get_bedrooms() {
    return bedrooms;
  }
  public void set_bedrooms(Integer bedrooms) {
    this.bedrooms = bedrooms;
  }
  public Rental_data with_bedrooms(Integer bedrooms) {
    this.bedrooms = bedrooms;
    return this;
  }
  private Integer minimum_nights;
  public Integer get_minimum_nights() {
    return minimum_nights;
  }
  public void set_minimum_nights(Integer minimum_nights) {
    this.minimum_nights = minimum_nights;
  }
  public Rental_data with_minimum_nights(Integer minimum_nights) {
    this.minimum_nights = minimum_nights;
    return this;
  }
  private Integer location_id;
  public Integer get_location_id() {
    return location_id;
  }
  public void set_location_id(Integer location_id) {
    this.location_id = location_id;
  }
  public Rental_data with_location_id(Integer location_id) {
    this.location_id = location_id;
    return this;
  }
  private Integer price;
  public Integer get_price() {
    return price;
  }
  public void set_price(Integer price) {
    this.price = price;
  }
  public Rental_data with_price(Integer price) {
    this.price = price;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Rental_data)) {
      return false;
    }
    Rental_data that = (Rental_data) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.property_type == null ? that.property_type == null : this.property_type.equals(that.property_type));
    equal = equal && (this.room_type == null ? that.room_type == null : this.room_type.equals(that.room_type));
    equal = equal && (this.bathrooms == null ? that.bathrooms == null : this.bathrooms.equals(that.bathrooms));
    equal = equal && (this.bedrooms == null ? that.bedrooms == null : this.bedrooms.equals(that.bedrooms));
    equal = equal && (this.minimum_nights == null ? that.minimum_nights == null : this.minimum_nights.equals(that.minimum_nights));
    equal = equal && (this.location_id == null ? that.location_id == null : this.location_id.equals(that.location_id));
    equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Rental_data)) {
      return false;
    }
    Rental_data that = (Rental_data) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.property_type == null ? that.property_type == null : this.property_type.equals(that.property_type));
    equal = equal && (this.room_type == null ? that.room_type == null : this.room_type.equals(that.room_type));
    equal = equal && (this.bathrooms == null ? that.bathrooms == null : this.bathrooms.equals(that.bathrooms));
    equal = equal && (this.bedrooms == null ? that.bedrooms == null : this.bedrooms.equals(that.bedrooms));
    equal = equal && (this.minimum_nights == null ? that.minimum_nights == null : this.minimum_nights.equals(that.minimum_nights));
    equal = equal && (this.location_id == null ? that.location_id == null : this.location_id.equals(that.location_id));
    equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.property_type = JdbcWritableBridge.readString(2, __dbResults);
    this.room_type = JdbcWritableBridge.readString(3, __dbResults);
    this.bathrooms = JdbcWritableBridge.readFloat(4, __dbResults);
    this.bedrooms = JdbcWritableBridge.readInteger(5, __dbResults);
    this.minimum_nights = JdbcWritableBridge.readInteger(6, __dbResults);
    this.location_id = JdbcWritableBridge.readInteger(7, __dbResults);
    this.price = JdbcWritableBridge.readInteger(8, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.property_type = JdbcWritableBridge.readString(2, __dbResults);
    this.room_type = JdbcWritableBridge.readString(3, __dbResults);
    this.bathrooms = JdbcWritableBridge.readFloat(4, __dbResults);
    this.bedrooms = JdbcWritableBridge.readInteger(5, __dbResults);
    this.minimum_nights = JdbcWritableBridge.readInteger(6, __dbResults);
    this.location_id = JdbcWritableBridge.readInteger(7, __dbResults);
    this.price = JdbcWritableBridge.readInteger(8, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(property_type, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(room_type, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeFloat(bathrooms, 4 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(bedrooms, 5 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(minimum_nights, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(location_id, 7 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(price, 8 + __off, 4, __dbStmt);
    return 8;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(property_type, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(room_type, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeFloat(bathrooms, 4 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(bedrooms, 5 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(minimum_nights, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(location_id, 7 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(price, 8 + __off, 4, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.property_type = null;
    } else {
    this.property_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.room_type = null;
    } else {
    this.room_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.bathrooms = null;
    } else {
    this.bathrooms = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.bedrooms = null;
    } else {
    this.bedrooms = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.minimum_nights = null;
    } else {
    this.minimum_nights = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.location_id = null;
    } else {
    this.location_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.price = null;
    } else {
    this.price = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.property_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, property_type);
    }
    if (null == this.room_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, room_type);
    }
    if (null == this.bathrooms) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.bathrooms);
    }
    if (null == this.bedrooms) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.bedrooms);
    }
    if (null == this.minimum_nights) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.minimum_nights);
    }
    if (null == this.location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.location_id);
    }
    if (null == this.price) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.price);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.property_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, property_type);
    }
    if (null == this.room_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, room_type);
    }
    if (null == this.bathrooms) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.bathrooms);
    }
    if (null == this.bedrooms) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.bedrooms);
    }
    if (null == this.minimum_nights) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.minimum_nights);
    }
    if (null == this.location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.location_id);
    }
    if (null == this.price) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.price);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(property_type==null?"null":property_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(room_type==null?"null":room_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bathrooms==null?"null":"" + bathrooms, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bedrooms==null?"null":"" + bedrooms, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(minimum_nights==null?"null":"" + minimum_nights, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location_id==null?"null":"" + location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(price==null?"null":"" + price, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(property_type==null?"null":property_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(room_type==null?"null":room_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bathrooms==null?"null":"" + bathrooms, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bedrooms==null?"null":"" + bedrooms, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(minimum_nights==null?"null":"" + minimum_nights, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(location_id==null?"null":"" + location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(price==null?"null":"" + price, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.property_type = null; } else {
      this.property_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.room_type = null; } else {
      this.room_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bathrooms = null; } else {
      this.bathrooms = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bedrooms = null; } else {
      this.bedrooms = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.minimum_nights = null; } else {
      this.minimum_nights = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.location_id = null; } else {
      this.location_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.price = null; } else {
      this.price = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.property_type = null; } else {
      this.property_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.room_type = null; } else {
      this.room_type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bathrooms = null; } else {
      this.bathrooms = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bedrooms = null; } else {
      this.bedrooms = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.minimum_nights = null; } else {
      this.minimum_nights = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.location_id = null; } else {
      this.location_id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.price = null; } else {
      this.price = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    Rental_data o = (Rental_data) super.clone();
    return o;
  }

  public void clone0(Rental_data o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("property_type", this.property_type);
    __sqoop$field_map.put("room_type", this.room_type);
    __sqoop$field_map.put("bathrooms", this.bathrooms);
    __sqoop$field_map.put("bedrooms", this.bedrooms);
    __sqoop$field_map.put("minimum_nights", this.minimum_nights);
    __sqoop$field_map.put("location_id", this.location_id);
    __sqoop$field_map.put("price", this.price);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("property_type", this.property_type);
    __sqoop$field_map.put("room_type", this.room_type);
    __sqoop$field_map.put("bathrooms", this.bathrooms);
    __sqoop$field_map.put("bedrooms", this.bedrooms);
    __sqoop$field_map.put("minimum_nights", this.minimum_nights);
    __sqoop$field_map.put("location_id", this.location_id);
    __sqoop$field_map.put("price", this.price);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
    }
    else    if ("property_type".equals(__fieldName)) {
      this.property_type = (String) __fieldVal;
    }
    else    if ("room_type".equals(__fieldName)) {
      this.room_type = (String) __fieldVal;
    }
    else    if ("bathrooms".equals(__fieldName)) {
      this.bathrooms = (Float) __fieldVal;
    }
    else    if ("bedrooms".equals(__fieldName)) {
      this.bedrooms = (Integer) __fieldVal;
    }
    else    if ("minimum_nights".equals(__fieldName)) {
      this.minimum_nights = (Integer) __fieldVal;
    }
    else    if ("location_id".equals(__fieldName)) {
      this.location_id = (Integer) __fieldVal;
    }
    else    if ("price".equals(__fieldName)) {
      this.price = (Integer) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
      return true;
    }
    else    if ("property_type".equals(__fieldName)) {
      this.property_type = (String) __fieldVal;
      return true;
    }
    else    if ("room_type".equals(__fieldName)) {
      this.room_type = (String) __fieldVal;
      return true;
    }
    else    if ("bathrooms".equals(__fieldName)) {
      this.bathrooms = (Float) __fieldVal;
      return true;
    }
    else    if ("bedrooms".equals(__fieldName)) {
      this.bedrooms = (Integer) __fieldVal;
      return true;
    }
    else    if ("minimum_nights".equals(__fieldName)) {
      this.minimum_nights = (Integer) __fieldVal;
      return true;
    }
    else    if ("location_id".equals(__fieldName)) {
      this.location_id = (Integer) __fieldVal;
      return true;
    }
    else    if ("price".equals(__fieldName)) {
      this.price = (Integer) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
