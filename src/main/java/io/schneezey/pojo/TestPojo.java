package io.schneezey.pojo;

//import java.text.SimpleDateFormat;
//import java.util.Date;

//import org.apache.avro.reflect.DateAsLongEncoding;
import org.apache.avro.reflect.Nullable;

/*
 * Notes:
 * byte[] not supported by simple Spring Mapper.  Avro requires a conversion to ByteBuffer.  
 * 		This could be accounted for in a one-off condition check in the Spring Mapper.  
 * 		But I've solved this in my custom mapper using a new Avro annotation.
 * 
 * enum not supported by simple Spring Mapper.  
 * 		The error appears to be a string to enum conversion problem.  
 * 		But I've solved this in my custom mapper using java reflection.
 * 
 * java.util.Date not supported by simple Spring Mapper.
 * 		The error is a date to long conversion issue.  The schema identifies the encoding annotation correctly.  but the mapper does not.
 * 		But I've solved this in my custom mapper using java reflection
 */
public class TestPojo {
	
	private Integer id = 0;

	// Primitives
	private float testPFloat = (float) 0.0;
	private double testPDouble = 0.0;
	private int testPInt = 2000;
	private long testPLong = 3000L;
	private boolean testPBoolean = true;	
	//private byte[] testPBytes = new byte[0];

	@Nullable private Float testNFloat = null;
	@Nullable private Double testNDouble = null;
	@Nullable private Integer testNInt = null;
	@Nullable private Long testNLong = null;
	@Nullable private Boolean testNBoolean = null;	
	//@Nullable private byte[] testNBytes = null;

	@Nullable private Float testN1Float = new Float(1.0);
	@Nullable private Double testN1Double = new Double(2.0);
	@Nullable private Integer testN1Int = new Integer(3);
	@Nullable private Long testN1Long = new Long(50000L);
	@Nullable private Boolean testN1Boolean = new Boolean(false);	
	//@Nullable private byte[] testN1Bytes = new byte[5];
/*	
	@AvroEncode(using=DateAsLongEncoding.class) 	
	private Date testDate = new Date();

	@Nullable
	@AvroEncode(using=DateAsLongEncoding.class) 	
	private Date testNDate = null;
	
	@Nullable
	@AvroEncode(using=DateAsLongEncoding.class) 	
	private Date testN1Date = new Date();

	public enum TEST_ENUM { TYPEA, TYPEB, TYPEC };
	private TEST_ENUM testenum = TEST_ENUM.TYPEA;
*/	
	public String toString() {
		//SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		StringBuffer buffer = new StringBuffer();
		buffer.append("id: ").append(id).append("\n")
		
		.append("testFloat: ").append(testPFloat).append("\n")
		.append("testDouble: ").append(testPDouble).append("\n")
		.append("testInt: ").append(testPInt).append("\n")
		.append("testLong: ").append(testPLong).append("\n")
		.append("testBoolean: ").append(testPBoolean).append("\n")
		//.append("testBytes: ").append(testBytes==null ? "null" : testBytes).append("\n")

		.append("testNFloat: ").append(testNFloat==null ? "null" : testNFloat ).append("\n")
		.append("testNDouble: ").append(testNDouble==null ? "null" : testNDouble).append("\n")
		.append("testNInt: ").append(testNInt==null ? "null" : testNInt).append("\n")
		.append("testNLong: ").append(testNLong==null ? "null" : testNLong).append("\n")
		.append("testNBoolean: ").append(testNBoolean==null ? "null" : testNBoolean).append("\n")
		//.append("testNBytes: ").append(testNBytes==null ? "null" : testNBytes).append("\n")
		
		.append("testN1Float: ").append(testN1Float==null ? "null" : testN1Float ).append("\n")
		.append("testN1Double: ").append(testN1Double==null ? "null" : testN1Double).append("\n")
		.append("testN1Int: ").append(testN1Int==null ? "null" : testN1Int).append("\n")
		.append("testN1Long: ").append(testN1Long==null ? "null" : testN1Long).append("\n")
		.append("testN1Boolean: ").append(testN1Boolean==null ? "null" : testN1Boolean).append("\n")
		//.append("testN1Bytes: ").append(testN1Bytes==null ? "null" : testN1Bytes).append("\n")
		
		//.append("testDate: ").append(testDate==null ? "null" : df.format(testDate)).append("\n")
		//.append("testNDate: ").append(testNDate==null ? "null" : df.format(testNDate)).append("\n")
		//.append("testN1Date: ").append(testN1Date==null ? "null" : df.format(testN1Date)).append("\n")

		//.append("testenum").append(testenum.toString()).append("\n")

		;
		return buffer.toString();
	}
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}


	public float getTestFloat() {
		return testPFloat;
	}

	public void setTestFloat(float testFloat) {
		this.testPFloat = testFloat;
	}

	public double getTestDouble() {
		return testPDouble;
	}

	public void setTestDouble(double testDouble) {
		this.testPDouble = testDouble;
	}

	public int getTestInt() {
		return testPInt;
	}

	public void setTestInt(int testInt) {
		this.testPInt = testInt;
	}

	public long getTestLong() {
		return testPLong;
	}

	public void setTestLong(long testLong) {
		this.testPLong = testLong;
	}

	public boolean isTestBoolean() {
		return testPBoolean;
	}

	public void setTestBoolean(boolean testBoolean) {
		this.testPBoolean = testBoolean;
	}
/*
	public byte[] getTestBytes() {
		return testPBytes;
	}

	public void setTestBytes(byte[] testBytes) {
		this.testPBytes = testBytes;
	}
*/
	public Float getTestNFloat() {
		return testNFloat;
	}

	public void setTestNFloat(Float testNFloat) {
		this.testNFloat = testNFloat;
	}

	public Double getTestNDouble() {
		return testNDouble;
	}

	public void setTestNDouble(Double testNDouble) {
		this.testNDouble = testNDouble;
	}

	public Integer getTestNInt() {
		return testNInt;
	}

	public void setTestNInt(Integer testNInt) {
		this.testNInt = testNInt;
	}

	public Long getTestNLong() {
		return testNLong;
	}

	public void setTestNLong(Long testNLong) {
		this.testNLong = testNLong;
	}

	public Long getTestN1Long() {
		return testN1Long;
	}

	public void setTestN1Long(Long testN1Long) {
		this.testN1Long = testN1Long;
	}

	public Boolean getTestNBoolean() {
		return testNBoolean;
	}

	public void setTestNBoolean(Boolean testNBoolean) {
		this.testNBoolean = testNBoolean;
	}
/*
	public byte[] getTestNBytes() {
		return testNBytes;
	}

	public void setTestNBytes(byte[] testNBytes) {
		this.testNBytes = testNBytes;
	}
*/
	public Float getTestN1Float() {
		return testN1Float;
	}

	public void setTestN1Float(Float testN1Float) {
		this.testN1Float = testN1Float;
	}

	public Double getTestN1Double() {
		return testN1Double;
	}

	public void setTestN1Double(Double testN1Double) {
		this.testN1Double = testN1Double;
	}

	public Integer getTestN1Int() {
		return testN1Int;
	}

	public void setTestN1Int(Integer testN1Int) {
		this.testN1Int = testN1Int;
	}

	public Boolean getTestN1Boolean() {
		return testN1Boolean;
	}

	public void setTestN1Boolean(Boolean testN1Boolean) {
		this.testN1Boolean = testN1Boolean;
	}
/*
	public byte[] getTestN1Bytes() {
		return testN1Bytes;
	}

	public void setTestN1Bytes(byte[] testN1Bytes) {
		this.testN1Bytes = testN1Bytes;
	}

	public Date getTestDate() {
		return testDate;
	}

	public void setTestDate(Date testDate) {
		this.testDate = testDate;
	}

	public Date getTestNDate() {
		return testNDate;
	}

	public void setTestNDate(Date testNDate) {
		this.testNDate = testNDate;
	}

	public Date getTestN1Date() {
		return testN1Date;
	}

	public void setTestN1Date(Date testN1Date) {
		this.testN1Date = testN1Date;
	}


	public TEST_ENUM getTestenum() {
		return testenum;
	}

	public void setTestenum(TEST_ENUM testenum) {
		this.testenum = testenum;
	}
*/	
}