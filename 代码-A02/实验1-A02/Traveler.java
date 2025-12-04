import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Traveler implements WritableComparable {
	public String review_id, review_date, temperature, user_id, user_birthday, user_nationality, user_career;
	public Double longitude, latitude, altitude, rating, user_income;
	public Traveler(){
	}
	
	public Traveler(String src){
		String[] tuples = src.split("\\|");
		review_id = tuples[0];
		longitude = Double.valueOf(tuples[1]);
		latitude = Double.valueOf(tuples[2]);
		altitude = Double.valueOf(tuples[3]);
		review_date = Convert.convertDate(tuples[4]);
		temperature = Convert.convertTemperature(tuples[5]);
		try{//使用异常处理的方式将空值先填成默认值
			rating = Double.valueOf(tuples[6]);
		} catch (Exception e){
			rating = -1.0;
		}
		user_id = tuples[7];;
		user_birthday = Convert.convertDate(tuples[8]);
		user_nationality = tuples[9].toLowerCase();;
		user_career = tuples[10].toLowerCase();
		try{//使用异常处理的方式将空值先填成默认值
			user_income = Double.valueOf(tuples[11]);
		} catch (Exception e){
			user_income = -1.0;
		}
	}

	@Override
	public Traveler clone(){
		Traveler re = new Traveler();
		re.review_id = this.review_id;
		re.longitude = this.longitude;
		re.latitude = this.latitude;
		re.altitude = this.altitude;
		re.review_date = this.review_date;
		re.temperature = this.temperature;
		re.rating = this.rating;
		re.user_id = this.user_id;
		re.user_birthday = this.user_birthday;
		re.user_nationality = this.user_nationality;
		re.user_career = this.user_career;
		re.user_income = this.user_income;
		return re;
	}

	@Override
	public String toString() {
		return review_id + "|" +
				String.format("%.6f", longitude) + "|" +
				String.format("%.6f", latitude) + "|" +
				String.format("%.6f", altitude) + "|" +
				review_date + "|" +
				temperature + "|" +
				String.format("%.2f", rating) + "|" +
				user_id + "|" +
				user_birthday + "|" +
				user_nationality + "|" +
				user_career + "|" +
				String.format("%.0f", user_income);
	}
	
	@Override
		public void write(DataOutput dataOutput) throws IOException {
			dataOutput.writeUTF(review_id);
			dataOutput.writeDouble(longitude);
			dataOutput.writeDouble(latitude);
			dataOutput.writeDouble(altitude);
			dataOutput.writeUTF(review_date);
			dataOutput.writeUTF(temperature);
			dataOutput.writeDouble(rating);
			dataOutput.writeUTF(user_id);
			dataOutput.writeUTF(user_birthday);
			dataOutput.writeUTF(user_nationality);
			dataOutput.writeUTF(user_career);
			dataOutput.writeDouble(user_income);
		}
		

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.review_id = dataInput.readUTF();
		this.longitude = dataInput.readDouble();
		this.latitude = dataInput.readDouble();
		this.altitude = dataInput.readDouble();
		this.review_date = dataInput.readUTF();
		this.temperature = dataInput.readUTF();
		this.rating = dataInput.readDouble();
		this.user_id = dataInput.readUTF();
		this.user_birthday = dataInput.readUTF();
		this.user_nationality = dataInput.readUTF();
		this.user_career = dataInput.readUTF();
		this.user_income = dataInput.readDouble();
	}
	
	

	

	@Override
	public int compareTo(Object o) {
		return this.hashCode() < o.hashCode() ? 1 : 0;
	}
}
