package graycho.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;


public class PX_DistanceLatLng extends UDF {	
	
	private double R = 6378.137;                  //Earth radius in km (WGS84)
	
	public double evaluate(double lat1, double lon1, double lat2, double lon2) {

		  double dLat  = deg2rad( lat2 - lat1 );
		  double dLong = deg2rad( lon2 - lon1 );

		  double a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLong/2) * Math.sin(dLong/2);
		  
		  double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		  double d = R * c;
		  
		  return Math.round(d * 1000);
	}
	
	private double deg2rad(double deg){
		return deg * (Math.PI/180);
	}
}
