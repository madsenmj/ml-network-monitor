package storm;

import java.io.Serializable;

import org.joda.time.DateTime;
import org.joda.time.Seconds;

public class NDTData implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5531885500361167502L;
	private String _daytime;
	private Float _avgrtt;
	private Integer _ninsub;
	private Float _avginsub;
	private Float _sigmainsub;
	private Float _datafreq;
	private Integer _outlier;
	private String _dataday;
	private Float epsilon = 0.75f;
	
	//Constructors
	public NDTData(String daytime, Integer ninsub, Float avgrtt, Float avginsub, Float sigmainsub, Float datafreq){
		this._daytime = daytime;
		this._avgrtt = avgrtt;
		this._ninsub = ninsub;
		this._avginsub = avginsub;
		this._sigmainsub = sigmainsub;
		this._datafreq = datafreq;
		this._outlier = 0;

		DateTime dt = new DateTime(this._daytime);
		this._dataday = dt.toString("YYYYMMdd");
	}
	
	public NDTData(String daytime, Float avgrtt, NDTData olddata){
		this._daytime = daytime;
		this._avgrtt = avgrtt;
		this._ninsub = olddata.getNinsub() + 1;
		this._avginsub = (olddata.getAvginsub() * olddata.getNinsub() + this._avgrtt)/this._ninsub;
		this._sigmainsub = (float) Math.sqrt((( olddata.getSigmainsub()*olddata.getSigmainsub() + olddata.getAvginsub()*olddata.getAvginsub() )
							*olddata.getNinsub() + this._avgrtt*this._avgrtt)/this._ninsub - this._avginsub*this._avginsub);
		this._outlier = 0;
		if (this._ninsub > 2 
				&& Math.abs(this._avgrtt - this._avginsub)/this._sigmainsub > epsilon 
				&& this._avgrtt > this._avginsub){
			this._outlier = 1;
		}
		
		DateTime dt1 = new DateTime(olddata.getDay());
		DateTime dt2 = new DateTime(this._daytime);
		Integer dt = Seconds.secondsBetween(dt1, dt2).getSeconds();
		this._datafreq = (olddata.getDatafreq() + 1f/dt * 86400)/2f;

		this._dataday = dt2.toString("YYYYMMdd");
	}
	
	//Functions to get variables
	public String getDay(){
		return this._daytime;
	}
	public Integer getNinsub(){
		return this._ninsub;
	}
	public Float getAvginsub(){
		return this._avginsub;
	}
	public Float getSigmainsub(){
		return this._sigmainsub;
	}
	public Float getDatafreq(){
		return this._datafreq;
	}
	public Integer getOutlier(){
		return this._outlier;
	}
	
	public String toString(){
		String output = "";
		output += this._daytime + "," + this._avgrtt.toString() + ","
				+ this._ninsub.toString() + "," + this._avginsub.toString() + ","
				+ this._sigmainsub.toString() + "," + this._datafreq.toString() + ","
				+ this._outlier.toString();
		return output;
	}
	
	/*
	"ninsub",
	"avginsub",
	"sigmainsub",
	"datafreq",
	"outlier",
	"dataday",
	*/
	public String toFields(){
		String output = "";
		output += this._ninsub.toString() + "," + this._avginsub.toString() + ","
				+ this._sigmainsub.toString() + "," + this._datafreq.toString() + ","
				+ this._outlier.toString() + "," + this._dataday;
		return output;
	}
	
}
