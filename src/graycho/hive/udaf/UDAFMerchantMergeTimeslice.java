package graycho.hive.udaf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * 
 * <b>제목</b><br/>
 * graycho.hive.udaf.UDAFMerchantMergeTimeslice - Creation date: 2017. 2. 27. <br/>
 * 
 * @author cho.eungsup
 * 
 *         TimeMerge UDAF 사용 방법 (Hive 버전)
 * 
 *         1. 설명 : TimeMerge UDAF 는 시간(점) 로그를 기대 체류 시간동안 체류 하였으면 체류했다고 판단하여 그 시간을 이어주는 UDAF 이다.
 * 
 *         2. 사용 방법
 * 
 *         a. UDAF 를 JAR 파일로 만든다.
 * 
 *         b. JAR 파일을 Hive DB 에 PUT 하여 밀어 넣는다.
 * 
 *         3. 주의 할점 - order by 는 무조건 enter_time 을 기준으로 한다. - lateral view explode 를 사용해야 보기좋은 형식의 Table 로 나온다.
 *
 */

@Description(name = "UDAFMerchantMergeTimeslice", value = "_FUNC_(expr, expr, expr)")
public class UDAFMerchantMergeTimeslice extends AbstractGenericUDAFResolver {

	ListObjectInspector listOI;
	StringObjectInspector elementOI;

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		// TODO Auto-generated method stub info

		if (parameters.length != 3) {
			throw new UDFArgumentLengthException(UDAFMerchantMergeTimeslice.class.getSimpleName() + " accepts exactly one argument.");
		}

		return new DwellTimeEvaluator();
	}

	@SuppressWarnings("deprecation")
	public static class DwellTimeEvaluator extends GenericUDAFEvaluator {

		ObjectInspector[] inputOIs;
		ObjectInspector[] outputOIs;

		PrimitiveObjectInspector enterTimeOI;
		PrimitiveObjectInspector exitTimeOI;
		PrimitiveObjectInspector expectDwellTimeOI;

		ObjectInspector structOI;

		int aggCollength = 0;

		static class MerchantDwellTime implements AggregationBuffer {
			List<Object[]> objects;
		}

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			// TODO Auto-generated method stub
			super.init(m, parameters);
			System.out.println("## init > m : " + m.toString() + ", param length : " + parameters.length + ", pram type : " + parameters[0].getClass().getName());

			int length = parameters.length;

			if (length > 1 || !(parameters[0] instanceof StandardListObjectInspector)) {
				assert (m == Mode.COMPLETE || m == Mode.FINAL);
				enterTimeOI = (PrimitiveObjectInspector) parameters[0];
				exitTimeOI = (PrimitiveObjectInspector) parameters[1];
				expectDwellTimeOI = (PrimitiveObjectInspector) parameters[2];

				initMapSide(parameters);

			} else {
				assert (m == Mode.PARTIAL1 || m == Mode.PARTIAL2);

				initReduceSide((StandardListObjectInspector) parameters[0]);
			}

			return structOI;
		}

		/* Initialize the UDAF on the map side. */
		private void initMapSide(ObjectInspector[] parameters) throws HiveException {
			System.out.println("## initMapSide");
			StructObjectInspector soi;
			aggCollength = parameters.length;

			try {
				List<String> fieldNames = new ArrayList<String>();
				List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
				fieldNames.add("enterTime");
				fieldNames.add("exitTime");
				fieldNames.add("dwellTime");

				fieldOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorOptions.JAVA));
				fieldOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorOptions.JAVA));
				fieldOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorOptions.JAVA));

				inputOIs = parameters;

				soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
				structOI = ObjectInspectorFactory.getStandardListObjectInspector(soi);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		/* Initialize the UDAF on the reduce side (or the map side in some cases). */
		private void initReduceSide(StandardListObjectInspector inputStructOI) throws HiveException {
			System.out.println("## initReduceSide");
			List<String> fieldNames = new ArrayList<String>(aggCollength);
			List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
			fieldNames.add("enterTime");
			fieldNames.add("exitTime");
			fieldNames.add("dwellTime");

			fieldOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(Text.class, ObjectInspectorOptions.JAVA));
			fieldOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(Text.class, ObjectInspectorOptions.JAVA));
			fieldOIs.add(ObjectInspectorFactory.getReflectionObjectInspector(Text.class, ObjectInspectorOptions.JAVA));

			StructObjectInspector soi;
			soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
			structOI = ObjectInspectorFactory.getStandardListObjectInspector(soi);
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			// TODO Auto-generated method stub
			// System.out.println("## getNewAggregationBuffer");
			MerchantDwellTime result = new MerchantDwellTime();
			return result;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			// TODO Auto-generated method stub
			// System.out.println("## reset");
			// MerchantDwellTime myagg = new MerchantDwellTime();
			MerchantDwellTime myagg = (MerchantDwellTime) agg;
			myagg.objects = null;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			// TODO Auto-generated method stub
			// assert (parameters.length == 1);
			// System.out.println("## iterate > param length : " + parameters.length + ", parameters > " +
			// parameters.getClass().getTypeName());
			// MerchantDwellTime merchantAgg = (MerchantDwellTime) agg;
			merge(agg, parameters);
		}

		// Called when Hive decides to combine one partial aggregation with another
		// key 하나에 담기 위함.

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			// TODO Auto-generated method stub
			// System.out.println("## merge > partial type : " + partial.getClass().getTypeName() + ", partial : " +
			// partial);

			if (partial != null) {
				MerchantDwellTime merchantAgg = (MerchantDwellTime) agg;

				List<Object> objects;
				if (partial instanceof Object[]) {
					objects = Arrays.asList((Object[]) partial);

				} else if (partial instanceof LazyBinaryArray) {
					List<Object> structs = ((LazyBinaryArray) partial).getList();
					merchantAgg.objects = new ArrayList<Object[]>();

					for (int i = 0; i < structs.size(); i++) {
						objects = ((LazyBinaryStruct) structs.get(i)).getFieldsAsList();
						int length = objects.size();
						Object[] objectArray = new Object[length];
						for (int oi = 0; oi < objects.size(); oi++) {
							objectArray[oi] = objects.get(oi);
						}
						merchantAgg.objects.add(objectArray);
					}
					return;

				} else {
					throw new HiveException("Invalid type: " + partial.getClass().getName());
				}

				if (merchantAgg.objects == null) {
					merchantAgg.objects = new ArrayList<Object[]>();
				}

				long enterTime = PrimitiveObjectInspectorUtils.getLong(objects.get(0), enterTimeOI);
				long exitTime = PrimitiveObjectInspectorUtils.getLong(objects.get(1), exitTimeOI);
				int expectDwellTime = PrimitiveObjectInspectorUtils.getInt(objects.get(2), expectDwellTimeOI);

				int length = objects.size();

				Object[] buffer = new Object[length];

				buffer[0] = unixTimeToDateFormat(enterTime);
				buffer[1] = unixTimeToDateFormat(exitTime);
				buffer[2] = expectDwellTime;
				merchantAgg.objects.add(buffer);

			}
		}

		// Called when the final result of the aggregation needed.
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			MerchantDwellTime merchantAgg = (MerchantDwellTime) agg;
			// System.out.println("## terminate > " + merchantAgg.objects);
			List<Object[]> results = new ArrayList<Object[]>();
			List<Object[]> objects = (ArrayList<Object[]>) merchantAgg.objects;

			Collections.sort(objects, new Comparator<Object[]>() {
				@Override
				public int compare(Object[] o1, Object[] o2) {
					// TODO Auto-generated method stub
					long enterTime1 = Long.parseLong(String.valueOf(o1[0]));
					long enterTime2 = Long.parseLong(String.valueOf(o2[0]));
					if (enterTime1 > enterTime2) {
						return 1;
					} else if (enterTime1 < enterTime2) {
						return -1;
					} else {
						return 0;
					}
				}
			});

			for (Object[] object : objects) {

				// System.out.println("## processDataMerge > objects size : "+objects.size()+", results size : " +
				// results.size());
				int length = object.length;

				// 데이터 추출
				long enterTime = Long.parseLong(String.valueOf(object[0]));
				long exitTime = Long.parseLong(String.valueOf(object[1]));
				int expectDwellTime = Integer.parseInt(String.valueOf(object[2]));

				// 비교 대상자
				long compEnterTime = dateFormatToUnixTime(String.valueOf(enterTime));
				long compExitTime = dateFormatToUnixTime(String.valueOf(exitTime));

				if (results.isEmpty()) {
					Object[] buffer = new Object[length];
					buffer[0] = new Text(String.valueOf(enterTime));
					buffer[1] = new Text(String.valueOf(exitTime));
					buffer[2] = new Text(String.valueOf((int) ((compExitTime - compEnterTime) / 60)));
					results.add(buffer);

					continue;
				}

				// 비교 Base
				long baseEnterTime = dateFormatToUnixTime(String.valueOf(results.get(results.size() - 1)[0]));
				long baseExitTime = dateFormatToUnixTime(String.valueOf(results.get(results.size() - 1)[1]));

				boolean isIncreasing = false;

				if (baseExitTime >= compEnterTime) {
					// preAgg 의 TimeLine 에 postAgg 의 Enter Time 범위 일 경우,
					if (baseExitTime < compExitTime) {
						baseExitTime = compExitTime;
					}

				} else {
					baseExitTime = baseExitTime + (expectDwellTime * 60);

					// post EnterTime 이 pre ExitTime 이전일 경우
					if (baseExitTime >= compEnterTime) {
						baseExitTime = compExitTime;
					} else {
						// 두개의 Time slice 가 아얘 떨어져 있는 경우..
						isIncreasing = true;
					}
				}

				if (isIncreasing) {
					Object[] buffer = new Object[length];
					buffer[0] = new Text(unixTimeToDateFormat(compEnterTime));
					buffer[1] = new Text(unixTimeToDateFormat(compExitTime));
					buffer[2] = new Text(String.valueOf((int) ((compExitTime - compEnterTime) / 60)));

					results.add(buffer);
				} else {
					// enterTime
					results.get(results.size() - 1)[0] = new Text(unixTimeToDateFormat(baseEnterTime));
					// exitTime
					results.get(results.size() - 1)[1] = new Text(unixTimeToDateFormat(baseExitTime));
					// 기대 체류 시간
					results.get(results.size() - 1)[2] = new Text(String.valueOf((int) ((baseExitTime - baseEnterTime) / 60)));
				}

			}

			return results;
		}

		// Called when Hive wants partially aggregated results.
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			// System.out.println("## terminatePartial > agg type : " + agg.getClass().getTypeName());
			MerchantDwellTime merchantAgg = (MerchantDwellTime) agg;

			return merchantAgg.objects;
		}

		private String unixTimeToDateFormat(long time) {
			Date date = new Date(time * 1000L);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss", Locale.KOREA);
			String formattedDate = sdf.format(date);

			return formattedDate;
		}

		private long dateFormatToUnixTime(String time) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss", Locale.KOREA);
			long unixTime = 0;
			try {
				unixTime = sdf.parse(time).getTime() / 1000;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return unixTime;

		}

	}

}
