package com.hive.bitmap.udf;

import com.hive.bitmap.common.BitmapUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;

@Description(name = "bitmap_intersect", value = "_FUNC_(expr) - Calculate the grouped bitmap intersection , Returns an doris bitmap representation of a column.")
public class BitmapIntersectUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] args) throws SemanticException {
        if (args.length != 1) {
            throw new UDFArgumentException(String.format("Exactly one argument is expected, but get %d", args.length));
        }
        return new IntersectEvaluator();
    }

    public static class IntersectEvaluator extends GenericUDAFEvaluator {

        private transient BinaryObjectInspector binaryOI;

        @AggregationType(estimable = true)
        static class BitmapAgg extends AbstractAggregationBuffer {
            Roaring64Bitmap bitmap;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            this.binaryOI = (BinaryObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            BitmapAgg result = new BitmapAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((BitmapAgg) agg).bitmap = new Roaring64Bitmap();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            BitmapAgg tmpAgg = (BitmapAgg) agg;
            try {
                return BitmapUtil.serializeToBytes(tmpAgg.bitmap);
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            BitmapAgg tmpAgg = (BitmapAgg) agg;
            byte[] partialResult = this.binaryOI.getPrimitiveJavaObject(partial);
            try {
                if (tmpAgg.bitmap.isEmpty()) {
                    tmpAgg.bitmap.or(BitmapUtil.deserializeToBitmap(partialResult));
                } else {
                    tmpAgg.bitmap.and(BitmapUtil.deserializeToBitmap(partialResult));
                }
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }
    }
}
