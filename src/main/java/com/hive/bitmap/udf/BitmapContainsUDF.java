// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.hive.bitmap.udf;

import com.hive.bitmap.common.BitmapUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.util.Iterator;

@Description(name = "bitmap_contains")
public class BitmapContainsUDF extends GenericUDF {

    private transient BinaryObjectInspector inputOI01;
    private transient PrimitiveObjectInspector inputOI02;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 2) {
            throw new UDFArgumentTypeException(arguments.length, "Exactly two argument is expected.");
        }

        ObjectInspector input0 = arguments[0];
        ObjectInspector input1 = arguments[1];
        if (!(input0 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("first argument must be a binary");
        }

        if (!(input1 instanceof IntObjectInspector || input1 instanceof LongObjectInspector || input1 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("second argument must be a int or bigint or bitmap");
        }

        this.inputOI01 = (BinaryObjectInspector) input0;
        this.inputOI02 = (PrimitiveObjectInspector) input1;

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[]  args) throws HiveException {
        Roaring64Bitmap bitmapValue = getBitmapFromBytes(args[0]);

        if (this.inputOI02 instanceof BinaryObjectInspector) {
            Roaring64Bitmap bitmap2 = getBitmapFromBytes(args[1]);
            return checkBitmapContains(bitmapValue, bitmap2);
        } else {
            long number = PrimitiveObjectInspectorUtils.getLong(args[1].get(), inputOI02);
            return bitmapValue.contains(number);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: bitmap_contains(bitmap,num)";
    }

    private boolean checkBitmapContains(Roaring64Bitmap bitmap1, Roaring64Bitmap bitmap2) {

        if (bitmap2.isEmpty()) {
            return false;
        }
        Iterator<Long> iterator = bitmap2.iterator();
        while (iterator.hasNext()) {
            if (!bitmap1.contains(iterator.next())) {
                return false;
            }
        }
        return true;
    }

    private Roaring64Bitmap getBitmapFromBytes(DeferredObject arg) throws HiveException {
        byte[] inputBytes = this.inputOI01.getPrimitiveJavaObject(arg.get());
        try {
            return BitmapUtil.deserializeToBitmap(inputBytes);
        } catch (IOException ioException) {
            throw new HiveException(ioException);
        }
    }
}
