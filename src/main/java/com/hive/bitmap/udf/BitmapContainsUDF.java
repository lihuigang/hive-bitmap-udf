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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Description(name = "bitmap_to_array")
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

        if (!(input1 instanceof IntObjectInspector || input1 instanceof LongObjectInspector)) {
            throw new UDFArgumentException("second argument must be a int or bigint");
        }

        this.inputOI01 = (BinaryObjectInspector) input0;
        this.inputOI02 = (PrimitiveObjectInspector) input1;

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[]  args) throws HiveException {

        byte[] inputBytes = this.inputOI01.getPrimitiveJavaObject(args[0].get());
        long number = PrimitiveObjectInspectorUtils.getLong(args[1].get(), inputOI02);
        try {
            Roaring64Bitmap bitmapValue = BitmapUtil.deserializeToBitmap(inputBytes);
            return bitmapValue.contains(number);
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new HiveException(ioException);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: bitmap_contains(bitmap,num)";
    }
}
