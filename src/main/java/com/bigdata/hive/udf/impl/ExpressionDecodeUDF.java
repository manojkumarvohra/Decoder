package com.bigdata.hive.udf.impl;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

@Description(name = "decode_expr", value = "_FUNC_(expr, val1, output1, .... , defaultOutput) "
		+ "- Returns output1 if expr=val1 otherwise defaultOutput", extended = "Example:\n"
				+ " > SELECT _FUNC_(chc_id, null, -99, 1) LIMIT 1;\n -99 if chc_id Is null Otherwise 1")
public class ExpressionDecodeUDF extends GenericUDF {

	private ObjectInspector[] argumentObjInspectors;
	private GenericUDFUtils.ReturnObjectInspectorResolver outputResolver;
	private GenericUDFUtils.ReturnObjectInspectorResolver typeResolver;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length < 3) {
			throw new UDFArgumentLengthException(
					"The function decode_expr(expr, val1, output1, .... , defaultOutput) requires "
							+ "at least three arguments.");
		}

		argumentObjInspectors = arguments;
		typeResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		outputResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		typeResolver.update(arguments[0]);
		for (int i = 1; i + 1 < arguments.length; i += 2) {
			// verify type of val arguments against expression
			if (!typeResolver.update(arguments[i])) {
				throw new UDFArgumentTypeException(i,
						"The value of 'val'" + (i + 1) + " should have the same type: \""
								+ typeResolver.get().getTypeName() + "\" is expected but \""
								+ arguments[i].getTypeName() + "\" is found");
			}
			// verify the types of output arguments against each other
			if (!outputResolver.update(arguments[i + 1])) {
				throw new UDFArgumentTypeException(i + 1,
						"The value of output " + (i + 1) + " should have the same type: \""
								+ outputResolver.get().getTypeName() + "\" is expected but \""
								+ arguments[i + 1].getTypeName() + "\" is found");
			}
		}
		// verify the types of default output arguments against other outputs
		if (!outputResolver.update(arguments[arguments.length - 1])) {
			throw new UDFArgumentTypeException(arguments.length - 1,
					"The value of return should have the same type: \"" + outputResolver.get().getTypeName()
							+ "\" is expected but \"" + arguments[arguments.length - 1].getTypeName() + "\" is found");
		}

		return outputResolver.get();

	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		Object expressionValue = arguments[0].get();
		Object defaultValue = null;
		Object returnValue = null;

		if (arguments.length % 2 == 0) {
			defaultValue = arguments[arguments.length - 1].get();
		}

		if (expressionValue == null) {

			for (int i = 1; i + 1 < arguments.length; i += 2) {
				Object matchValue = arguments[i].get();

				if (matchValue == null) {
					returnValue = arguments[i + 1].get();
					returnValue = outputResolver.convertIfNecessary(returnValue, argumentObjInspectors[i + 1]);
					break;
				}
			}
		} else {

			for (int i = 1; i + 1 < arguments.length; i += 2) {
				Object matchValue = arguments[i].get();
				if (matchValue == null) {
					continue;
				}

				Object caseObj = ((PrimitiveObjectInspector) argumentObjInspectors[i])
						.getPrimitiveJavaObject(matchValue);
				Object fieldObj = ((PrimitiveObjectInspector) argumentObjInspectors[0])
						.getPrimitiveJavaObject(expressionValue);

				if (caseObj.toString().equals(fieldObj.toString())) {
					returnValue = arguments[i + 1].get();
					returnValue = outputResolver.convertIfNecessary(returnValue, argumentObjInspectors[i + 1]);
					break;
				}

			}

		}
		
		// output default if no match found for expression
		if (returnValue == null) {
			returnValue = defaultValue;
			returnValue = outputResolver.convertIfNecessary(returnValue, argumentObjInspectors[arguments.length - 1]);
		}
		return returnValue;
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("decode_expr(");
		for (int i = 0; i < children.length - 1; i++) {
			sb.append(children[i]).append(", ");
		}
		sb.append(children[children.length - 1]).append(")");
		return sb.toString();
	}
}
