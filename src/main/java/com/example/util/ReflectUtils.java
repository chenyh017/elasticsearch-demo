package com.example.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class ReflectUtils {

	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	private ReflectUtils() {
		super();
	}

	public static Method getMethodByName(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
		try {
			return clazz.getMethod(methodName, parameterTypes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public static Object invokeGetMethod(Object bean, Class<?> clazz, Field field) {
		String methodName = "get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
		Method method = ReflectUtils.getMethodByName(clazz, methodName);
		try {
			return method.invoke(bean);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T map2Bean(Map<String, Object> map, Class<T> clazz) {
		T obj = newInstance(clazz);
		try {
			for (Entry<String, Object> kv : map.entrySet()) {
				String methodName = "set" + kv.getKey().substring(0, 1).toUpperCase() + kv.getKey().substring(1);
				System.out.println(kv.getValue().getClass());
				if (kv.getValue() instanceof String && isDateTime(kv.getValue().toString())) {
					Method method = ReflectUtils.getMethodByName(clazz, methodName, LocalDateTime.class);
					method.invoke(obj,
							LocalDateTime.parse(kv.getValue().toString(), DateTimeFormatter.ofPattern(DATE_FORMAT)));
				} else {
					Method method = ReflectUtils.getMethodByName(clazz, methodName, kv.getValue().getClass());
					method.invoke(obj, kv.getValue());
				}
			}
			return obj;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T newInstance(Class<T> clazz) {
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static boolean isDateTime(String value) {
		String reg = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}";// yyyy-MM-dd HH:mm:ss 或 yyyy-MM-dd
		return Pattern.compile(reg).matcher(value).matches();

	}

}
