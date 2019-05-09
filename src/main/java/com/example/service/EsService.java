package com.example.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import com.alibaba.fastjson.JSONObject;
import com.example.bean.EsIdVersion;
import com.example.util.ReflectUtils;

public class EsService {

	private RestHighLevelClient client;

	public EsService(RestHighLevelClient client) {
		this.client = client;
	}

	private static final String DOC_NAME = "doc";
	private static final String PROPERTIES = "properties";
	private static final String TYPE = "type";
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public boolean createIndex(Class<?> clazz, String indexName, int numberOfShards, int numberOfReplicas) {
		try {
			CreateIndexRequest request = new CreateIndexRequest(indexName);
			request.settings(Settings.builder().put("index.number_of_shards", numberOfShards)
					.put("index.number_of_replicas", numberOfReplicas));
			JSONObject docJson = new JSONObject();
			JSONObject properties = new JSONObject();
			JSONObject property = new JSONObject();
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				if (!field.getName().equals("id") && !field.getName().equals("version")) {
					JSONObject propertyType = new JSONObject();
					propertyType.put(TYPE, getType(field.getType()));
					property.put(field.getName(), propertyType);
					if ("date".equals(getType(field.getType()))) {
						propertyType.put("format", DATE_FORMAT);
					}
				}
			}
			properties.put(PROPERTIES, property);
			docJson.put(DOC_NAME, properties);
			request.mapping(DOC_NAME, docJson.toJSONString(), XContentType.JSON);

			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
			return createIndexResponse.isAcknowledged();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean deleteIndex(String indexName) {
		DeleteIndexRequest request = new DeleteIndexRequest(indexName);
		request.timeout(TimeValue.timeValueMinutes(2));
		try {
			AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
			return deleteIndexResponse.isAcknowledged();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	public boolean save(String indexName, Object bean) {
		IndexRequest request = buildSaveIndexRequest(indexName, bean);
		try {
			client.index(request, RequestOptions.DEFAULT);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean saveAll(String indexName, List<?> list) {
		BulkRequest request = new BulkRequest();
		for (Object bean : list) {
			request.add(buildSaveIndexRequest(indexName, bean));
		}
		return bulkRequest(request);
	}

	private boolean bulkRequest(BulkRequest request) {
		try {
			client.bulk(request, RequestOptions.DEFAULT);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean delete(String indexName, EsIdVersion<?> param) {
		DeleteRequest request = getDeleteDocRequest(indexName, param);
		try {
			client.delete(request, RequestOptions.DEFAULT);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private DeleteRequest getDeleteDocRequest(String indexName, EsIdVersion<?> param) {
		DeleteRequest request = new DeleteRequest(indexName, DOC_NAME, param.getId().toString());
		if (param.getVersion() != null) {
			request.version(param.getVersion());
		}
		return request;
	}

	public boolean deleteAll(String indexName, List<EsIdVersion<?>> params) {
		BulkRequest request = new BulkRequest();
		for (EsIdVersion<?> param : params) {
			request.add(getDeleteDocRequest(indexName, param));
		}
		return bulkRequest(request);
	}

	public <T> T get(String indexName, EsIdVersion<?> param, Class<T> clazz) {
		GetRequest getRequest = new GetRequest(indexName, DOC_NAME, param.getId().toString());
		if (param.getVersion() != null) {
			getRequest.version(param.getVersion());
		}
		try {
			GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
			Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
			if (sourceAsMap == null || sourceAsMap.isEmpty()) {
				return ReflectUtils.newInstance(clazz);
			}
			T obj = ReflectUtils.map2Bean(sourceAsMap, clazz);
			setEsVersion(clazz, getResponse, obj);
			setEsId(clazz, getResponse, obj, param.getId().getClass());
			return obj;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private void setEsId(Class<?> clazz, GetResponse getResponse, Object obj, Class<?> idType) {
		try {
			Method method = ReflectUtils.getMethodByName(clazz, "setId", idType);
			if (idType == String.class) {
				method.invoke(obj, getResponse.getId());
				return;
			}

			if (idType == Integer.class) {
				method.invoke(obj, Integer.parseInt(getResponse.getId()));
				return;
			}

			if (idType == Long.class) {
				method.invoke(obj, Long.parseLong(getResponse.getId()));
				return;
			}

		} catch (Exception e) {
			//warn method setEsVersion not exist

		}
	}

	private void setEsVersion(Class<?> clazz, GetResponse getResponse, Object obj) {
		try {
			Method method = ReflectUtils.getMethodByName(clazz, "setEsVersion", Long.class);
			method.invoke(obj, getResponse.getVersion());
		} catch (Exception e) {
			//warn method setEsVersion not exist

		}
	}

	private IndexRequest buildSaveIndexRequest(String indexName, Object bean) {
		IndexRequest request = new IndexRequest(indexName, DOC_NAME, getId(bean).toString());
		Map<String, Object> jsonMap = new HashMap<>();
		Class<?> clazz = bean.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			setSaveRequestSrouce(bean, request, jsonMap, clazz, field);

		}
		request.source(jsonMap);
		return request;
	}

	private void setSaveRequestSrouce(Object bean, IndexRequest request, Map<String, Object> jsonMap, Class<?> clazz,
			Field field) {
		if (field.getName().equals("id")) {
			return;
		}

		if (field.getName().equals("esVersion")) {
			if (field.getType() == Long.class) {
				Object version = ReflectUtils.invokeGetMethod(bean, clazz, field);
				if (version != null) {
					request.version((Long) version);
				}
			}
			return;
		}
		if (field.getType() == LocalDateTime.class) {
			Object value = ReflectUtils.invokeGetMethod(bean, clazz, field);
			if (value != null) {
				LocalDateTime date = (LocalDateTime) value;
				jsonMap.put(field.getName(), date.format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
			}
			return;
		}
		Object value = ReflectUtils.invokeGetMethod(bean, clazz, field);
		if (value != null) {
			jsonMap.put(field.getName(), value);
		}
	}

	private Object getId(Object bean) {
		try {
			Class<?> clazz = bean.getClass();
			Method method = ReflectUtils.getMethodByName(clazz, "getId");
			if (method == null) {
				throw new RuntimeException("方法getId不存在");
			}
			Object id = method.invoke(bean);
			if (id == null) {
				throw new RuntimeException("ID为空");
			}
			return method.invoke(bean);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean getClusterHealth() {
		ClusterHealthRequest request = new ClusterHealthRequest("_all");
		request.timeout("1s");
		ClusterHealthResponse response;
		try {
			response = client.cluster().health(request, RequestOptions.DEFAULT);
			return response.getStatus() == ClusterHealthStatus.RED;
		} catch (IOException e) {
			return false;
		}
	}

	private String getType(Class<?> type) {
		if (type == Integer.class) {
			return "integer";
		}
		if (type == Long.class) {
			return "long";
		}
		if (type == String.class) {
			return "keyword";
		}
		if (type == LocalDateTime.class) {
			return "date";
		}
		return null;
	}

}
