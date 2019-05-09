package com.example.demo;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.example.bean.EsIdVersion;
import com.example.bean.WfTask;
import com.example.service.EsService;

public class WfTaskEsTest {

	private EsService esService;

	private RestHighLevelClient client;

	@Before
	public void setClient() {
		this.client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.0.156", 9200, "http")));
		esService = new EsService(this.client);
	}

	@Test
	public void createAnddeleteIndex() {
		Assert.assertTrue(esService.createIndex(WfTask.class, "create_delete_index", 3, 0));
		Assert.assertTrue(esService.deleteIndex("create_delete_index"));
	}

	@Test
	public void health() {
		Assert.assertTrue(!esService.getClusterHealth());
	}

	@Test
	public void save() {
		String indexName = "save";
		WfTask wfTask1 = new WfTask();
		wfTask1.setId(1);
		wfTask1.setName("测试");
		wfTask1.setPushTime(LocalDateTime.now());
		Assert.assertTrue(esService.save(indexName, wfTask1));
		WfTask result = esService.get(indexName, new EsIdVersion<Integer>(1), WfTask.class);
		Assert.assertEquals((Object) 1, result.getId());
		Assert.assertTrue(esService.deleteIndex(indexName));
	}

	@Test
	public void saveAll() {
		String indexName = "save_all";
		esService.createIndex(WfTask.class, indexName, 3, 0);
		List<WfTask> list = new ArrayList<>();
		for (int i = 1; i < 10; i++) {
			WfTask wfTask1 = new WfTask();
			wfTask1.setId(i);
			wfTask1.setName("测试");
			wfTask1.setPushTime(LocalDateTime.now());
			list.add(wfTask1);
		}
		Assert.assertTrue(esService.saveAll(indexName, list));

		for (int i = 1; i < 10; i++) {
			WfTask result = esService.get(indexName, new EsIdVersion<Integer>(i), WfTask.class);
			Assert.assertEquals((Object) i, result.getId());
		}
		Assert.assertTrue(esService.deleteIndex(indexName));
	}

	@Test
	public void delete() {
		String indexName = "test_delete";
		esService.createIndex(WfTask.class, indexName, 3, 0);
		WfTask wfTask1 = new WfTask();
		wfTask1.setId(10);
		wfTask1.setName("测试");
		wfTask1.setPushTime(LocalDateTime.now());
		wfTask1.setCurrentStatus(10);
		esService.save(indexName, wfTask1);

		WfTask result = esService.get(indexName, new EsIdVersion<Integer>(wfTask1.getId(), wfTask1.getEsVersion()),
				WfTask.class);
		Assert.assertEquals((Object) 10, result.getId());

		Assert.assertTrue(esService.delete(indexName, new EsIdVersion<Integer>(wfTask1.getId())));

		result = esService.get(indexName, new EsIdVersion<Integer>(wfTask1.getId(), wfTask1.getEsVersion()),
				WfTask.class);
		Assert.assertTrue(result.getId() == null);

		Assert.assertTrue(esService.deleteIndex(indexName));

	}

	@Test
	public void deleteAll() {
		String indexName = "test_delete_all";
		esService.createIndex(WfTask.class, indexName, 3, 0);

		List<WfTask> list = new ArrayList<>();
		WfTask wfTask1 = new WfTask();
		wfTask1.setId(10);
		wfTask1.setName("测试");
		wfTask1.setPushTime(LocalDateTime.now());
		wfTask1.setCurrentStatus(10);
		list.add(wfTask1);

		wfTask1 = new WfTask();
		wfTask1.setId(11);
		wfTask1.setName("测试");
		wfTask1.setPushTime(LocalDateTime.now());
		wfTask1.setCurrentStatus(10);
		list.add(wfTask1);

		esService.saveAll(indexName, list);

		WfTask result = esService.get(indexName, new EsIdVersion<Integer>(wfTask1.getId(), wfTask1.getEsVersion()),
				WfTask.class);
		Assert.assertEquals((Object) 11, result.getId());

		List<EsIdVersion<?>> params = new ArrayList<>();
		params.add(new EsIdVersion<Integer>(10));
		params.add(new EsIdVersion<Integer>(11));
		Assert.assertTrue(esService.deleteAll(indexName, params));

		result = esService.get(indexName, new EsIdVersion<Integer>(wfTask1.getId(), wfTask1.getEsVersion()),
				WfTask.class);
		Assert.assertTrue(result.getId() == null);

		Assert.assertTrue(esService.deleteIndex(indexName));

	}

	@Test
	public void demo() throws IOException {
		String indexName = "demo";
		esService.createIndex(WfTask.class, indexName, 3, 0);
		WfTask wfTask1 = new WfTask();
		wfTask1.setId(10);
		wfTask1.setCreateUser("创建1");
		wfTask1.setPushUser("推土机");
		wfTask1.setPushTime(LocalDateTime.now());
		wfTask1.setCurrentStatus(1);
		wfTask1.setLastUser("不为空");
		esService.save(indexName, wfTask1);

		
		try {
			Thread.sleep(1000l);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		//select * from demo where createUser='创建1' and createTime is null  and pushUser like '%推%' and currentStatus in (1,2) and (pushTime < now() or lastUser is not null) 

		//构造BoolQueryBuilder
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

		//3、用boolQueryBuilder拼接条件
		boolQueryBuilder.must(QueryBuilders.termQuery("createUser", "创建1")); // createUser='创建1'
		boolQueryBuilder.must(QueryBuilders.wildcardQuery("pushUser", "*推*")); // pushUser like '%推%'
		boolQueryBuilder.mustNot(QueryBuilders.existsQuery("createTime")); //createTime is null
		boolQueryBuilder.must(QueryBuilders.termsQuery("currentStatus", new int[] { 1, 2 })); // currentStatus in (1,2) 
		//OR
		boolQueryBuilder.should(QueryBuilders.rangeQuery("pushTime")
				.lt(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))); //pushTime < now()
		boolQueryBuilder.should(QueryBuilders.existsQuery("lastUser")); //lastUser is not null

		//构造SearchSourceBuilder
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(boolQueryBuilder);

		//构造SearchRequest，指定indexName
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.source(searchSourceBuilder);
		

		searchSourceBuilder.toString();

		//SearchResponse
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

		//获取查询命中结果
		SearchHits hits = searchResponse.getHits();

		System.out.println(hits.getTotalHits());
		
		Assert.assertTrue(esService.deleteIndex(indexName));

	}

}
