package com.example.bean;

public class EsIdVersion<T> {

	private T id;
	private Long version;

	public EsIdVersion(T id, Long version) {
		this.id = id;
		this.version = version;
	}

	public EsIdVersion(T id) {
		this(id, null);
	}

	public T getId() {
		return id;
	}

	public Long getVersion() {
		return version;
	}

}
