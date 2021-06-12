package com.mystorm.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldGroupDto {

  @JsonProperty("id")
  private String id;

  @JsonProperty("customerId")
  private String customerId;

  public FieldGroupDto() {}

  public FieldGroupDto(String id, String customerId) {
    this.id = id;
    this.customerId = customerId;
  }

  public String getId() {
    return id;
  }

  public String getCustomerId() {
    return customerId;
  }

  @Override
  public String toString() {
    return "FieldGroupDto{" + "id='" + id + '\'' + ", customerId='" + customerId + '\'' + '}';
  }
}
