package com.mystorm.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EccProcessorDto {

  private String id;

  private Integer userId;

  private String tnxId;

  public EccProcessorDto() {}

  public EccProcessorDto(String id, Integer userId, String tnxId) {
    this.id = id;
    this.userId = userId;
    this.tnxId = tnxId;
  }

  public String getId() {
    return id;
  }

  public Integer getUserId() {
    return userId;
  }

  public String getTnxId() {
    return tnxId;
  }

  @Override
  public String toString() {
    return "EccProcessorDto{"
        + "id='"
        + id
        + '\''
        + ", userId="
        + userId
        + ", tnxId='"
        + tnxId
        + '\''
        + '}';
  }
}
