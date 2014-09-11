/**
 * Copyright 2014 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.spring.hbase.example.model.frontend;

/**
 * A front end model for the web page snapshot content
 */
public class WebPageSnapshotContent {

  /**
   * The URL of the web page
   */
  private String url;

  /**
   * The UTC time that this page was fetched at.
   */
  private long fetchedAt;

  /**
   * The content of the web page
   */
  private String content;

  /**
   * Get the URL of the web page.
   * 
   * @return The URL
   */
  public String getUrl() {
    return url;
  }

  /**
   * Set the URL of the web page
   * 
   * @param url
   *          The URL to set
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Get the epoch time the web page was fetched at.
   * 
   * @return The time as an epoch
   */
  public long getFetchedAt() {
    return fetchedAt;
  }

  /**
   * Set the epoch time the web page was fetched at.
   * 
   * @param fetchedAt
   */
  public void setFetchedAt(long fetchedAt) {
    this.fetchedAt = fetchedAt;
  }

  /**
   * Get the content of the web page
   * 
   * @return The content of the web page
   */
  public String getContent() {
    return content;
  }

  /**
   * Set the content of the web page
   * 
   * @param content
   *          The content of the web page
   */
  public void setContent(String content) {
    this.content = content;
  }

}
