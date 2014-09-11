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

import java.util.List;

/**
 * A front end model that contains metadata about a WebPageSnapshot
 */
public class WebPageSnapshotMeta {

  /**
   * The URL of the web page
   */
  private String url;

  /**
   * The UTC time that this page was fetched at.
   */
  private long fetchedAt;

  /**
   * The amount of time it took to fetch the web page, in ms
   */
  private int fetchTimeMs;

  /**
   * Get the size of the web page
   */
  private int size;

  /**
   * The title of the HTML page, if one exists
   */
  private String title;

  /**
   * The description from the HTML meta tag
   */
  private String description;

  /**
   * The keywords from the HTML meta tag
   */
  private List<String> keywords;

  /**
   * The outlinks from this page
   */
  private List<String> outlinks;

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
   * Get the epoch time the WebPageSnapshot was fetched at.
   * 
   * @return The time as an epoch
   */
  public long getFetchedAt() {
    return fetchedAt;
  }

  /**
   * Set the epoch time the WebPageSnapshot was fetched at.
   * 
   * @param fetchedAt
   */
  public void setFetchedAt(long fetchedAt) {
    this.fetchedAt = fetchedAt;
  }

  /**
   * Get the amount of time it took to fetch the web page.
   * 
   * @return The amount of time, in ms, it took to fetch the web page.
   */
  public int getFetchTimeMs() {
    return fetchTimeMs;
  }

  /**
   * Set the amount of time it took to fetch the web page.
   * 
   * @param fetchTimeMs
   *          The amount of time, in ms, it took to fetch the web page.
   */
  public void setFetchTimeMs(int fetchTimeMs) {
    this.fetchTimeMs = fetchTimeMs;
  }

  /**
   * Get the size of the web page
   * 
   * @return The size in bytes
   */
  public int getSize() {
    return size;
  }

  /**
   * Set the size of the web page
   * 
   * @param size
   *          The size in bytes
   */
  public void setSize(int size) {
    this.size = size;
  }

  /**
   * Get the title of the web page
   * 
   * @return The title if one exists, otherwise null
   */
  public String getTitle() {
    return title;
  }

  /**
   * Set the title of the web page
   * 
   * @param title
   *          The title of the web page
   */
  public void setTitle(String title) {
    this.title = title;
  }

  /**
   * Get the description of the web page
   * 
   * @return The description of the web page if it exists, otherwise null
   */
  public String getDescription() {
    return description;
  }

  /**
   * Set the description of the web page
   * 
   * @param description
   *          The description of the web page
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Get the keywords that were set in the meta tag for this web page
   * 
   * @return The keywords of the web page
   */
  public List<String> getKeywords() {
    return keywords;
  }

  /**
   * Set the keywords of the web page
   * 
   * @param keywords
   *          THe keywords of the web page.
   */
  public void setKeywords(List<String> keywords) {
    this.keywords = keywords;
  }

  /**
   * Get the list of outlinks from this web page
   * 
   * @return The list of outlinks from thsi web page
   */
  public List<String> getOutlinks() {
    return outlinks;
  }

  /**
   * Set the list of outlinks from this web page
   * 
   * @param outlinks
   *          The list of outlinks from this web page
   */
  public void setOutlinks(List<String> outlinks) {
    this.outlinks = outlinks;
  }

}
