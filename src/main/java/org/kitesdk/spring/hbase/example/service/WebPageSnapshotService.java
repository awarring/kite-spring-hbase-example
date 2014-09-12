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
package org.kitesdk.spring.hbase.example.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.spring.hbase.example.model.WebPageSnapshotModel;
import org.kitesdk.spring.hbase.example.model.frontend.WebPageSnapshotContent;
import org.kitesdk.spring.hbase.example.model.frontend.WebPageSnapshotMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.stereotype.Component;

/**
 * Service for WebPageSnapshot operations
 */
@Component
public class WebPageSnapshotService {

  @Autowired
  private RandomAccessDataset<WebPageSnapshotModel> webPageSnapshotModels;

  @Autowired
  private ConversionService conversionService;

  /**
   * Take a snapshot of an URL. This WebPageSnapshot is stored in HBase. Returns
   * the WebPageSnapshotMeta
   * 
   * @param url
   *          The URL to take a snapshot of
   * @return The WebPageSnapshotMeta for the page that we snapshotted.
   * @throws IOException
   */
  public WebPageSnapshotMeta takeSnapshot(String url) throws IOException {
    WebPageSnapshotModel webPageSnapshotModel = fetchWebPage(url);
    webPageSnapshotModels.put(webPageSnapshotModel);
    return conversionService.convert(webPageSnapshotModel,
        WebPageSnapshotMeta.class);
  }

  /**
   * Get the most recent WebPageSnapshotMeta from HBase
   * 
   * @param url
   *          The URL to fetch the most recent WebPageSnapshotMeta from
   * @return The WebPageSnapshotMeta, or null if one doesn't exist for this URL.
   */
  public WebPageSnapshotMeta getWebPageSnapshotMeta(String url) {
    WebPageSnapshotModel model = getMostRecentWebPageSnapshot(url);
    if (model != null) {
      return conversionService.convert(model, WebPageSnapshotMeta.class);
    } else {
      return null;
    }
  }

  /**
   * Get all WebPageSnapshotMeta from an URL that have been snapshotted since
   * the "since" param.
   * 
   * @param url
   *          The URL to get WebPageSnapshotMeta instances from
   * @param since
   *          The epoch timestamp
   * @return The list of WebPageSnapshotMeta instances.
   */
  public List<WebPageSnapshotMeta> getWebPageSnapshotMetaSince(String url,
      long since) {
    return convertList(getWebPageSnapshotsSince(url, since),
        WebPageSnapshotMeta.class);
  }

  /**
   * Get the most recent WebPageSnapshotContent from HBase
   * 
   * @param url
   *          The URL to fetch the most recent WebPageSnapshotContent from
   * @return The WebPageSnapshotContent, or null if one doesn't exists for this
   *         URL.
   */
  public WebPageSnapshotContent getWebPageSnapshotContent(String url) {
    WebPageSnapshotModel model = getMostRecentWebPageSnapshot(url);
    if (model != null) {
      return conversionService.convert(model, WebPageSnapshotContent.class);
    } else {
      return null;
    }
  }

  /**
   * Get all WebPageSnapshotContent from an URL that have been snapshotted since
   * the "since" param.
   * 
   * @param url
   *          The URL to get WebPageSnapshotContent instances from
   * @param since
   *          The epoch timestamp
   * @return The list of WebPageSnapshotContent instances.
   */
  public List<WebPageSnapshotContent> getWebPageSnapshotContentSince(
      String url, long since) {
    return convertList(getWebPageSnapshotsSince(url, since),
        WebPageSnapshotContent.class);
  }

  /**
   * Get the most recent WebPageSnapshotModel from HBase
   * 
   * @param url
   *          The URL to get the snapshotted page from HBase
   * @return The WebPageSnapshotModel, or null if there are no fetches for this
   *         URL
   */
  private WebPageSnapshotModel getMostRecentWebPageSnapshot(String url) {
    DatasetReader<WebPageSnapshotModel> reader = null;
    try {
      // we don't know the exact timestamp in the key, but we know since keys
      // are in timestamp descending order that the first row for an URL will be
      // the most recent.
      reader = webPageSnapshotModels.from("url", url)
          .from("fetchedAtRevTs", 0L).to("url", url)
          .to("fetchedAtRevTs", Long.MAX_VALUE).newReader();
      if (reader.hasNext()) {
        return reader.next();
      } else {
        return null;
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Get WebPageSnapshotModels for an URL from HBase since the since param.
   * 
   * @param url
   *          The URL of the page to fetch
   * @param since
   *          The models to fetch since
   * @return The list of models that have been fetched for an URL since the
   *         since param.
   */
  private List<WebPageSnapshotModel> getWebPageSnapshotsSince(String url,
      long since) {
    List<WebPageSnapshotModel> models = new ArrayList<WebPageSnapshotModel>();
    DatasetReader<WebPageSnapshotModel> reader = null;
    try {
      reader = webPageSnapshotModels.from("url", url)
          .from("fetchedAtRevTs", 0L).to("url", url)
          .to("fetchedAtRevTs", since).newReader();
      while (reader.hasNext()) {
        models.add(reader.next());
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return models;
  }

  /**
   * Fetch the web page from the URL, parse the HTML to populate the metadata
   * required by WebPageSnapshotModel, and return the constructed
   * WebPageSnapshotModel.
   * 
   * @param url
   *          The URL to fetch the web page from
   * @return The WebPageSnapshotModel
   * @throws IOException
   *           Thrown if there's an issue fetching the web page.
   */
  private WebPageSnapshotModel fetchWebPage(String url) throws IOException {
    long fetchTime = System.currentTimeMillis();
    Connection connection = Jsoup.connect(url);
    Response response = connection.execute();
    long postFetchTime = System.currentTimeMillis();
    int timeToFetch = (int) (fetchTime - postFetchTime);

    Document doc = response.parse();
    String destinationUrl = response.url().toString();
    String title = doc.title();
    String description = getDescriptionFromDocument(doc);
    List<String> keywords = getKeywordsFromDocument(doc);
    List<String> outlinks = getOutlinksFromDocument(doc);

    return WebPageSnapshotModel.newBuilder().setUrl(destinationUrl)
        .setFetchedAtRevTs(Long.MAX_VALUE - fetchTime)
        .setSize(doc.html().length()).setFetchedAt(fetchTime)
        .setFetchTimeMs(timeToFetch).setTitle(title)
        .setDescription(description).setKeywords(keywords)
        .setOutlinks(outlinks).setContent(doc.html()).build();
  }

  /**
   * Parse the description out of the meta tag if one exists. Otherwise, return
   * null
   * 
   * @param doc
   *          The Document to parse
   * @return The description if it exists in the HTML, otherwise null.
   */
  private String getDescriptionFromDocument(Document doc) {
    Elements metaDescriptionElements = doc.select("meta[name=description]");
    return metaDescriptionElements.size() > 0 ? metaDescriptionElements
        .attr("content") : null;
  }

  /**
   * Parse the keywords out of the meta tag if one exists. Otherwise, return an
   * empty list.
   * 
   * @param doc
   *          The Document ot parse
   * @return The list of keywords.
   */
  private List<String> getKeywordsFromDocument(Document doc) {
    List<String> keywords = new ArrayList<String>();
    Elements keywordsElements = doc.select("meta[name=keywords]");
    for (Element keywordsElement : keywordsElements) {
      for (String keyword : keywordsElement.attr("content").split(",")) {
        keywords.add(keyword.trim());
      }
    }
    return keywords;
  }

  /**
   * Parse the outlinks from a href tags in the document, and return them as a
   * list
   * 
   * @param doc
   *          The document to parse
   * @return The list of outlinks as URL strings.
   */
  private List<String> getOutlinksFromDocument(Document doc) {
    List<String> outlinks = new ArrayList<String>();
    Elements linkElements = doc.select("a[href]");
    for (Element linkElement : linkElements) {
      outlinks.add(linkElement.attr("href").trim());
    }
    return outlinks;
  }

  /**
   * Use the conversionService to convert a list of objects to clazz
   * 
   * @param list
   *          The list of objects to convert
   * @param clazz
   *          The class to convert those objects to
   * @return The list of converted objects.
   */
  private <T> List<T> convertList(List<?> list, Class<T> clazz) {
    List<T> returnList = new ArrayList<T>();
    for (Object o : list) {
      returnList.add(conversionService.convert(o, clazz));
    }
    return returnList;
  }
}
