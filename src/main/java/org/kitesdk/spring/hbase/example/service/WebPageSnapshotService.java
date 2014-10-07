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
import org.kitesdk.data.Key;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.spring.hbase.example.model.WebPageRedirectModel;
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
  private RandomAccessDataset<WebPageRedirectModel> webPageRedirectModels;

  @Autowired
  private ConversionService conversionService;

  /**
   * Take a snapshot of an URL. This WebPageSnapshot is stored in HBase. Returns
   * the WebPageSnapshotMeta
   * 
   * If the URL is a redirect, the snapshot is stored under the final URL
   * destination. A WebPageRedirectModel is stored in the redirect table so when
   * fetching snapshots, we can follow the proper redirect path.
   * 
   * @param url
   *          The URL to take a snapshot of
   * @return The WebPageSnapshotMeta for the page that we snapshotted.
   * @throws IOException
   */
  public WebPageSnapshotMeta takeSnapshot(String url) throws IOException {
    WebPageSnapshotModel webPageSnapshotModel = fetchWebPage(url);
    if (!webPageSnapshotModel.getUrl().equals(url)) {
      // Url is different, so must have redirected. Store the redirect model
      WebPageRedirectModel redirectModel = WebPageRedirectModel.newBuilder()
          .setUrl(url).setDestinationUrl(webPageSnapshotModel.getUrl()).build();
      webPageRedirectModels.put(redirectModel);
    } else {
      // If redirect exists, remove it since this URL no longer redirects
      Key key = new Key.Builder(webPageRedirectModels).add("url", url).build();
      WebPageRedirectModel redirectModel = webPageRedirectModels.get(key);
      if (redirectModel != null) {
        webPageRedirectModels.delete(key);
      }
    }
    webPageSnapshotModels.put(webPageSnapshotModel);
    return conversionService.convert(webPageSnapshotModel,
        WebPageSnapshotMeta.class);
  }

  /**
   * Get the most recent WebPageSnapshotMeta from HBase
   * 
   * @param url
   *          The URL of the WebPageSnapshotMeta to get from HBase.
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
   * Get the WebPageSnapshotMeta that was fetched at a particular timestamp from
   * HBase
   * 
   * @param url
   *          The URL of the WebPageSnapshotMeta to get from HBase.
   * @param ts
   *          The snapshot timestamp of the WebPageSnapshotMeta to get from
   *          HBase.
   * @return The WebPageSnapshotMeta, or null if one doesn't exist for this URL
   *         at this timestamp.
   */
  public WebPageSnapshotMeta getWebPageSnapshotMeta(String url, long ts) {
    WebPageSnapshotModel model = this.getWebPageSnapshot(url, ts);
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
   * Get the WebPageSnapshotContent that was fetched at a particular timestamp
   * from HBase
   * 
   * @param url
   *          The URL of the WebPageSnapshotContent to get from HBase.
   * @param ts
   *          The snapshot timestamp of the WebPageSnapshotContent to get from
   *          HBase.
   * @return The WebPageSnapshotContent, or null if one doesn't exist for this
   *         URL at this timestamp.
   */
  public WebPageSnapshotContent getWebPageSnapshotContent(String url, long ts) {
    WebPageSnapshotModel model = getWebPageSnapshot(url, ts);
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
   * Get the epoch timestamps for every snapshot time of an URL in HBase.
   * 
   * @param url
   *          The URL of the page to get snapshot timestamps for
   * @return The list of timestamps
   */
  public List<Long> getSnapshotTimestamps(String url) {
    url = normalizeUrl(url);
    List<Long> snapshotTimestamps = new ArrayList<Long>();
    DatasetReader<WebPageSnapshotModel> reader = null;
    try {
      reader = webPageSnapshotModels.from("url", url)
          .from("fetchedAtRevTs", 0L).to("url", url)
          .to("fetchedAtRevTs", Long.MAX_VALUE).newReader();
      while (reader.hasNext()) {
        snapshotTimestamps.add(reader.next().getFetchedAt());
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return snapshotTimestamps;
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
    url = normalizeUrl(url);
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
   * Get the WebPageSnapshotModel from HBase
   * 
   * @param url
   *          The URL of the WebPageSnapshotModel
   * @param ts
   *          The snapshot timestamp of the WebPageSnapshotModel
   * @return The WebPageSnapshotModel, or null if there is no snapshot for the
   *         URL at this timestamp.
   */
  private WebPageSnapshotModel getWebPageSnapshot(String url, long ts) {
    url = normalizeUrl(url);
    Key key = new Key.Builder(webPageSnapshotModels).add("url", url)
        .add("fetchedAtRevTs", Long.MAX_VALUE - ts).build();
    return webPageSnapshotModels.get(key);
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
    url = normalizeUrl(url);
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
   * Normalize an URL, which currently only consists of returning a redirect
   * destination if an URL is a redirect, or otherwise the passed in url.
   * 
   * @param url
   *          The url to normalize
   * @return The normalized URL;
   */
  private String normalizeUrl(String url) {
    // If this url is a redirect, get it's destination URL to fetch from our
    // HBase store since we store all snapshots under the final destination the
    // page lives at.
    WebPageRedirectModel redirectModel = getRedirect(url);
    if (redirectModel != null) {
      return redirectModel.getDestinationUrl();
    } else {
      return url;
    }
  }

  /**
   * Return a WebPageRedirectModel if an URL is one that redirects to a
   * different source. Otherwise, returns null.
   * 
   * @return The WebPageRedirectModel
   */
  private WebPageRedirectModel getRedirect(String url) {
    Key key = new Key.Builder(webPageRedirectModels).add("url", url).build();
    return webPageRedirectModels.get(key);
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
    int timeToFetch = (int) (postFetchTime - fetchTime);

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
