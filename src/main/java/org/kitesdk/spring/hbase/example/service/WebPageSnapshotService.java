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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.spring.hbase.example.model.WebPageSnapshotModel;
import org.kitesdk.spring.hbase.example.model.frontend.WebPageSnapshotContent;
import org.kitesdk.spring.hbase.example.model.frontend.WebPageSnapshotMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.stereotype.Component;

@Component
public class WebPageSnapshotService {

  @Autowired
  private RandomAccessDataset<WebPageSnapshotModel> webPageSnapshotModels;

  @Autowired
  private ConversionService conversionService;

  public void takeSnapshot(String url) throws IOException {
    WebPageSnapshotModel webPageSnapshotModel = fetchWebPage(url);
    webPageSnapshotModels.put(webPageSnapshotModel);
  }

  public WebPageSnapshotMeta getWebPageSnapshotMeta(String url) {
    WebPageSnapshotModel model = getMostRecentWebPageSnapshot(url);
    if (model != null) {
      return conversionService.convert(model, WebPageSnapshotMeta.class);
    } else {
      return null;
    }
  }

  public List<WebPageSnapshotMeta> getWebPageSnapshotMetaSince(String url,
      long since) {
    return convertList(getWebPageSnapshotsSince(url, since),
        WebPageSnapshotMeta.class);
  }

  public WebPageSnapshotContent getWebPageSnapshotContent(String url) {
    WebPageSnapshotModel model = getMostRecentWebPageSnapshot(url);
    if (model != null) {
      return conversionService.convert(model, WebPageSnapshotContent.class);
    } else {
      return null;
    }
  }

  public List<WebPageSnapshotContent> getWebPageSnapshotContentSince(
      String url, long since) {
    return convertList(getWebPageSnapshotsSince(url, since),
        WebPageSnapshotContent.class);
  }

  private WebPageSnapshotModel getMostRecentWebPageSnapshot(String url) {
    DatasetReader<WebPageSnapshotModel> reader = null;
    try {
      reader = webPageSnapshotModels.from("url", url).from("fetchedAtRevTs", 0L)
          .newReader();
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

  private List<WebPageSnapshotModel> getWebPageSnapshotsSince(String url,
      long since) {
    List<WebPageSnapshotModel> models = new ArrayList<WebPageSnapshotModel>();
    DatasetReader<WebPageSnapshotModel> reader = null;
    try {
      reader = webPageSnapshotModels.from("url", url).from("fetchedAtRevTs", 0L)
          .to("url", url).to("fetchedAtRevTs", since).newReader();
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

  private WebPageSnapshotModel fetchWebPage(String url) throws IOException {
    long fetchTime = System.currentTimeMillis();
    Document doc = Jsoup.connect(url).get();
    long postFetchTime = System.currentTimeMillis();
    int timeToFetch = (int) (fetchTime - postFetchTime);

    return WebPageSnapshotModel.newBuilder().setUrl(url)
        .setFetchedAtRevTs(Long.MAX_VALUE - fetchTime)
        .setSize(doc.html().length()).setFetchedAt(fetchTime)
        .setFetchTimeMs(timeToFetch).setTitle("").setDescription("")
        .setKeywords(new ArrayList<String>())
        .setOutlinks(new ArrayList<String>()).setContent(doc.html()).build();
  }

  private <T> List<T> convertList(List<?> list, Class<T> clazz) {
    List<T> returnList = new ArrayList<T>();
    for (Object o : list) {
      returnList.add(conversionService.convert(o, clazz));
    }
    return returnList;
  }
}
