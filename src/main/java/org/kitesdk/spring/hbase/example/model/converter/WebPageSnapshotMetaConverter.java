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
package org.kitesdk.spring.hbase.example.model.converter;

import org.kitesdk.spring.hbase.example.model.WebPageSnapshotModel;
import org.kitesdk.spring.hbase.example.model.frontend.WebPageSnapshotMeta;
import org.springframework.core.convert.converter.Converter;

/**
 * Converter to convert from the backend model WebPageSnapshotModel to the
 * frontend model WebPageSnapshotMeta
 */
public class WebPageSnapshotMetaConverter implements
    Converter<WebPageSnapshotModel, WebPageSnapshotMeta> {

  @Override
  public WebPageSnapshotMeta convert(WebPageSnapshotModel model) {
    WebPageSnapshotMeta meta = new WebPageSnapshotMeta();
    meta.setUrl(model.getUrl());
    meta.setFetchedAt(model.getFetchedAt());
    meta.setFetchTimeMs(model.getFetchTimeMs());
    meta.setSize(model.getSize());
    meta.setTitle(model.getTitle());
    meta.setDescription(model.getDescription());
    meta.setKeywords(model.getKeywords());
    meta.setOutlinks(model.getOutlinks());
    return meta;
  }

}
