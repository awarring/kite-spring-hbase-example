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
package org.kitesdk.spring.hbase.example.controller;

import java.io.IOException;

import org.kitesdk.spring.hbase.example.model.frontend.WebPageSnapshotContent;
import org.kitesdk.spring.hbase.example.model.frontend.WebPageSnapshotMeta;
import org.kitesdk.spring.hbase.example.service.WebPageSnapshotService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("/")
public class WebPageSnapshotController {

  @Autowired
  private WebPageSnapshotService webPageSnapshotService;

  @RequestMapping(value = "/takeSnapshot", method = RequestMethod.POST)
  @ResponseBody
  public WebPageSnapshotMeta takeSnapshot(@RequestParam("url") String url)
      throws IOException {
    webPageSnapshotService.takeSnapshot(url);
    return getMostRecentMeta(url);
  }

  @RequestMapping(value = "/mostRecentMeta", method = RequestMethod.GET)
  @ResponseBody
  public WebPageSnapshotMeta getMostRecentMeta(@RequestParam("url") String url) {
    return webPageSnapshotService.getWebPageSnapshotMeta(url);
  }

  @RequestMapping(value = "/mostRecentContent", method = RequestMethod.GET)
  @ResponseBody
  public WebPageSnapshotContent getMostRecentContent(
      @RequestParam("url") String url) {
    return webPageSnapshotService.getWebPageSnapshotContent(url);
  }

  @RequestMapping(value = "/demo", method = RequestMethod.GET)
  public ModelAndView getHome() {
    ModelAndView mav = new ModelAndView();
    mav.setViewName("get_snapshot");
    return mav;
  }

}
