SNAPSHOT = {

  currentUrl : null,

	init : function(){
		SNAPSHOT.addEvents();
	},

	addEvents : function(){

		$(".header").click( function(){
			$(this).toggleClass("expanded");
			if( $(this).hasClass("expanded") ){
				var divWidth = $(window).width() - 40;
				$(this).parent().animate( {width: divWidth+"px"}, "fast", function(){
					if( $(this).attr("id") == "pageFrame" ){
						var pos = $("#pageFrame").position().top;
						$(window).scrollTop(pos);
					}
				});
			}else{
				$(this).parent().animate( {width: "45%"}, "fast");
			}
		});

		$("#urlTxtBox").keydown(function (e){
		    if(e.keyCode == 13){
		    	e.preventDefault();
		    	$("#takeSnapshotBtn").click();
		    }
		});

		$("#viewSnapshotBtn").click( function(){
			var url = SNAPSHOT.getUrl();

			SNAPSHOT.clearPage();

			if( SNAPSHOT.validURL(url) ){
				SNAPSHOT.getAndAddWebPageMeta(url);
				SNAPSHOT.setDivHeight();
				SNAPSHOT.addWebPageContent(url);
			} else{
				$(".errorMsg").html("<b>Enter a valid url.  ie:</b> <em>http://www.google.com</em>");
			}
		});

		$("#takeSnapshotBtn").click( function(){

			var url = SNAPSHOT.getUrl();

			SNAPSHOT.clearPage();

			if( SNAPSHOT.validURL(url) ){

				$.post("/takeSnapshot", { url: url }, function(data){
					if (data != null){
						SNAPSHOT.addWebPageMeta(url, data);
						SNAPSHOT.setDivHeight();
						SNAPSHOT.addWebPageContent( data.url );
						$(".infoMsg").html("<b>Snapshot successfully taken.");
					}else{
						$(".errorMsg").html("Sorry. No data returned");
					}

				}).fail(function(xhr){
					switch (xhr.status) {
						case 404:
							$(".errorMsg").html("Error HTTP Status Code Fetching Snapshot");
							break;
						default:
							$(".errorMsg").html("Uknown error taking snapshot.");
					}
				});
			}else{
				$(".errorMsg").html("<b>Enter a valid url.  ie:</b> <em>http://www.google.com</em>");
			}

		});

		$( "#tsSelectBox" ).change(function() {
			if (SNAPSHOT.currentUrl == null) {
				return;
			}

			var ts = $("#tsSelectBox").val();
			var url = SNAPSHOT.currentUrl;
			SNAPSHOT.clearPage();
			SNAPSHOT.getAndAddWebPageMeta(url, ts);
			SNAPSHOT.setDivHeight();
			SNAPSHOT.addWebPageContent(url, ts);
		});
	},

	getAndAddWebPageMeta: function( url, ts ){
		if (arguments.length == 2) {
			var ajaxURL = "/meta?url=" + url + "&ts=" + ts;
		} else {
			var ajaxURL = "/mostRecentMeta?url=" + url;
		}
		$.getJSON( ajaxURL, function(data){
			if (data != null) {
			  SNAPSHOT.addWebPageMeta(url, data);
			} else {
				$(".errorMsg").html("Sorry. No data returned");
			}
		});
	},

	addWebPageMeta: function( url, data ){
		if( typeof data.title != "undefined"){
			$("#metaList").append("<li><span>Title:</span> " + data.title + "</li>");
			$("#pageFrame .header").text(data.title);
		}else{
			$("#metaList").append("<li><span>Title:</span> </li>");
		}

		if(url != data.url) {
			$(".warnMsg").html("<b>URL is stored under redirect destination URL: " + data.url + "</b>");
		}
		$("#metaList").append("<li><span>URL:</span> " + data.url + "</li>");

		for( prop in data ){
			if( prop != "title" && prop != "url" && prop != "outlinks" ){

				$("#metaList").append("<li><span>" + prop + ":</span> " + data[prop].toString() + "</li>");

			}else if( prop == "outlinks"){

				var htmlStr = "<li><span>" + prop + ":</span> <ul>";
				var linkArray = data[prop] || [];

				for( var i=0, maxi=linkArray.length; i < maxi; i++ ){
					htmlStr += "<li>" + linkArray[i] + "</li>";
				}

				htmlStr += "</ul></li>";

				$("#metaList").append(htmlStr);
			}
		}

		$.getJSON("/snapshotTimestamps?url=" + data.url, function(tsData) {
			var selectBox = $("#tsSelectBox");
			for (var i in tsData) {
				var ts = tsData[i];
				var date = new Date(ts);
				var dateStr = $.format.date(date, 'yyyy-MM-dd HH:mm:ss');
				selectBox.append("<option value='" + ts + "'>" + dateStr + "</option>");
			}
			selectBox.val(data.fetchedAt);
		});

	},

	addWebPageContent: function( url, ts ){
		if (arguments.length == 2) {
			var ajaxURL = "/content?url=" + url + "&ts=" + ts;
		} else {
			var ajaxURL = "/mostRecentContent?url=" + url;
		}
		$.getJSON( ajaxURL, function(data){
			if( data != null  && typeof data.content != "undefined" ){
				var contentWithBaseSet = data.content.replace( "<head>", "<head><base href='" + url +"' target='_blank'></base>")
				document.getElementById("iframeContent").contentWindow.document.write(contentWithBaseSet);
			}
			$(".container").show();
			$("#tsContainer").show();
			SNAPSHOT.currentUrl = url;
		});
	},

	getUrl : function(){
		var url = $("#urlTxtBox").val();
		if (url.indexOf("://") == -1) {
			url = "http://" + url;
			$("#urlTxtBox").val(url);
		}
		return url;
	},

	validURL : function(url){
		var urlPattern = /(http|ftp|https):\/\/[\w-]+(\.[\w-]+)+([\w.,@?^=%&amp;:\/~+#-]*[\w@?^=%&amp;\/~+#-])?/;
		if( urlPattern.test(url) ){
			return true;
		}else{
			return false;
		}
	},

	clearPage : function(){
		$(".errorMsg").html("");
		$(".warnMsg").html("");
		$(".infoMsg").html("");
		$("#metaList").html("");
		$("#pageFrame .header").text(" ");
		$("#pageFrame iframe").attr("src", "");
		$("#pageFrame iframe").html("");
		$('#tsSelectBox').find('option').remove();
		$(".container").hide();
		$("#tsContainer").hide();
		SNAPSHOT.currentUrl = null;
	},

	setDivHeight : function(){
		var contentHeight = $(window).height() - 260;
		var iframeHeight = contentHeight - 8;
		$(".content").height(contentHeight + "px");
		$("#iframeContent").height(iframeHeight + "px");
	}

}
