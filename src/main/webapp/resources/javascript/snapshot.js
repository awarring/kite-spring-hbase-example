SNAPSHOT = {
		
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
		    	$("#submitBtn").click();
		    }
		});

		$("#submitBtn").click( function(){
			
			var url = $("#urlTxtBox").val();
			
			SNAPSHOT.clearPage();
			
			if( SNAPSHOT.validURL(url) ){
				
				$.post("/takeSnapshot", { url: url }, function(data){
					
					if( data != null ){
						if( typeof data.title != "undefined"){
							$("#metaList").append("<li><span>Title:</span> " + data.title + "</li>");
							$("#pageFrame .header").text(data.title);
						}else{
							$("#metaList").append("<li><span>Title:</span> </li>");
						}
						
						$("#metaList").append("<li><span>URL:</span> " + url + "</li>");
						
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
						SNAPSHOT.setDivHeight();
						SNAPSHOT.addWebPageContent( url );
					}else{
						$(".errorMsg").html("Sorry. No data returned");
					}
					
				});
				
				//$("#pageFrame iframe").attr("src", url)
				
			}else{
				$(".errorMsg").html("<b>Enter a valid url.  ie:</b> <em>http://www.google.com</em>");
			}
			
		});
	},
	
	addWebPageContent: function( url ){
		var ajaxURL = "/mostRecentContent?url=" + url;
		$.getJSON( ajaxURL, function(data){
			if( data != null  && typeof data.content != "undefined" ){
				var contentWithBaseSet = data.content.replace( "<head>", "<head><base href='" + url +"' target='_blank'></base>")
				document.getElementById("iframeContent").contentWindow.document.write(contentWithBaseSet);
			}
			$(".container").show();
		});
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
		$("#metaList").html("");
		$("#pageFrame .header").text(" ");
		$("#pageFrame iframe").attr("src", "");
		$("#pageFrame iframe").html("");
	},
	
	setDivHeight : function(){
		var contentHeight = $(window).height() - 260;
		var iframeHeight = contentHeight - 8;
		$(".content").height(contentHeight + "px");
		$("#iframeContent").height(iframeHeight + "px");
	}
	
}