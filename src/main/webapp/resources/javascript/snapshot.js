SNAPSHOT = {
		
	init : function(){
		SNAPSHOT.addEvents();
	},

	addEvents : function(){
		
		$(".header").click( function(){ 
			$(this).toggleClass("expanded");
			if( $(this).hasClass("expanded") ){
				var divWidth = $(window).width() - 40;
				$(this).parent().animate( {width: divWidth+"px"}, "fast");
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
				
				$.post("http://localhost:8080/takeSnapshot", { url: url }, function(data){
					
					if( data != null ){
						if( typeof data.title != "undefined"){
							$("#metaList").append("<li><span>Title:</span> " + data.title + "</li>");
							$("#pageFrame .header").text(data.title);
						}else{
							$("#metaList").append("<li><span>Title:</span> </li>");
						}
						
						$("#metaList").append("<li><span>URL:</span> " + url + "</li>");
						
						for( prop in data ){
							if( prop != "title" && prop != "url" ){
								$("#metaList").append("<li><span>" + prop + ":</span> " + data[prop].toString() + "</li>");
							}
						}
						
						SNAPSHOT.addWebPageContent( url );
					}else{
						$(".errorMsg").html("Sorry. No data returned");
					}
					
				});
				
				$("#pageFrame iframe").attr("src", url)
				
			}else{
				$(".errorMsg").html("<b>Enter a valid url.  ie:</b> <em>http://www.google.com</em>");
			}
			
		});
	},
	
	addWebPageContent: function( url ){
		var ajaxURL = "http://localhost:8080/mostRecentContent?url=" + url;
		$.getJSON( ajaxURL, function(data){
			if( data != null  && typeof data.content != "undefined" ){
				$("#pageContent pre").text( data.content );
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
		$("#pageContent pre").html("");
		$("#pageFrame .header").text(" ");
		$("#pageFrame iframe").attr("src", "");
		$("#pageFrame iframe").html("");
	}
	
}