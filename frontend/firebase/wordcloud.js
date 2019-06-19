function getImage()
{
	var storage = firebase.storage();
	var storageRef = storage.refFromURL('gs://real-time-data-visualization.appspot.com/wordcloud.png')
	//var httpsReference = storage.refFromURL('https://firebasestorage.googleapis.com/b/real-time-data-visualization.appspot.com/o/wordcloud.png');
	storageRef.getDownloadURL().then(function(url) {
  	// Or inserted into an <img> element:
  	var ctx = document.getElementById('imageBox').getContext('2d');
	var img = new Image();
  	img.onload = function(){
    		ctx.drawImage(img, 0, 0, 300, 300);
    		//ctx.beginPath();
    		//ctx.moveTo(400,400);
    		//ctx.stroke();
  	};
  	img.src = url;
	}).catch(function(error) {
  		// Handle any errors
	});
	//document.getElementById('imageBox').src = httpsReference
}
//var db = firebase.firestore();
//db.collection("GoogleTrend").doc("Top 1").onSnapshot(function(doc) {
	//getImage()

//});

