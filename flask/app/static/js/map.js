var map, heatmap, infoWindow;

function initMap() {
    $.ajax({
        type: "GET",
        url: "/api/heatmap/sf",
        dataType: "text"
    }).then((data) => {
        infoWindow = new google.maps.InfoWindow();
        renderHeatMap(data);
        return $.ajax({
            type: "GET",
            url: "/api/listings/sf",
            dataType: "text"
        });
    }).then((data) => {
        renderMarker(data);
    });
}

function toggleHeatmap() {
    heatmap.setMap(heatmap.getMap() ? null : map);
}

function getIcon(weight) {
    if (weight == undefined) {
        return null;
    } else if (weight >= 0 && weight < 2) {
        return "/static/image/airbnb1.jpg";
    } else if (weight >= 2 && weight < 4) {
        return "/static/image/airbnb2.jpg";
    } else if (weight >= 4 && weight < 6) {
        return "/static/image/airbnb3.jpg";
    } else if (weight >= 6 && weight < 8) {
        return "/static/image/airbnb4.jpg"
    } else if (weight >= 8 && weight < 10) {
        return "/static/image/airbnb5.jpg";
    } else {
        return null;
    }
}

function renderMarker(input) {
    let data = JSON.parse(input);
    for (i = 0; i < data.length; i++) {
        let listing = data[i];
        let latitude = parseFloat(listing.latitude);
        let longitude = parseFloat(listing.longitude);
        let weight = parseFloat(listing.weight);
        let marker = new google.maps.Marker({
            position: new google.maps.LatLng(latitude, longitude),
            //scaledSize: new google.maps.Size(2, 3),
            icon: getIcon(weight),
            map: map
        });

        google.maps.event.addListener(marker, 'click', (function(marker, i) {
            return function() {
              infoWindow.setContent(listing.name);
              infoWindow.open(map, marker);
            }
        })(marker, i));
    }
}

function parsePointAsJson(data) {
    let allPoints = JSON.parse(data);
    let points = [];
    for (let i=0; i<allPoints.length; i++) {
        let latitude = parseFloat(allPoints[i].latitude);
        let longitude = parseFloat(allPoints[i].longitude);
        let weight = parseFloat(allPoints[i].weight);
        points.push({location: new google.maps.LatLng(latitude, longitude), weight: weight});
    }
    return points;
}

function renderHeatMap(allText) {
    let points = parsePointAsJson(allText);

    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 13,
        center: {lat: 37.775, lng: -122.434},
        mapTypeId: 'satellite'
    });

    heatmap = new google.maps.visualization.HeatmapLayer({
        //dissipating: false,
        data: points,
        map: map
    });

    heatmap.set('radius', 18);
    heatmap.set('gradient', GRADIENT);
}

var GRADIENT = [
    'rgba(0, 255, 255, 0)',
    'rgba(0, 255, 255, 1)',
    'rgba(0, 191, 255, 1)',
    'rgba(0, 127, 255, 1)',
    'rgba(0, 63, 255, 1)',
    'rgba(0, 0, 255, 1)',
    'rgba(0, 0, 223, 1)',
    'rgba(0, 0, 191, 1)',
    'rgba(0, 0, 159, 1)',
    'rgba(0, 0, 127, 1)',
    'rgba(63, 0, 91, 1)',
    'rgba(127, 0, 63, 1)',
    'rgba(191, 0, 31, 1)',
    'rgba(255, 0, 0, 1)'
];

function changeGradient() {
    heatmap.set('gradient', heatmap.get('gradient') ? null : GRADIENT);
}

function changeRadius() {
    heatmap.set('radius', heatmap.get('radius') ? null : 25);
}

function changeOpacity() {
    heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
}