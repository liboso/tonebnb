let map, heatmap, infoWindow, safetyMarkers, safetyInfoWindow;

function resetMarkers(markers) {
    if (markers !== undefined && Array.isArray(markers)) {
        markers.forEach(marker => marker.setMap(null))
    }
}

function initMap() {
    $.get("/api/heatmap/sf").then(data => {
        infoWindow = new google.maps.InfoWindow();
        safetyInfoWindow = new google.maps.InfoWindow();
        renderHeatMap(data);
        return $.get("/api/listings/sf");
    }).then(data => {
        renderAirbnbMarkers(data);
    });
}

function getIcon(weight) {
    if (weight == undefined) {
        return "/static/image/airbnb1.jpg";
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
        return "/static/image/airbnb1.jpg";
    }
}

function getInfoIcon(type) {
    if (type == 1) {
        return "/static/image/info_1.jpg";
    } else  {
        return "/static/image/info_2.jpg";
    }
}

function renderAirbnbMarkers(data) {
    for (i = 0; i < data.length; i++) {
        let listing = data[i];
        let marker = new google.maps.Marker({
            position: new google.maps.LatLng(listing.latitude, listing.longitude),
            icon: getIcon(listing.score),
            map: map
        });

        google.maps.event.addListener(marker, 'click', (function(marker, i) {
            return function() {
              infoWindow.setContent(listing.name);
              infoWindow.open(map, marker);
              renderSafetyMarkers(marker.position.lat(), marker.position.lng())
            }
        })(marker, i));
    }
}

function renderSafetyMarkers(latitude, longitude) {
    resetMarkers(safetyMarkers);
    safetyMarkers = [];
    $.get("/api/safetyinfo", {latitude: latitude, longitude: longitude}).done((data) => {
        safetyMarkers = data.map(item => {
            let marker = new google.maps.Marker({
                position: new google.maps.LatLng(item.latitude, item.longitude),
                icon: "/static/image/info_3.jpg",
                map: map
            });
            marker.addListener('mouseover', function() {
                let content = `
                    <div>
                        <p><b>Incident Report</b></p>
                        <div><b>Occur Date: </b>${item.date}</div>
                        <div><b>Description: </b>${item.description}</div>
                    </div>
                `;
                safetyInfoWindow.setContent(content)
                safetyInfoWindow.open(map, this);
            });
            marker.addListener('mouseout', function() {
                safetyInfoWindow.close();
            });
            return marker;
        });
    });
}

function renderHeatMap(data) {
    let points = data.map(point => {
        return { location: new google.maps.LatLng(point.latitude, point.longitude), weight: point.weight };
    });

    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 13,
        center: {lat: 37.775, lng: -122.434},
        mapTypeId: 'satellite'
    });

    heatmap = new google.maps.visualization.HeatmapLayer({
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
