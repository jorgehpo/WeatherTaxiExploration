"use strict";
$( document ).ready(initMap);

var map;
var group = {};
var heatmap;
var heatmapData = new google.maps.MVCArray();

var dimension = {};
var data, parameters;

function createFilterFunction(input){
    var query = function(x){
        var selected = input.val();
        if (selected == null){
            return true;
        }else{
            for (var i = 0; i < selected.length; i++){
                if (x == selected[i])
                    return true;
            }
            return false;
        }
    }
    return query;
}


function initMap() {
    $(".selectpicker").selectpicker();

    $("#rangeRadius").slider({
        formatter: function(value) {
            return 'Current value: ' + value;
        }
    });

    map = new google.maps.Map(document.getElementById("map"), {
        center: {lat: 40.7128, lng: -74.0059},
        zoom: 12,
        mapTypeControlOptions: {
          mapTypeIds: [],
          disableDefaultUI: true
        },
        styles:[{"featureType":"administrative","elementType":"labels.text.fill","stylers":[{"color":"#444444"}]},{"featureType":"landscape","elementType":"all","stylers":[{"color":"#f2f2f2"}]},{"featureType":"poi","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"road","elementType":"all","stylers":[{"saturation":-100},{"lightness":45}]},{"featureType":"road.highway","elementType":"all","stylers":[{"visibility":"simplified"}]},{"featureType":"road.arterial","elementType":"labels.icon","stylers":[{"visibility":"off"}]},{"featureType":"transit","elementType":"all","stylers":[{"visibility":"off"}]},{"featureType":"water","elementType":"all","stylers":[{"color":"#3182bd"},{"visibility":"on"}]}]
    });

    heatmap = new google.maps.visualization.HeatmapLayer({
        data: heatmapData,
        radius:14
    });
    heatmap.setMap(map);

    $("#rangeRadius").slider().on("slideStop",function(x){
        heatmap.setOptions({radius:+$("#rangeRadius").val()});
    });

    $("#txtReport").text("0 taxi trips selected.");

    $("#queryBtn").click(function(){
        var total = 0
        dimension["typeOp"].filter( $('input[name=typeOp]:checked').val() );

        
        var start_latitude = parameters["min_latitude"] + parameters["size_bin_latitude"]/2;
        var start_longitude = parameters["min_longitude"] + parameters["size_bin_longitude"]/2;
        var filtered_data = dimension["typeOp"].top(Infinity);

        var nbins_latitude = parameters["nbins_latitude"];

        var nbins_longitude = parameters["nbins_longitude"];

        var size_bin_latitude = parameters["size_bin_latitude"];
        var size_bin_longitude = parameters["size_bin_longitude"];
        var heatmapAccumm = new Int32Array(nbins_latitude * nbins_longitude);

        heatmapData.clear();

        filtered_data.forEach(function(d){
            for (var i = 0; i < d["heatmap"].length; i++){
                var bin = d["heatmap"][i];
                var latitude =  bin[0];
                var longitude = bin[1];
                // latitude: i
                // longitude: j
                heatmapAccumm[latitude*nbins_longitude + longitude] += bin[2];
                total += bin[2];
            }
        });

        console.log(total)

        $("#txtReport").text(numeral(total).format("0,0") + " taxi trips selected.");

        for (var latitude = 0; latitude < nbins_latitude; latitude++){
            for (var longitude = 0; longitude < nbins_longitude; longitude++){
                if (heatmapAccumm[latitude*nbins_longitude + longitude] > 0){
                    heatmapData.push({location: new google.maps.LatLng(start_latitude + size_bin_latitude *latitude, start_longitude + size_bin_longitude * longitude), weight: heatmapAccumm[latitude*nbins_longitude + longitude]});
                }
            }                        
        }

        if (heatmapData.length > 0){
            heatmap.setMap(map);
        }else{
            heatmap.setMap();
        }

    } );


    d3.json("taxi_weather_queries.json",function(data_json){

        parameters = data_json["parameters"];
        data = crossfilter(data_json["queries"])

        dimension["typeOp"] = data.dimension(function(fact){return fact["typeOp"]});
        dimension["weekday"] = data.dimension(function(fact){return fact["weekday"]});
        dimension["month"] = data.dimension(function(fact){return fact["month"]});
        dimension["hour"] = data.dimension(function(fact){return fact["hour"]});
        dimension["weather"] = data.dimension(function(fact){return fact["weather"]});
        dimension["temperature"] = data.dimension(function(fact){return fact["temperature"]});


        group["month"] = dimension["month"].group();
        group["weekday"] = dimension["weekday"].group();
        group["hour"] = dimension["hour"].group();
        group["temperature"] = dimension["temperature"].group();
        group["weather"] = dimension["weather"].group();
        group["typeOp"] = dimension["typeOp"].group();


        var monthChart = dc.rowChart('#monthChart');
        monthChart.width(180)
            .elasticX(true)
            .height(12*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["month"])
            .dimension(dimension["month"])
            .label(function (d){
                return ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"][d.key-1];
            });
        monthChart.colors(d3.scale.ordinal().range(['#3182bd']));
        monthChart.render();
                
        var dayOfWeekChart = dc.rowChart('#weekdayChart');
        dayOfWeekChart.width(180)
            .elasticX(true)
            .height(7*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["weekday"])
            .dimension(dimension["weekday"])
            .label(function (d){
                return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][d.key]
            }); 
        dayOfWeekChart.colors(d3.scale.ordinal().range(['#3182bd']));
        dayOfWeekChart.render();


        var hourChart = dc.rowChart('#hourChart');
        hourChart.width(180)
            .elasticX(true)
            .height(6*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["hour"])
            .dimension(dimension["hour"])
            .label(function (d){
                return ["0am - 4am","4am - 8am","8am - 12am","12pm - 4pm","4pm - 8pm","8pm - 12pm"][d.key]
            }); 
        hourChart.colors(d3.scale.ordinal().range(['#3182bd']));
        hourChart.render();


        var tempChart = dc.rowChart('#tempChart');
        tempChart.width(180)
            .elasticX(true)
            .height(5*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["temperature"])
            .dimension(dimension["temperature"])
            .label(function (d){
                return ["-20° F - 0° F","0° F - 20° F","20° F - 40° F","40° F - 60° F","60° F - 80° F","80° F - 100° F"][d.key]
            }); 
        tempChart.colors(d3.scale.ordinal().range(['#3182bd']));
        tempChart.render();


       var weatherChart = dc.rowChart('#weatherChart');
        weatherChart.width(180)
            .elasticX(true)
            .height(5*25+20)
            .margins({top: 0, left: 10, right: 0, bottom: 20})
            .group(group["weather"])
            .dimension(dimension["weather"])
            .label(function (d){
                return {"NoPrecipitation":"No Precipitation", "Drizzle":"Drizzle", "Rain":"Rain", "SolidPrecipitation":"Solid Precipitation", "ShoweryPrecipitation":"Showery Precipitation"}[d.key]
            }); 
        weatherChart.colors(d3.scale.ordinal().range(['#3182bd']));
        weatherChart.render();

        console.log(weatherChart)
      });
}//initmap
