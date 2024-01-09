using System.Globalization;
using System.Text.Json;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nulah.BoMDataCollector.App.Models;

namespace Nulah.BoMDataCollector.App.Controllers;

internal class BoMCollector
{
	private readonly HttpClient _client;
	private readonly IOptionsMonitor<Configuration> _configuration;
	private readonly ILogger<BoMCollector> _logger;
	private readonly InfluxDBClient _influxDbClient;

	public BoMCollector(InfluxDBClient influxDbClient, IOptionsMonitor<Configuration> configuration, ILogger<BoMCollector> logger)
	{
		_influxDbClient = influxDbClient;
		_configuration = configuration;
		_logger = logger;
		_client = new HttpClient();
		_client.DefaultRequestHeaders.Add("X-Scraper", "Nulah.BoMDataCollector");
		_client.DefaultRequestHeaders.Add("X-Source", "https://github.com/ZacMillionaire/Nulah.BoMDataCollector");
		_client.DefaultRequestHeaders.Add("X-Target-Interval", "30m");
	}

	/// <summary>
	/// Retrieves data from the source defined in configuration, and sends the data to the configured InfluxDb target
	/// </summary>
	public async Task CollectData()
	{
		using var logScope = _logger.BeginScope("CollectData");
		var data = await ReadJsonFromSource(_configuration.CurrentValue.DataSource);
		if (data != null)
		{
			// Always log the copyright information from the source if present
			if (data.observations.notice.FirstOrDefault() is { } notice)
			{
				_logger.LogInformation("[{timestamp}] {header} {feedbackUrl}", DateTime.Now, notice.copyright, notice.feedback_url);
			}

			if (data.observations.header.FirstOrDefault() is { } header)
			{
				_logger.LogInformation("[{timestamp}] {productName} for {city}, {state}, {header}",
					DateTime.Now,
					header.product_name,
					header.name,
					header.state,
					header.refresh_message);
			}

			WriteToInflux(data);
			_logger.LogInformation("[{timestamp}] Collection complete", DateTime.Now);
		}
		else
		{
			_logger.LogError("[{timestamp}] No response from source", DateTime.Now);
		}
	}

	private async Task<BoMData?> ReadJsonFromSource(string source)
	{
		// TODO: add a try catch to this to account for things failing
		_logger.LogInformation("[{timestamp}] Collecting from data source: '{source}'", DateTime.Now, source);
		var req = await _client.GetStreamAsync(source);
		return JsonSerializer.Deserialize<BoMData>(req);
	}


	private void WriteToInflux(BoMData data)
	{
		_logger.LogInformation("[{timestamp}] Writing data to InfluxDb", DateTime.Now);

		using var writeApi = _influxDbClient.GetWriteApi();

		var header = data.observations.header.FirstOrDefault();

		foreach (var observation in data.observations.data)
		{
			// Gross but unlikely the format will ever change so we don't realistically need error handling here.
			// When I say unlikely to change, this format has not changed for 10 years.
			var measurementTime = DateTime.ParseExact(observation.aifstime_utc, "yyyyMMddHHmmss", CultureInfo.InvariantCulture);

			// rain trace can also be a single dash in the response, but lets treat it as if it could be any invalid decimal
			// value instead and use a TryParse.
			// A single dash is not the same as a zero value for our use case (the data may come back as 0.0 as there
			// could have been a trace amount of rain somewhere but the json is only accurate to 1 decimal place
			//http://www.bom.gov.au/catalogue/observations/about-weather-observations.shtml
			var rainTraceParsed = decimal.TryParse(observation.rain_trace, out var rainTrace);

			var point = PointData.Measurement("weather-data").Tag("source", "Bureau of Meteorology")
				.Field("air_temp", observation.air_temp)
				.Field("apparent_t", observation.apparent_t)
				// this is also known as wet bulb
				.Field("delta_t", observation.delta_t)
				.Field("rel_hum", observation.rel_hum)
				// Again, null is different to 0 contextually
				.Field("rain_trace", rainTraceParsed ? rainTrace : null)
				.Field("press", observation.press)
				.Field("press_msl", observation.press_msl)
				.Field("press_qnh", observation.press_qnh)
				.Field("wind_dir", observation.wind_dir)
				.Field("wind_spd_kmh", observation.wind_spd_kmh)
				.Field("gust_kmh", observation.gust_kmh)
				.Field("dewpt", observation.dewpt)
				.Timestamp(measurementTime, WritePrecision.S);

			// Add location Ids. The result from this is an array but appears to be a length of 1 so we check
			// that we have header details before adding them, just incase responses change in the future
			if (header != null)
			{
				point = point.Tag("ID", header.ID)
					.Tag("main_ID", header.main_ID);
			}

			// Writing individual points or batching them up and using writepoints are identical under the hood as they're
			// both batched, one is simply a convenience when you naturally build up a list of points.
			// Don't think you're clever by creating a list of PointData then using WritePoints because you're
			// creating a list for no reason.
			writeApi.WritePoint(point, _configuration.CurrentValue.InfluxDb.Bucket, _configuration.CurrentValue.InfluxDb.Organisation);
		}

		_logger.LogInformation("[{timestamp}] Data sent to InfluxDb", DateTime.Now);
	}

	#region Json Classes

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	private class BoMData
	{
		public Observations observations { get; set; }
	}

	private class Observations
	{
		public Notice[] notice { get; set; }

		public Header[] header { get; set; }

		public Data[] data { get; set; }
	}

	private class Notice
	{
		public string copyright { get; set; }

		public string copyright_url { get; set; }

		public string disclaimer_url { get; set; }

		public string feedback_url { get; set; }
	}

	private class Header
	{
		public string refresh_message { get; set; }

		public string ID { get; set; }

		public string main_ID { get; set; }

		public string name { get; set; }

		public string state_time_zone { get; set; }

		public string time_zone { get; set; }

		public string product_name { get; set; }

		public string state { get; set; }
	}

	private class Data
	{
		public int? sort_order { get; set; }

		public int? wmo { get; set; }

		public string name { get; set; }

		public string history_product { get; set; }

		public string local_date_time { get; set; }

		public string local_date_time_full { get; set; }

		public string aifstime_utc { get; set; }

		public double? lat { get; set; }

		public double? lon { get; set; }

		public double? apparent_t { get; set; }

		public string cloud { get; set; }

		public object cloud_base_m { get; set; }

		public object cloud_oktas { get; set; }

		public object cloud_type_id { get; set; }

		public string cloud_type { get; set; }

		public double? delta_t { get; set; }

		public int? gust_kmh { get; set; }

		public int? gust_kt { get; set; }

		public double? air_temp { get; set; }

		public double? dewpt { get; set; }

		public double? press { get; set; }

		public double? press_qnh { get; set; }

		public double? press_msl { get; set; }

		public string press_tend { get; set; }

		public string rain_trace { get; set; }

		public int? rel_hum { get; set; }

		public string sea_state { get; set; }

		public string swell_dir_worded { get; set; }

		public object swell_height { get; set; }

		public object swell_period { get; set; }

		public string vis_km { get; set; }

		public string weather { get; set; }

		public string wind_dir { get; set; }

		public int? wind_spd_kmh { get; set; }

		public int? wind_spd_kt { get; set; }
	}
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

	#endregion
}