namespace Nulah.BoMDataCollector.App.Models;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

internal class Configuration
{
	public string DataSource { get; set; }
	public TimeSpan Repeat { get; set; }
	public InfluxDb InfluxDb { get; set; }
}

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.