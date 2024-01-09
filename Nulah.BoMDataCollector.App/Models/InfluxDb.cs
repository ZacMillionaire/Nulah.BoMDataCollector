namespace Nulah.BoMDataCollector.App.Models;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

internal class InfluxDb
{
	public string Token { get; set; }
	public string Url { get; set; }
	public string Bucket { get; set; }
	public string Organisation { get; set; }
}

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.