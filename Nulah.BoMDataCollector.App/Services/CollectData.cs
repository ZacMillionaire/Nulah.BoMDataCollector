using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nulah.BoMDataCollector.App.Controllers;
using Nulah.BoMDataCollector.App.Models;

namespace Nulah.BoMDataCollector.App.Services;

internal class CollectData : IHostedService, IDisposable
{
	private readonly BoMCollector _collector;
	private readonly ILogger<CollectData> _logger;
	private Timer? _timer = null;
	private readonly IOptionsMonitor<Configuration> _configuration;

	public CollectData(BoMCollector collector, ILogger<CollectData> logger, IOptionsMonitor<Configuration> configuration)
	{
		_collector = collector;
		_logger = logger;
		_configuration = configuration;
	}

	public Task StartAsync(CancellationToken stoppingToken)
	{
		_logger.LogInformation("Collect Data Service running.");

		_timer = new Timer(Collect, null, TimeSpan.Zero,
			_configuration.CurrentValue.Repeat);


		return Task.CompletedTask;
	}

	public Task StopAsync(CancellationToken stoppingToken)
	{
		_logger.LogInformation("Collect Data Service is stopping.");

		return Task.CompletedTask;
	}

	public void Dispose()
	{
		_timer?.Dispose();
	}

	private async void Collect(object? state)
	{
		await _collector.CollectData();
	}
}