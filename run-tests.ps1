# Test Runner Script
# Activates the virtual environment and runs pytest based on user selection

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "         Test Runner" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Which tests would you like to run?" -ForegroundColor Yellow
Write-Host "  [1] Integration tests" -ForegroundColor White
Write-Host "  [2] Unit tests" -ForegroundColor White
Write-Host "  [3] Both" -ForegroundColor White
Write-Host ""

$choice = Read-Host "Enter your choice (1, 2, or 3)"

switch ($choice)
{
	"1"
	{
		Write-Host ""
		Write-Host "Running integration tests..." -ForegroundColor Green
		.\.venv\Scripts\activate
		pytest --html=report.html .\tests\integration_testing\
		start .\report.html
	}
	"2"
	{
		Write-Host ""
		Write-Host "Running unit tests..." -ForegroundColor Green
		.\.venv\Scripts\activate
		pytest --html=report.html .\tests\unit_testing\
		start .\report.html
	}
	"3"
	{
		Write-Host ""
		Write-Host "Running all tests..." -ForegroundColor Green
		.\.venv\Scripts\activate
		pytest --html=report.html .\tests\integration_testing\ .\tests\unit_testing\
		start .\report.html
	}
	default
	{
		Write-Host ""
		Write-Host "Invalid choice '$choice'. Please enter 1, 2, or 3." -ForegroundColor Red
		exit 1
	}
}
