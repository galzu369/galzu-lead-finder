# Script to create GitHub repository via API
# Requires a GitHub Personal Access Token (PAT)
# Get one at: https://github.com/settings/tokens
# Usage: .\create-repo.ps1 -GitHubToken YOUR_TOKEN
# Or: $token = "YOUR_TOKEN"; .\create-repo.ps1 -GitHubToken $token

param(
    [Parameter(Mandatory=$false)]
    [string]$GitHubToken
)

# If token not provided, try to get from environment or prompt
if (-not $GitHubToken) {
    $GitHubToken = $env:GITHUB_TOKEN
    if (-not $GitHubToken) {
        Write-Host "GitHub Personal Access Token required!" -ForegroundColor Yellow
        Write-Host "Get one at: https://github.com/settings/tokens" -ForegroundColor Cyan
        Write-Host "Required scopes: repo" -ForegroundColor Cyan
        $GitHubToken = Read-Host "Enter your GitHub token" -AsSecureString
        $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($GitHubToken)
        $GitHubToken = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
    }
}

# Try both token formats (classic tokens use "token", fine-grained use "Bearer")
$headers1 = @{
    Authorization = "token $GitHubToken"
    Accept = "application/vnd.github.v3+json"
}

$headers2 = @{
    Authorization = "Bearer $GitHubToken"
    Accept = "application/vnd.github.v3+json"
}

$body = @{
    name = "galzu-lead-finder"
    description = "Galzu Lead Finder dashboard with Google Maps scraper and email enrichment"
    private = $false
    auto_init = $false
} | ConvertTo-Json

# First, verify token is valid
Write-Host "Verifying GitHub token..." -ForegroundColor Cyan
try {
    $testHeaders = @{
        Authorization = "Bearer $GitHubToken"
        Accept = "application/vnd.github.v3+json"
    }
    $user = Invoke-RestMethod -Uri "https://api.github.com/user" -Headers $testHeaders
    Write-Host "Token valid! Authenticated as: $($user.login)" -ForegroundColor Green
} catch {
    Write-Host "Token verification failed. Trying alternative format..." -ForegroundColor Yellow
    try {
        $testHeaders = @{
            Authorization = "token $GitHubToken"
            Accept = "application/vnd.github.v3+json"
        }
        $user = Invoke-RestMethod -Uri "https://api.github.com/user" -Headers $testHeaders
        Write-Host "Token valid! Authenticated as: $($user.login)" -ForegroundColor Green
    } catch {
        Write-Host "Error: Invalid token or insufficient permissions" -ForegroundColor Red
        Write-Host "Make sure your token has 'repo' scope (or 'Contents: Read and write' for fine-grained tokens)" -ForegroundColor Yellow
        exit 1
    }
}

# Try to create repository
try {
    Write-Host "`nCreating repository 'galzu-lead-finder' on GitHub..." -ForegroundColor Cyan
    $response = $null
    $errorDetails = $null
    
    # Try Bearer first (for fine-grained tokens)
    try {
        $response = Invoke-RestMethod -Uri "https://api.github.com/user/repos" `
            -Method Post `
            -Body $body `
            -Headers $headers2 `
            -ContentType "application/json"
    } catch {
        $errorDetails = $_
        # Try token format (for classic tokens)
        try {
            $response = Invoke-RestMethod -Uri "https://api.github.com/user/repos" `
                -Method Post `
                -Body $body `
                -Headers $headers1 `
                -ContentType "application/json"
        } catch {
            throw $_
        }
    }
    
    Write-Host "Repository created successfully!" -ForegroundColor Green
    Write-Host "Repository URL: $($response.html_url)" -ForegroundColor Green
    
    # Add remote and push
    Write-Host "`nAdding remote and pushing code..." -ForegroundColor Cyan
    git remote remove origin 2>$null
    git remote add origin $response.clone_url
    git branch -M main
    git push -u origin main
    
    Write-Host "`nDone! Repository is ready at: $($response.html_url)" -ForegroundColor Green
} catch {
    Write-Host "`nError: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.Exception.Response) {
        $statusCode = [int]$_.Exception.Response.StatusCode
        $statusDescription = $_.Exception.Response.StatusDescription
        Write-Host "Status: $statusCode $statusDescription" -ForegroundColor Red
        
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $responseBody = $reader.ReadToEnd()
            $reader.Close()
            Write-Host "Response: $responseBody" -ForegroundColor Red
            
            # Try to parse JSON error message
            try {
                $errorJson = $responseBody | ConvertFrom-Json
                if ($errorJson.message) {
                    Write-Host "`nGitHub says: $($errorJson.message)" -ForegroundColor Yellow
                }
                if ($errorJson.errors) {
                    foreach ($err in $errorJson.errors) {
                        Write-Host "  - $($err.message)" -ForegroundColor Yellow
                    }
                }
            } catch {
                # Not JSON, show raw response
            }
        } catch {
            Write-Host "Could not read error response" -ForegroundColor Red
        }
    }
    
    Write-Host "`nTroubleshooting:" -ForegroundColor Yellow
    Write-Host "1. Verify token has 'repo' scope (classic) or 'Contents: Read and write' (fine-grained)" -ForegroundColor Cyan
    Write-Host "2. For fine-grained tokens, ensure 'galzu-lead-finder' repository is selected" -ForegroundColor Cyan
    Write-Host "3. Token might be expired - create a new one at: https://github.com/settings/tokens" -ForegroundColor Cyan
    
    exit 1
}
