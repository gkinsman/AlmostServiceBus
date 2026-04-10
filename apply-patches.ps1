# Apply external framework patches to submodules.
# Run this after cloning / updating submodules.
# Safe to re-run — skips already-applied patches and missing submodules.

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

function Apply-Patch($submodule, $patchFile) {
    $patchName = Split-Path $patchFile -Leaf
    $submodulePath = Join-Path $root $submodule

    if (-not (Test-Path $submodulePath)) {
        Write-Host "  Skipped: $patchName ($submodule not checked out)" -ForegroundColor DarkGray
        return
    }

    Push-Location $submodulePath
    try {
        # Check if patch applies cleanly
        $result = git apply --check --ignore-whitespace $patchFile 2>&1
        if ($LASTEXITCODE -eq 0) {
            git apply --ignore-whitespace $patchFile
            Write-Host "  Applied: $patchName" -ForegroundColor Green
        } else {
            # Already applied or conflicts — try reverse check
            $reverse = git apply --check --reverse --ignore-whitespace $patchFile 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  Skipped: $patchName (already applied)" -ForegroundColor Yellow
            } else {
                Write-Host "  FAILED:  $patchName — manual resolution needed" -ForegroundColor Red
                Write-Host "           $result" -ForegroundColor DarkRed
            }
        }
    } finally {
        Pop-Location
    }
}

foreach ($framework in Get-ChildItem "$root/patches" -Directory | Sort-Object Name) {
    $submodule = "external/$($framework.Name)"
    Write-Host "Applying $($framework.Name) patches..." -ForegroundColor Cyan
    foreach ($patch in Get-ChildItem "$($framework.FullName)/*.patch" | Sort-Object Name) {
        Apply-Patch $submodule $patch.FullName
    }
}

Write-Host "Done." -ForegroundColor Green
