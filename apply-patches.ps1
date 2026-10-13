# Apply external framework patches to submodules.
# Run this after cloning / updating submodules.
# Safe to re-run — skips already-applied patches and missing submodules.

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

function Apply-Patch($submodule, $patchFile) {
    $patchName = Split-Path $patchFile -Leaf
    $submodulePath = Join-Path $root $submodule

    # An uninitialised submodule is an empty directory; git apply there would
    # silently target the parent repo and report success.
    if (-not (Test-Path (Join-Path $submodulePath ".git"))) {
        Write-Host "  Skipped: $patchName ($submodule not checked out)" -ForegroundColor DarkGray
        return
    }

    # Clones made before .gitattributes pinned patches/** to LF may still have
    # CRLF copies, which git apply rejects as corrupt.
    $bytes = [System.IO.File]::ReadAllBytes($patchFile)
    $text = [System.Text.Encoding]::UTF8.GetString($bytes)
    $tempPatch = $null
    if ($text.Contains("`r`n")) {
        $tempPatch = [System.IO.Path]::GetTempFileName()
        [System.IO.File]::WriteAllText($tempPatch, $text.Replace("`r`n", "`n"), (New-Object System.Text.UTF8Encoding $false))
        $patchFile = $tempPatch
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
        if ($tempPatch) { Remove-Item $tempPatch -ErrorAction SilentlyContinue }
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
