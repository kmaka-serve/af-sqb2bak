param($Input)

# ---------- Проксі Request/Query для сумісності з твоїм кодом ----------
$Request         = $Input.Request
$TriggerMetadata = $Input.TriggerMetadata
if (-not $Request) {
  $Request = [pscustomobject]@{ Body=$null; Query=$null }
}

# Перехоплення Push-OutputBinding (бо activity не HTTP)
if (-not (Get-Command Push-OutputBinding -ErrorAction SilentlyContinue)) {
  function Push-OutputBinding {
    param([string]$Name,[object]$Value)
    $script:__durableResult = $Value
  }
}

# --- Збір host-логів у буфер, щоб віддати їх у HTTP-відповідь і записати у файл ---
$script:__hostLog = New-Object System.Collections.Generic.List[string]
function global:Write-Host {
  param([Parameter(ValueFromRemainingArguments = $true)] $Args)
  $msg = ($Args | ForEach-Object { "$_" }) -join ' '
  $script:__hostLog.Add($msg)
  Microsoft.PowerShell.Utility\Write-Host $msg
}

# ---------- Хелпери ----------
function ReadKey($obj, [string]$key) {
  if ($null -eq $obj) { return $null }
  try { if ($obj.ContainsKey($key)) { return $obj[$key] } } catch {}
  try { $p = $obj.PSObject.Properties[$key]; if ($p) { return $p.Value } } catch {}
  return $null
}
function Parse-IncomingInstant([string]$s) {
  if ([string]::IsNullOrWhiteSpace($s)) { return $null }
  $s = $s.Trim()
  if ($s -match '^\d{10}$') { return [datetime]::ParseExact($s,'yyyyMMddHH',[Globalization.CultureInfo]::InvariantCulture) }
  $fmt = @('yyyy-MM-ddTHH:mm:ssZ','yyyy-MM-ddTHH:mm:ss','yyyy-MM-dd HH:mm:ss','yyyy-MM-ddTHH:mm','yyyy-MM-dd HH:mm')
  foreach ($f in $fmt) { try { return [datetime]::ParseExact($s,$f,[Globalization.CultureInfo]::InvariantCulture,[Globalization.DateTimeStyles]::AssumeUniversal) } catch {} }
  try { return [datetime]::Parse($s, [Globalization.CultureInfo]::InvariantCulture) } catch { return $null }
}
function Encode-BlobPath([string]$path) {
  if ([string]::IsNullOrWhiteSpace($path)) { return $path }
  (($path -split '/') | ForEach-Object { [System.Uri]::EscapeDataString($_) }) -join '/'
}
function Normalize-Sas([string]$sas) {
  if ([string]::IsNullOrWhiteSpace($sas)) { return $null }
  # ВАЖЛИВО: без пробілу/переносу після &, інакше підпис зламається
  $s = ($sas | Out-String).Trim().Trim('"',"'") -replace '&amp;','&'
  if ($s -match '^https?://') {
    try { $u=[Uri]$s; $q=$u.Query; if (-not $q) { return $null }; if ($q[0] -ne '?'){ $q='?'+$q }; return $q } catch { return $null }
  }
  if ($s[0] -ne '?') { $s = '?' + $s }
  return $s
}
function Join-Sas([string]$baseUrl, [string]$sas) {
  if ([string]::IsNullOrWhiteSpace($sas)) { return $baseUrl }
  if ($baseUrl.Contains('?')) { return "$baseUrl&$($sas.TrimStart('?'))" }
  return "$baseUrl$($sas)"
}
function Mask-Sas([string]$s) { if (-not $s){return $s}; return ($s -replace '(sig=)[^&]+','$1***') }

# (не використовується, лишив на випадок потреби)
function Ensure-Container([string]$account,[string]$container,[string]$sas) {
  $h=[System.Net.Http.HttpClientHandler]::new()
  $c=[System.Net.Http.HttpClient]::new($h)
  try {
    $urlBase = "https://$account.blob.core.windows.net/$container?restype=container"
    $url     = Join-Sas $urlBase $sas
    $req     = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::Put, $url)
    $resp    = $c.SendAsync($req).Result
    $code    = [int]$resp.StatusCode
    Write-Host ("INFO  Ensure-Container '{0}': HTTP {1}" -f $container,$code)
    if ($code -notin 201,202,409) {
      throw ("Ensure-Container failed: {0} {1}" -f $resp.StatusCode, $resp.ReasonPhrase)
    }
  } finally { $c.Dispose(); $h.Dispose() }
}

# PUT з поверненням статусу
function Upload-FromFile([string]$url, [string]$filePath) {
  $h=[System.Net.Http.HttpClientHandler]::new()
  $c=[System.Net.Http.HttpClient]::new($h); $c.Timeout=[TimeSpan]::FromMinutes(30)

  $fs = $null; $content = $null; $r = $null
  try {
    if (-not (Test-Path -LiteralPath $filePath)) {
      throw "Local file not found: $filePath"
    }

    $fs = [IO.FileStream]::new($filePath,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::Read,81920,$true)
    $content = [System.Net.Http.StreamContent]::new($fs)
    $content.Headers.ContentType = [System.Net.Http.Headers.MediaTypeHeaderValue]::Parse("application/octet-stream")
    $content.Headers.Add("x-ms-blob-type","BlockBlob")
    # Опційно корисно додати версію API:
    $content.Headers.TryAddWithoutValidation("x-ms-version","2021-08-06") | Out-Null

    $r = $c.PutAsync($url,$content).Result
    $code = [int]$r.StatusCode
    $r.EnsureSuccessStatusCode() | Out-Null
    return $code
  } catch {
    # Розкриваємо найглибше повідомлення помилки (напр., 403/404)
    $e = $_.Exception
    while ($e.InnerException) { $e = $e.InnerException }
    throw "Upload failed: $($e.Message)"
  } finally {
    if ($r)       { $r.Dispose() }
    if ($content) { $content.Dispose() }
    if ($fs)      { $fs.Dispose() }
    $c.Dispose(); $h.Dispose()
  }
}


function Resolve-ToolsDir([string]$configured) {
  if ([string]::IsNullOrWhiteSpace($configured)) { return (Join-Path (Join-Path $env:HOME 'data') 'tools') }
  $expanded = [Environment]::ExpandEnvironmentVariables($configured)
  if ([IO.Path]::IsPathRooted($expanded)) { return $expanded }
  return (Join-Path $env:HOME $expanded)
}
function Parse-NameTS([string]$n){
  if ($n -match '_([0-9]{12})\.SQB$') { return [datetime]::ParseExact($Matches[1],'yyyyMMddHHmm',$null) }
  if ($n -match '_([0-9]{10})\.SQB$') { return [datetime]::ParseExact($Matches[1] + '00','yyyyMMddHHmm',$null) }
  return $null
}
function Get-DstBlobForFile([string]$templateBlob,[string]$srcFileName){
  if ([string]::IsNullOrWhiteSpace($templateBlob)) { return ([IO.Path]::ChangeExtension($srcFileName,'.bak')) }
  $tmpl = $templateBlob -replace '\\','/'
  $idx = $tmpl.LastIndexOf('/')
  $dir = if ($idx -ge 0) { $tmpl.Substring(0,$idx) } else { "" }
  $bakName = [IO.Path]::GetFileName(([IO.Path]::ChangeExtension($srcFileName,'.bak')))
  if ([string]::IsNullOrWhiteSpace($dir)) { return $bakName }
  return "$dir/$bakName"
}
function Get-BlobDir([string]$blobPath) {
  if ([string]::IsNullOrWhiteSpace($blobPath)) { return "" }
  $p = $blobPath -replace '\\','/'
  $i = $p.LastIndexOf('/')
  if ($i -ge 0) { return $p.Substring(0,$i) } else { return "" }
}

# --- Безопасное разрешение пути под корнем ---
function Resolve-UnderRoot([string]$root, [string]$path){
  if ([string]::IsNullOrWhiteSpace($path) -or $path -eq '.' -or $path -eq './' -or $path -eq '.\') {
    $path = $root
  } else {
    $path = [Environment]::ExpandEnvironmentVariables($path)
    if (-not [IO.Path]::IsPathRooted($path)) { $path = Join-Path $root $path }
  }
  $fullRoot   = [IO.Path]::GetFullPath($root)
  $fullTarget = [IO.Path]::GetFullPath($path)
  if ($fullTarget.Length -lt $fullRoot.Length -or -not $fullTarget.StartsWith($fullRoot, [StringComparison]::InvariantCultureIgnoreCase)) {
    throw "Target path '$fullTarget' is outside of allowed root '$fullRoot'."
  }
  return $fullTarget
}

# --- Очистка с отчётом ---
function Invoke-CleanupUnderRoot([string]$root,[string]$path,[string]$mode){
  $result = [pscustomobject]@{
    status       = $null
    mode         = $mode
    target       = $null
    root         = $root
    deletedFiles = 0
    deletedDirs  = 0
    existed      = $false
    message      = $null
  }
  try {
    $target = Resolve-UnderRoot -root $root -path $path
    $result.target = $target
    if (-not (Test-Path -LiteralPath $target)) {
      $result.status  = "not_found"
      $result.message = "Путь не найден, ничего не удалено."
      return $result
    }
    $result.existed = $true
    $item   = Get-Item -LiteralPath $target
    $isDir  = $item.PSIsContainer
    $toCount = if ($isDir) { Get-ChildItem -Path $target -Force -Recurse -ErrorAction SilentlyContinue }
               else        { Get-Item -LiteralPath $target -Force -ErrorAction SilentlyContinue }
    $result.deletedFiles = ($toCount | Where-Object { -not $_.PSIsContainer }).Count
    $result.deletedDirs  = ($toCount | Where-Object { $_.PSIsContainer }).Count

    if ($isDir) {
      if ($mode -eq 'self') {
        Remove-Item -LiteralPath $target -Recurse -Force -ErrorAction SilentlyContinue
        $result.status  = "deleted_self"
        $result.message = "Каталог удалён целиком."
      } else {
        Get-ChildItem -Path $target -Force -Recurse -ErrorAction SilentlyContinue |
          Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
        $result.status  = "deleted_contents"
        $result.message = "Содержимое каталога удалено, каталог оставлен."
      }
    } else {
      Remove-Item -LiteralPath $target -Force -ErrorAction SilentlyContinue
      $result.status  = "deleted_file"
      $result.message = "Файл удалён."
    }
  } catch {
    $result.status  = "error"
    $result.message = $_.Exception.Message
  }
  return $result
}

# ---------- HARDCODE ----------
$STORAGE_ACCOUNT_NAME = "pocfabric19082025"
$STORAGE_SAS          = "sv=2023-01-03&spr=https&st=2025-09-04T16%3A45%3A47Z&se=2025-10-31T17%3A45%3A00Z&sr=c&sp=racwdxltf&sig=f6AYXy%2FF%2B%2BRbBYHc6QgRK2N3%2BriVhsA94ZlOVc%2FlohE%3D"

$SRC_SHARE_DIR        = "C:\home\SqbFiles\BCRH"
$SRC_BLOB             = "___FULL_us_BCRH_multi_replica_2025081517.SQB"

$DST_CONTAINER        = "backups"
$DST_BLOB             = "BCRH/FULL_us_jkve_multi_replica_2025081517.bak"  # шлях через '/'

$SQL_PASSWORD         = "pZ1gW1nsdk@30(YfWjI7dTfuH@ND!Nre"
$TOOLS_SAS_URL        = ""
$TOOLS_DIR            = "C:\home\data\tools"

# --- Очистка File Share ---
$CLEANUP_ENABLED = $false
$CLEANUP_WHEN    = 'after'
$CLEANUP_PATH    = 'C:\home\SqbFiles\JKVE'
$CLEANUP_MODE    = 'contents'

# ===== validation / paths =====
$STORAGE_SAS = Normalize-Sas $STORAGE_SAS
if (-not $STORAGE_ACCOUNT_NAME -or -not $STORAGE_SAS) {
  $res = [pscustomobject]@{ StatusCode=500; Body='{"status":"error","message":"STORAGE_ACCOUNT_NAME/STORAGE_SAS не заполнены"}' }
  Push-OutputBinding -Name Response -Value $res
  return $res
}
if (-not $DST_CONTAINER) { $DST_CONTAINER = "bak-out" }
if (-not $DST_BLOB) {
  $DST_BLOB = ($SRC_BLOB -replace '\.sqb$','.bak')
  if (-not $DST_BLOB.EndsWith('.bak',[StringComparison]::InvariantCultureIgnoreCase)) { $DST_BLOB += '.bak' }
}

$home     = $env:HOME
$toolsDir = Resolve-ToolsDir $TOOLS_DIR
$srcBase  = [Environment]::ExpandEnvironmentVariables($SRC_SHARE_DIR)
if ([string]::IsNullOrWhiteSpace($srcBase)) { $srcBase = '' }
if (-not [IO.Path]::IsPathRooted($srcBase)) { $srcBase = Join-Path $home $srcBase }

# --- Очистка ДО при потребі ---
$cleanupSummary = $null
if ($CLEANUP_ENABLED -and ($CLEANUP_WHEN -eq 'before' -or $CLEANUP_WHEN -eq 'only')) {
  Write-Host ("INFO  CLEANUP [{0}] '{1}' in root '{2}'" -f $CLEANUP_MODE, $CLEANUP_PATH, $srcBase)
  $cleanupSummary = Invoke-CleanupUnderRoot -root $srcBase -path $CLEANUP_PATH -mode $CLEANUP_MODE
  if ($CLEANUP_WHEN -eq 'only') {
    $code = if ($cleanupSummary.status -eq 'error') { 400 } elseif ($cleanupSummary.status -eq 'not_found') { 404 } else { 200 }
    $res = [pscustomobject]@{ StatusCode=$code; Body=(@{ status="cleanup"; cleanup=$cleanupSummary } | ConvertTo-Json -Depth 6) }
    Push-OutputBinding -Name Response -Value $res
    if ($script:__durableResult) { return $script:__durableResult } else { return $res }
  }
}

# === ДІАПАЗОН + ОТРИМАННЯ DstParam ===
$startRaw = $Input.StartIso
$endRaw   = $Input.EndIso

# читаємо payload для можливих дублерів
$__payload = $null
try {
  if ($Request -and $Request.Body) {
    if ($Request.Body -is [string]) { $__payload = $Request.Body | ConvertFrom-Json -Depth 6 } else { $__payload = $Request.Body }
  }
} catch {}

if (-not $startRaw) { $startRaw = ReadKey $__payload 'Start' }
if (-not $endRaw)   { $endRaw   = ReadKey $__payload 'End' }

# <<< NEW: отримуємо DstParam з кількох місць >>>
$dstParam = $Input.DstParam
if (-not $dstParam) { $dstParam = ReadKey $__payload 'DstParam' }
if (-not $dstParam) { $dstParam = ReadKey $Request.Query 'DstParam' }
Write-Host ("INFO  DstParam: {0}" -f ($(if ($dstParam) { $dstParam } else { '<null>' })))

if (-not $startRaw) { $startRaw = ReadKey $Request.Query 'Start' }
if (-not $endRaw)   { $endRaw   = ReadKey $Request.Query 'End' }

$__StartTs = Parse-IncomingInstant $startRaw
$__EndTs   = Parse-IncomingInstant $endRaw
$__rangeGiven = [bool]($startRaw -or $endRaw)
Write-Host ("DEBUG Range raw: Start='{0}' End='{1}'" -f $startRaw,$endRaw)
Write-Host ("DEBUG Range dt : Start={0:yyyy-MM-dd HH:mm:ss} End={1:yyyy-MM-dd HH:mm:ss}" -f $__StartTs,$__EndTs)

# === Пошук файлів ===
$FILES_TO_PROCESS = @()
try {
  $all = Get-ChildItem -Path $srcBase -File -Filter '*.SQB'
  if ($__rangeGiven) {
    $entries = foreach ($f in $all) {
      $ts = Parse-NameTS $f.Name
      if ($ts) { [pscustomobject]@{ File=$f; TS=$ts } }
    }
    if ($__StartTs) { $entries = $entries | Where-Object { $_.TS -ge $__StartTs } }
    if ($__EndTs)   { $entries = $entries | Where-Object { $_.TS -le $__EndTs } }
    $FILES_TO_PROCESS = $entries | Sort-Object TS | Select-Object -ExpandProperty File
  } else {
    $FILES_TO_PROCESS = $all | Sort-Object Name
  }
} catch { $FILES_TO_PROCESS = @() }

Write-Host ("INFO  SRC dir: {0}" -f $srcBase)
Write-Host ("INFO  Total .SQB: {0}" -f ($(Get-ChildItem -Path $srcBase -File -Filter '*.SQB' -ErrorAction SilentlyContinue).Count))
Write-Host ("INFO  After filter: {0}" -f $FILES_TO_PROCESS.Count)
$FILES_TO_PROCESS | Select-Object -First 20 | ForEach-Object { Write-Host ("INFO  -> {0}" -f $_.Name) }

if (-not $FILES_TO_PROCESS -or $FILES_TO_PROCESS.Count -eq 0) {
  if ($__rangeGiven) {
    $msg = @{ status="empty"; message="SQB-файли за заданим діапазоном (по імені) не знайдені"; start=$__StartTs; end=$__EndTs; srcDir=$srcBase } | ConvertTo-Json
    $res = [pscustomobject]@{ StatusCode=404; Body=$msg }
    Push-OutputBinding -Name Response -Value $res
    if ($script:__durableResult) { return $script:__durableResult } else { return $res }
  }
  $shareInput = Join-Path $srcBase $SRC_BLOB
  if (-not (Test-Path $shareInput)) {
    $res = [pscustomobject]@{ StatusCode=404; Body=(@{status="error"; message=("Входной SQB не найден: " + $shareInput)} | ConvertTo-Json) }
    Push-OutputBinding -Name Response -Value $res
    if ($script:__durableResult) { return $script:__durableResult } else { return $res }
  }
  $FILES_TO_PROCESS = @([IO.FileInfo](Get-Item $shareInput))
} else {
  $shareInput = $FILES_TO_PROCESS[0].FullName
}

# === Робочі каталоги ===
$localRoot = if (Test-Path 'C:\home\site') { 'C:\home\site' } else { $env:TEMP }
$workDir   = Join-Path $localRoot ("sqb2bak_" + [guid]::NewGuid())
New-Item -ItemType Directory -Force -Path $workDir | Out-Null
Write-Host ("INFO  workDir: {0}" -f $workDir)

$toolsDirResolved = Resolve-ToolsDir $TOOLS_DIR
New-Item -ItemType Directory -Force -Path $toolsDirResolved | Out-Null

$converter = Join-Path $toolsDirResolved "SQBConverter.exe"
if (-not (Test-Path $converter)) {
  if ($TOOLS_SAS_URL) {
    Write-Host "INFO  Скачиваю SQBConverter.exe из TOOLS_SAS_URL..."
    $wc = New-Object System.Net.WebClient
    try { $wc.DownloadFile($TOOLS_SAS_URL, $converter) } finally { $wc.Dispose() }
  } else {
    $res = [pscustomobject]@{ StatusCode=500; Body='{"status":"error","message":"SQBConverter.exe не найден. Покладiть у $HOME\\data\\tools або вкажiть TOOLS_SAS_URL"}' }
    Push-OutputBinding -Name Response -Value $res
    if ($script:__durableResult) { return $script:__durableResult } else { return $res }
  }
}
try { Unblock-File -Path $converter -ErrorAction SilentlyContinue } catch {}
Write-Host ("INFO  Tools dir:  {0}" -f $toolsDirResolved)
Write-Host ("DEBUG SAS: " + (Mask-Sas $STORAGE_SAS))

#Ensure-Container -account $STORAGE_ACCOUNT_NAME -container $DST_CONTAINER -sas $STORAGE_SAS   # (не потрібно, контейнер існує)

# === Конвертація + аплоад ===
$results = @()
$lastOutLocal = $null
$lastBlobForFile = $null

foreach ($fi in $FILES_TO_PROCESS) {
  $shareInput = $fi.FullName
  $outLocal   = Join-Path $workDir ([IO.Path]::ChangeExtension($fi.Name, ".bak"))
  $lastOutLocal = $outLocal

  Write-Host ("INFO  Input SQB:  {0}" -f $shareInput)
  Write-Host ("INFO  Output BAK: {0}" -f $outLocal)

  Write-Host "INFO  Конвертирую SQB -> BAK..."
  $pw  = if ($SQL_PASSWORD) { $SQL_PASSWORD } elseif ($env:SQB_PASSWORD) { $env:SQB_PASSWORD } else { "" }
  $cmdArgs = @($shareInput, $outLocal) + ($(if ($pw){ @($pw) } else { @() }))

  try { Unblock-File -Path $converter -ErrorAction SilentlyContinue } catch {}
  Write-Host ('DEBUG SQBConverter: "{0}" {1}' -f $converter, ($cmdArgs | ForEach-Object { '"' + $_ + '"' } -join ' '))
  $convOut = & $converter @cmdArgs 2>&1
  $exit    = $LASTEXITCODE

  Write-Host ("INFO  SQBConverter exit: {0}" -f $exit)
  $convOut | ForEach-Object { Write-Host ("SQBConverter> {0}" -f $_) }

  if ($exit -ne 0 -or -not (Test-Path $outLocal) -or ((Get-Item $outLocal).Length -le 0)) {
    # спробуємо підказати, якщо проблема у паролі/шифруванні
    $hint = $null
    $joined = ($convOut | Out-String)
    if ($joined -match '(?i)password|passphrase|decrypt|encrypted') {
      $hint = "Можливо, невiрний пароль (SQL_PASSWORD/SQB_PASSWORD) або backup зашифровано iнакше."
    }
    $results += [pscustomobject]@{
      name     = $fi.Name; status="error"; message=("Конвертація не вдалася. " + ($hint ?? "")); exitCode=$exit; log=($convOut | Select-Object -First 50)
    }
    try { Remove-Item -Path $outLocal -Force -ErrorAction SilentlyContinue } catch {}
    continue
  }

  $dstBlobForFile = Get-DstBlobForFile -templateBlob $DST_BLOB -srcFileName $fi.Name
  $lastBlobForFile = $dstBlobForFile

  $dstUrlBase = "https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/$DST_CONTAINER/$(Encode-BlobPath $dstBlobForFile)"
  $dstUrl     = Join-Sas $dstUrlBase $STORAGE_SAS

  try {
    Write-Host ("INFO  PUT Blob: " + (Mask-Sas $dstUrl))
    $code = Upload-FromFile $dstUrl $outLocal
    Write-Host ("INFO  Upload OK: HTTP {0}" -f $code)
  } catch {
    $results += [pscustomobject]@{ name=$fi.Name; status="error"; message=("Не вдалося завантажити .бак. " + $_) }
    try { Remove-Item -Path $outLocal -Force -ErrorAction SilentlyContinue } catch {}
    continue
  }

  # опційна повторна спроба
  try {
    Write-Host ("INFO  Retry PUT Blob: " + (Mask-Sas $dstUrl))
    $code2 = Upload-FromFile $dstUrl $outLocal
    Write-Host ("INFO  Retry Upload OK: HTTP {0}" -f $code2)
  } catch { Write-Host ("WARN  Повторний аплоад не вдався: " + $_) }

  $results += [pscustomobject]@{
    name=$fi.Name; status="ok"; container=$DST_CONTAINER; blob=$dstBlobForFile;
    url = "https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/$DST_CONTAINER/$dstBlobForFile";
    localPath=$outLocal
  }

  try { Remove-Item -Path $outLocal -Force -ErrorAction SilentlyContinue } catch {}
}

# --- Запис і вивантаження RUN-ЛОГА у ту ж директорію, що і .bak ---
$logUpload = $null
try {
  $ts          = Get-Date -Format 'yyyyMMddHHmmss'
  $logName     = "sqb2bak_$ts.log"
  $logLocal    = Join-Path $workDir $logName

  # директорія для лога: з останнього успішного/розрахованого blob або з шаблону DST_BLOB
  $targetBase  = ($lastBlobForFile ?? $DST_BLOB)
  $dstLogDir   = Get-BlobDir $targetBase
  $logBlobPath = if ($dstLogDir) { "$dstLogDir/$logName" } else { $logName }

  # формуємо вміст лога (plain text)
  $header = @(
    "=== sqb2bak run ===",
    "UTC: $(Get-Date -AsUTC -Format s)Z",
    "Container: $DST_CONTAINER",
    "TemplateBlob: $DST_BLOB",
    "DstParam: $dstParam",            # <<< NEW: пишемо DstParam у лог-файл
    "SourceDir: $srcBase",
    "FilesFound: $($FILES_TO_PROCESS.Count)"
  ) -join [Environment]::NewLine

  $resultsJson = ($results | ConvertTo-Json -Depth 6)
  $hostJoined  = ($script:__hostLog -join [Environment]::NewLine)

  $logText = $header + [Environment]::NewLine + "-- Results --" + [Environment]::NewLine +
             $resultsJson + [Environment]::NewLine + "-- Host log --" + [Environment]::NewLine +
             $hostJoined + [Environment]::NewLine

  # Запис локально
  Set-Content -Path $logLocal -Value $logText -Encoding UTF8

  # Завантаження у Blob
  $logUrlBase = "https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/$DST_CONTAINER/$(Encode-BlobPath $logBlobPath)"
  $logUrl     = Join-Sas $logUrlBase $STORAGE_SAS

  Write-Host ("INFO  PUT Log Blob: " + (Mask-Sas $logUrl))
  $http = Upload-FromFile $logUrl $logLocal
  Write-Host ("INFO  Log Upload OK: HTTP {0}" -f $http)

  $logUpload = @{
    status    = "ok"
    container = $DST_CONTAINER
    blob      = $logBlobPath
    url       = "https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/$DST_CONTAINER/$logBlobPath"
    http      = $http
  }
} catch {
  Write-Host ("WARN  Log upload failed: " + $_)
  $logUpload = @{
    status    = "error"
    message   = "$_"
  }
}

# --- Очистка ПІСЛЯ ---
if ($CLEANUP_ENABLED -and $CLEANUP_WHEN -eq 'after') {
  $hadSuccess = ($results | Where-Object {$_.status -eq 'ok'}).Count -gt 0
  if ($hadSuccess) {
    Write-Host ("INFO  CLEANUP-AFTER [{0}] '{1}' in root '{2}'" -f $CLEANUP_MODE, $CLEANUP_PATH, $srcBase)
    $cleanupSummary = Invoke-CleanupUnderRoot -root $srcBase -path $CLEANUP_PATH -mode $CLEANUP_MODE
  } else {
    Write-Host "INFO  CLEANUP-AFTER: skipped (no successful uploads)."
  }
}

# -- ліміт на обсяг логів у відповіді --
$maxLogLines = 800
$hostLogOut = if ($script:__hostLog.Count -gt $maxLogLines) {
  $script:__hostLog | Select-Object -Last $maxLogLines
} else {
  $script:__hostLog
}

$res = [pscustomobject]@{
  StatusCode = 200
  Body = (@{
    status = if (($results | Where-Object {$_.status -ne 'ok'}).Count -gt 0) {
               if (($results | Where-Object {$_.status -eq 'ok'}).Count -gt 0) { "partial" } else { "error" }
             } else { "ok" }
    input  = @{ shareDir=$SRC_SHARE_DIR; file=$SRC_BLOB; path=$shareInput }
    output = @{
      container = $DST_CONTAINER
      blob      = ($lastBlobForFile ?? $DST_BLOB)
      url       = "https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/$DST_CONTAINER/$($lastBlobForFile ?? $DST_BLOB)"
      localPath = ($lastOutLocal ?? "")
    }
    cleanup = $cleanupSummary
    results = $results

    # Результат завантаження лога
    log = $logUpload

    # Логи хоста у відповіді
    logs = @{
      host      = $hostLogOut
      totalHost = $script:__hostLog.Count
      truncated = ($script:__hostLog.Count -gt $maxLogLines)
    }
  } | ConvertTo-Json -Depth 6)
}
Push-OutputBinding -Name Response -Value $res

try { Remove-Item -Path $workDir -Recurse -Force -ErrorAction SilentlyContinue } catch {}

if ($script:__durableResult) { return $script:__durableResult } else { return $res }
