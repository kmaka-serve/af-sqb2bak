using namespace System.Net
param($Request, $TriggerMetadata)

function GetParam([string]$name) {
  try { if ($Request.Query -and $Request.Query.ContainsKey($name)) { return $Request.Query[$name] } } catch {}
  try { if ($Request.Params -and $Request.Params.ContainsKey($name)) { return $Request.Params[$name] } } catch {}
  try {
    if ($Request.RawBody) {
      $rb = $Request.RawBody | ConvertFrom-Json
      if ($rb.PSObject.Properties[$name]) { return $rb.$name }
    }
  } catch {}
  try {
    if ($Request.Body) {
      if ($Request.Body -is [string]) {
        $b = $Request.Body | ConvertFrom-Json
        if ($b.PSObject.Properties[$name]) { return $b.$name }
      } elseif ($Request.Body -is [System.Collections.IDictionary]) {
        if ($Request.Body.Contains($name)) { return $Request.Body[$name] }
      } else {
        if ($Request.Body.PSObject.Properties[$name]) { return $Request.Body.$name }
      }
    }
  } catch {}
  try { if ($TriggerMetadata -and $TriggerMetadata.PSObject.Properties[$name]) { return $TriggerMetadata.$name } } catch {}
  return $null
}

function ToWinStart([string]$s){
  if (-not $s) { return $null }
  if ($s -match '^\d{12}$') { return '{0}-{1}-{2} {3}:{4}:00' -f $s.Substring(0,4),$s.Substring(4,2),$s.Substring(6,2),$s.Substring(8,2),$s.Substring(10,2) }
  if ($s -match '^\d{10}$') { return '{0}-{1}-{2} {3}:00:00'  -f $s.Substring(0,4),$s.Substring(4,2),$s.Substring(6,2),$s.Substring(8,2) }
  return ($s -replace 'T',' ' -replace 'Z','')
}
function ToWinEnd([string]$s){
  if (-not $s) { return $null }
  if ($s -match '^\d{12}$') { return '{0}-{1}-{2} {3}:{4}:59' -f $s.Substring(0,4),$s.Substring(4,2),$s.Substring(6,2),$s.Substring(8,2),$s.Substring(10,2) }
  if ($s -match '^\d{10}$') { return '{0}-{1}-{2} {3}:59:59'  -f $s.Substring(0,4),$s.Substring(4,2),$s.Substring(6,2),$s.Substring(8,2) }
  return ($s -replace 'T',' ' -replace 'Z','')
}

# Беремо з усіх можливих місць
$startRaw = GetParam 'Start'
$endRaw   = GetParam 'End'
$dstParam = GetParam 'DstParam'

# Дефолти — лише якщо нічого не прийшло
if (-not $startRaw) { $startRaw = (Get-Date -AsUTC).ToString('yyyyMMddHH') }
if (-not $endRaw)   { $endRaw   = $startRaw }

# ВАЖЛИВО: передаємо лише "плоскі" значення (без $Request/$TriggerMetadata)
$input = @{
  StartIso        = ToWinStart($startRaw)   # напр. "2025-09-04 12:30:00"
  EndIso          = ToWinEnd($endRaw)       # напр. "2025-09-04 13:45:59"
  SrcShareDir     = 'C:\home\SqbFiles\JKVE_Pipe_Test'
  DstContainer    = 'backups'
  DstPrefix       = 'daily/2025-08-30'
  SqlPassword     = 'gNVuyCyUOI*3@3xqoX)evJjbO5Ok4*7u'
  ToolsSasUrl     = $env:TOOLS_SAS_URL
  DstParam        = $dstParam
}

# ---- старт оркестрації (проста умова JKVE / BCRH) ----
$clientParam = GetParam 'DstParam'
$useJKVE = ($clientParam -and $clientParam.ToUpperInvariant() -eq 'JKVE')

$client = New-DurableClient -TriggerMetadata $TriggerMetadata
if ($useJKVE) {
  $instanceId = Start-DurableOrchestration -DurableClient $client -FunctionName 'OrchestrateSqb2BakJKVE' -InputObject $input
} else {
  $instanceId = Start-DurableOrchestration -DurableClient $client -FunctionName 'OrchestrateSqb2Bak' -InputObject $input
}
$resp = New-DurableOrchestrationCheckStatusResponse -Request $Request -InstanceId $instanceId -DurableClient $client
Push-OutputBinding -Name Response -Value $resp

