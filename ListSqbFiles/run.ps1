param($Input)  # StartIso, EndIso, SrcShareDir, ...

function ParseNameTs($n){
  if ($n -match '_([0-9]{12})\.SQB$') { return [datetime]::ParseExact($Matches[1],'yyyyMMddHHmm',$null) }
  elseif ($n -match '_([0-9]{10})\.SQB$') { return [datetime]::ParseExact($Matches[1]+'00','yyyyMMddHHmm',$null) }
  else { return $null }
}

$start = [datetime]$Input.StartIso
$end   = [datetime]$Input.EndIso

$all = Get-ChildItem -Path $Input.SrcShareDir -File -Filter *.SQB -Recurse
$entries = $all | ForEach-Object {
  $ts = ParseNameTs $_.Name; if (-not $ts) { $ts = $_.CreationTimeUtc }
  [pscustomobject]@{ FullName = $_.FullName; Name = $_.Name; TS = $ts }
}
$filtered = $entries | Where-Object { $_.TS -ge $start -and $_.TS -le $end } |
            Sort-Object TS | Select-Object -ExpandProperty FullName
return ,$filtered
