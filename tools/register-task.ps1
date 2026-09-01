# daily-update.ps1 을 매일 16:17(한국시간)에 실행하도록 작업 스케줄러에 등록한다.
# 한 번만 실행하면 된다. 관리자 권한은 필요 없다.
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File tools\register-task.ps1
#
# 확인:  schtasks /Query /TN "30weeks-daily-update" /V /FO LIST
# 해제:  schtasks /Delete /TN "30weeks-daily-update" /F

$taskName = "30weeks-daily-update"
$script   = Join-Path $PSScriptRoot "daily-update.ps1"
$runAt    = "16:17"

if (-not (Test-Path $script)) {
    Write-Error "daily-update.ps1 을 찾지 못했습니다: $script"
    exit 1
}

# PC가 꺼져 있어 시각을 놓쳤으면 켠 뒤에 이어서 실행한다(StartWhenAvailable).
# 배터리로 돌 때도 실행한다 — 노트북에서 안 도는 걸 막는다.
$action    = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$script`""
$trigger   = New-ScheduledTaskTrigger -Daily -At $runAt
$settings  = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

# 로그온한 사용자로 실행해야 gh CLI가 자격 증명 관리자에서 토큰을 읽을 수 있다.
# '사용자 로그온 여부와 관계없이 실행'으로 두면 토큰을 못 읽어 실패한다.
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal -Force `
    -Description "30주선 스크리너 매일 갱신 (GitHub 예약이 동작하지 않아 PC에서 건다)" | Out-Null

Write-Host "등록 완료: $taskName · 매일 $runAt"
Write-Host "확인: schtasks /Query /TN `"$taskName`" /V /FO LIST"
Write-Host "해제: schtasks /Delete /TN `"$taskName`" /F"
