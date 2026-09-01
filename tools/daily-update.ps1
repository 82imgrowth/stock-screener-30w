# 매일 정해진 시각에 스크리닝을 돌린다. Windows 작업 스케줄러가 이 파일을 실행한다.
#
# GitHub의 예약(schedule)은 이 저장소에서 동작하지 않는다 — 플랫폼이 정상인 날에도
# 4영업일 연속 미발화였고, 새 워크플로에 30분 주기 크론을 걸어도 6슬롯 내내 0건이었다.
# 반면 API 호출(workflow_dispatch)은 그 기간 내내 한 번도 실패하지 않았다.
# 그래서 예약을 GitHub에 맡기지 않고 이 PC가 직접 건다.
#
# 토큰(PAT)은 필요 없다. 이미 로그인된 gh CLI의 인증을 그대로 쓴다.
# 다만 그 인증이 Windows 자격 증명 관리자에 사용자 단위로 저장돼 있어,
# 작업 스케줄러에서 '사용자가 로그온할 때만 실행'으로 등록해야 한다.
#
# 등록:   tools\register-task.ps1
# 해제:   schtasks /Delete /TN "30weeks-daily-update" /F
# 기록:   %LOCALAPPDATA%\30weeks-update.log

param(
    # 주말·중복 검사를 건너뛰고 무조건 실행한다. 손으로 다시 돌릴 때 쓴다.
    [switch]$Force
)

$repo = "82imgrowth/stock-screener-30w"
$site = "https://82imgrowth.github.io/stock-screener-30w/data.json"
$log  = Join-Path $env:LOCALAPPDATA "30weeks-update.log"

function Write-Log([string]$msg) {
    "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg" | Out-File -Append -Encoding utf8 $log
}

# 주말은 장이 안 열려 돌릴 이유가 없다.
$dow = (Get-Date).DayOfWeek
if (-not $Force -and ($dow -eq [DayOfWeek]::Saturday -or $dow -eq [DayOfWeek]::Sunday)) {
    Write-Log "주말이라 건너뜀 ($dow)"
    exit 0
}

# 이미 오늘 데이터가 올라가 있으면 건너뛴다. PC가 꺼져 있어 놓친 작업이 부팅 후
# 뒤늦게 실행될 때, 손으로 이미 돌린 뒤라면 9분짜리 스크리닝을 또 돌릴 이유가 없다.
if (-not $Force) {
    try {
        $stamp = [DateTimeOffset]::Now.ToUnixTimeSeconds()
        $data = Invoke-RestMethod -Uri "${site}?t=$stamp" -TimeoutSec 20
        if ($data.updated.Substring(0, 10) -eq (Get-Date -Format 'yyyy-MM-dd')) {
            Write-Log "오늘 데이터가 이미 있어 건너뜀 (updated=$($data.updated))"
            exit 0
        }
    } catch {
        # 사이트를 못 읽어도 스크리닝은 돌리는 편이 낫다. 사실만 남기고 계속한다.
        Write-Log "기준 시각 확인 실패, 그대로 진행: $($_.Exception.Message)"
    }
}

$gh = (Get-Command gh -ErrorAction SilentlyContinue).Source
if (-not $gh) {
    Write-Log "gh CLI를 찾지 못했습니다. https://cli.github.com 설치 후 gh auth login 하세요."
    exit 1
}

& $gh workflow run daily-screening.yml --ref main -R $repo
if ($LASTEXITCODE -ne 0) {
    Write-Log "실행 요청 실패 (exit $LASTEXITCODE)"
    exit 1
}

Write-Log "실행 요청 성공. 약 10분 뒤 사이트에 반영됩니다."
exit 0
