# ==============================================================================
# Thai Stock Screener - Windows Task Scheduler Auto Setup Script
# Description: สคริปต์ PowerShell สำหรับสร้าง Scheduled Tasks บน Windows อัตโนมัติ
# Usage:
#   ติดตั้งงานทั้งหมด:
#     powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows_tasks.ps1
#   ถอนการติดตั้งงานทั้งหมด:
#     powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows_tasks.ps1 -Uninstall
# ==============================================================================

param (
    [switch]$Uninstall
)

$TaskFolder = "\ThaiStockScreener\"
$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path

# ค้นหา Python Executable ในโปรเจกต์
$PythonVenv = Join-Path $ProjectRoot "env\Scripts\python.exe"
if (-not (Test-Path $PythonVenv)) {
    $PythonVenv = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
}
if (-not (Test-Path $PythonVenv)) {
    $PythonVenv = (Get-Command python.exe -ErrorAction SilentlyContinue).Source
}

if (-not $PythonVenv) {
    Write-Error "❌ ไม่พบ Python interpreter กรุณาตรวจสอบว่ามี env\ หรือติดตั้ง Python แล้ว"
    exit 1
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Thai Stock Screener - Windows Task Scheduler Setup" -ForegroundColor Cyan
Write-Host " Project Directory : $ProjectRoot" -ForegroundColor Yellow
Write-Host " Python Path       : $PythonVenv" -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Cyan

# รายการงานทั้งหมด
$Tasks = @(
    @{
        Name = "01_PortScanner_Morning"
        Script = "jobs\run_port_scanner.py"
        Desc = "สแกนกราฟหุ้นในพอร์ตตอนเช้า"
        TriggerType = "DailyTime"
        Time = "09:00"
    },
    @{
        Name = "02_BuyScanner_PreMarket"
        Script = "jobs\run_buy_scanner.py"
        Desc = "สแกนหาจังหวะซื้อและส่งการ์ดขอ Approve เข้า Telegram"
        TriggerType = "DailyTime"
        Time = "09:15"
    },
    @{
        Name = "03_SellMonitor_Morning"
        Script = "jobs\run_sell_monitor.py"
        Desc = "เฝ้าระวัง Stop Loss และ Cut Loss ตลาดเช้า (ทุก 10 นาที)"
        TriggerType = "Repetition"
        StartTime = "10:00"
        Duration = "PT2H30M"
        Interval = "PT10M"
    },
    @{
        Name = "04_SyncOrders_Morning"
        Script = "jobs\sync_order_status.py"
        Desc = "ซิงค์สถานะ Order ตลาดเช้า (ทุก 5 นาที)"
        TriggerType = "Repetition"
        StartTime = "10:00"
        Duration = "PT2H30M"
        Interval = "PT5M"
    },
    @{
        Name = "05_SellMonitor_Afternoon"
        Script = "jobs\run_sell_monitor.py"
        Desc = "เฝ้าระวัง Stop Loss และ Cut Loss ตลาดบ่าย (ทุก 10 นาที)"
        TriggerType = "Repetition"
        StartTime = "14:30"
        Duration = "PT2H"
        Interval = "PT10M"
    },
    @{
        Name = "06_SyncOrders_Afternoon"
        Script = "jobs\sync_order_status.py"
        Desc = "ซิงค์สถานะ Order ตลาดบ่าย (ทุก 5 นาที)"
        TriggerType = "Repetition"
        StartTime = "14:30"
        Duration = "PT2H"
        Interval = "PT5M"
    },
    @{
        Name = "07_SyncOrders_MarketClose"
        Script = "jobs\sync_order_status.py"
        Desc = "ซิงค์สถานะ Order รอบปิดตลาด"
        TriggerType = "DailyTime"
        Time = "16:40"
    },
    @{
        Name = "08_TaskUpdate_EOD"
        Script = "taskUpdate.py"
        Desc = "อัปเดตราคา EOD, Indicators, สัญญาณ และซิงค์ Trailing Stop สิ้นวัน"
        TriggerType = "DailyTime"
        Time = "19:15"
    }
)

if ($Uninstall) {
    Write-Host "🗑️ กำลังถอนการติดตั้งงานทั้งหมดใน $TaskFolder ..." -ForegroundColor Yellow
    foreach ($t in $Tasks) {
        $taskPath = "$TaskFolder$($t.Name)"
        Unregister-ScheduledTask -TaskName $t.Name -TaskPath $TaskFolder -Confirm:$false -ErrorAction SilentlyContinue
        Write-Host "  - ลบงาน: $($t.Name)" -ForegroundColor Gray
    }
    Write-Host "✅ ถอนการติดตั้งงานบน Windows Task Scheduler เรียบร้อยแล้ว!" -ForegroundColor Green
    exit 0
}

# สร้าง Tasks บน Windows
Write-Host "⚙️ กำลังลงทะเบียน Scheduled Tasks สำหรับวันจันทร์ - ศุกร์..." -ForegroundColor Green

# ตรวจสอบ DaysOfWeek (Monday to Friday)
$daysOfWeek = @("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")

foreach ($t in $Tasks) {
    $taskName = $t.Name
    $scriptRelative = $t.Script
    $arguments = "$scriptRelative"

    # กำหนด Action
    $action = New-ScheduledTaskAction -Execute $PythonVenv -Argument $arguments -WorkingDirectory $ProjectRoot

    # กำหนด Trigger (รันเฉพาะวันจันทร์ - ศุกร์)
    if ($t.TriggerType -eq "DailyTime") {
        $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $daysOfWeek -At $t.Time
    } elseif ($t.TriggerType -eq "Repetition") {
        $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $daysOfWeek -At $t.StartTime
        $trigger.RepetitionInterval = (New-TimeSpan -Minutes ([int]($t.Interval -replace 'PT|M','')))
        $trigger.RepetitionDuration = (New-TimeSpan -Minutes ([int]($t.Duration -replace 'PT|H|M','' -replace '(\d+)H(\d+)M', '$1*60+$2' | Invoke-Expression 2>$null)))
        if (-not $trigger.RepetitionDuration.TotalMinutes) {
            # แปลง duration สำรอง
            if ($t.Duration -eq "PT2H30M") { $trigger.RepetitionDuration = [TimeSpan]::FromMinutes(150) }
            elseif ($t.Duration -eq "PT2H") { $trigger.RepetitionDuration = [TimeSpan]::FromMinutes(120) }
        }
    }

    $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 2)

    # ลงทะเบียน Task
    Register-ScheduledTask -TaskName $taskName -TaskPath $TaskFolder -Action $action -Trigger $trigger -Settings $settings -Description $t.Desc -Force | Out-Null
    Write-Host "  ✅ ลงทะเบียน: $taskName ($($t.Desc))" -ForegroundColor Green
}

Write-Host "`n🎉 ลงทะเบียนงานทั้งหมดใน Task Scheduler สำเร็จแล้ว!" -ForegroundColor Cyan
Write-Host "คุณสามารถเปิดดูหรือจัดการงานได้ที่ Start Menu -> Task Scheduler -> โฟลเดอร์ ThaiStockScreener" -ForegroundColor Gray
