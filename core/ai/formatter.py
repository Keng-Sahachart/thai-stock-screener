#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : formatter.py
LOCATION    : core/ai/
OCCASION    : ถูกเรียกโดย Telegram Bot หรือ Scanner เพื่อจัดรูปแบบผลลัพธ์ AI เป็น HTML
DESCRIPTION : Output Detail Formatter:
              - Level 1: MINIMAL  (สั้นมาก Flash Scorecard 1 บรรทัด/หุ้น)
              - Level 2: COMPACT  (กระชับ กำลังสวย สรุป 3 มิติ ชี้ขาดขึ้นจริง/หลอก)
              - Level 3: DETAILED (จัดเต็ม ทั้ง Pros, Risks, แผนเข้าซื้อและเทคนิคอล)
==============================================================================
"""

def parse_detail_level(param) -> int:
    """แปลง parameter 1, 2, 3 หรือชื่อ MINIMAL, COMPACT, DETAILED เป็น int (Default = 1)"""
    if not param:
        return 1
    p_str = str(param).strip().upper()
    if p_str in ("1", "MINIMAL", "MIN", "SHORT", "QUICK"):
        return 1
    elif p_str in ("2", "COMPACT", "MID", "NORMAL"):
        return 2
    elif p_str in ("3", "DETAILED", "FULL", "DETAIL"):
        return 3
    return 1

def format_ai_evaluation_html(ai_result: dict, detail_level: int = 1) -> str:
    """
    จัดรูปแบบข้อความ HTML สำหรับส่งเข้า Telegram ตามระดับความละเอียด 1, 2, 3
    """
    if not ai_result or ai_result.get("status") in ("DISABLED", "EMPTY"):
        msg = ai_result.get("message", "ไม่มีข้อมูลผลการวิเคราะห์ AI")
        return f"🤖 <b>AI Evaluation:</b> {msg}"

    if "error" in ai_result and not ai_result.get("evaluations"):
        return f"⚠️ <b>AI Evaluation Error:</b> <code>{ai_result['error']}</code>"

    level = parse_detail_level(detail_level)
    market_date = ai_result.get("market_date", "")
    top_pick = ai_result.get("top_pick", "NONE")
    evaluations = ai_result.get("evaluations", [])
    summary = ai_result.get("comparison_summary", "")

    # เรียงลำดับตาม confidence_score จากมากไปน้อย
    sorted_evals = sorted(evaluations, key=lambda x: x.get("confidence_score", 0), reverse=True)

    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

    # =========================================================================
    # LEVEL 1: MINIMAL (สั้นมาก 1 บรรทัดต่อหุ้น + ชี้ขาด ขึ้น/ลง)
    # =========================================================================
    if level == 1:
        lines = [
            f"⚡ <b>AI Flash Scorecard ({market_date})</b>",
            f"👑 <b>Top Pick:</b> <code>{top_pick}</code>\n"
        ]
        for idx, ev in enumerate(sorted_evals):
            sym = ev.get("symbol", "")
            score = ev.get("confidence_score", 0)
            dec = ev.get("decision", "PASS")
            dec_badge = "✅ <b>[BUY]</b>" if dec == "BUY" else "❌ <b>[PASS]</b>"
            
            # ดึงการตรวจจับขึ้นจริง/หลอก
            trap_data = ev.get("momentum_trap_check") or {}
            is_real = trap_data.get("verdict") == "GENUINE_MARKUP" or "ขึ้นจริง" in str(trap_data) or score >= 75
            trend_badge = "🟢 <b>ขึ้นจริง</b>" if is_real else "🔴 <b>ระวังดอย/หลอก</b>"

            # เกรด VI
            vi_data = ev.get("vi_quality_check") or {}
            vi_grade = vi_data.get("grade", "N/A")
            if vi_grade == "GRADE_A":
                vi_badge = "💎 VI-A"
            elif vi_grade == "GRADE_B":
                vi_badge = "🔹 VI-B"
            else:
                vi_badge = "⚠️ VI-อ่อน"

            m = medals[idx] if idx < len(medals) else f"#{idx+1}"
            lines.append(f"{m} <b>{sym}</b> ({score}คะแนน) | {trend_badge} | {vi_badge} | {dec_badge}")

        lines.append(f"\n💡 <i>(พิมพ์ /scan 2 เพื่อดูกระชับ หรือ /scan 3 เพื่อดูจัดเต็ม)</i>")
        return "\n".join(lines)

    # =========================================================================
    # LEVEL 2: COMPACT (กระชับ มีเหตุผลชี้ขาด 3 มิติ สบายตา)
    # =========================================================================
    elif level == 2:
        lines = [
            f"🏆 <b>สรุปวิเคราะห์ AI ประจำวัน ({market_date})</b>",
            f"👑 <b>Top Pick:</b> <code>{top_pick}</code>",
            f"💬 <i>\"{summary}\"</i>\n" if summary else ""
        ]

        for idx, ev in enumerate(sorted_evals):
            sym = ev.get("symbol", "")
            score = ev.get("confidence_score", 0)
            dec = ev.get("decision", "PASS")
            dec_badge = "✅ <b>แนะนำ: BUY</b>" if dec == "BUY" else "❌ <b>แนะนำ: PASS</b>"
            m = medals[idx] if idx < len(medals) else f"#{idx+1}"

            trap_data = ev.get("momentum_trap_check") or {}
            trap_verdict = "🟢 <b>ขึ้นจริง</b>" if trap_data.get("verdict") == "GENUINE_MARKUP" or score >= 75 else "🔴 <b>ระวังขึ้นหลอก/ดอย</b>"
            trap_desc = trap_data.get("summary", "")

            vi_data = ev.get("vi_quality_check") or {}
            vi_grade = vi_data.get("grade", "เกรดทั่วไป")
            vi_desc = vi_data.get("summary", "")

            card = [
                f"{m} <b>{sym}</b> | คะแนน: <b>{score}/100</b> | {dec_badge}",
                f"  • <b>พฤติกรรมราคา:</b> {trap_verdict} {trap_desc}".strip(),
                f"  • <b>ปัจจัยพื้นฐาน:</b> 💎 <b>{vi_grade}</b> {vi_desc}".strip(),
                f"  • <b>สรุป:</b> {ev.get('verdict', '')}".strip()
            ]
            lines.append("\n".join(card) + "\n")

        lines.append("💡 <i>(พิมพ์ /scan 1 ดูสั้น หรือ /scan 3 ดูละเอียดจัดเต็ม)</i>")
        return "\n".join(lines)

    # =========================================================================
    # LEVEL 3: DETAILED (ละเอียดจัดเต็ม ทั้ง Pros, Risks, ตัวเลขเทคนิค)
    # =========================================================================
    else:
        lines = [
            f"📊 <b>บทวิเคราะห์ AI เชิงลึก (Deep Dive) ประจำวัน {market_date}</b>",
            f"👑 <b>Top Pick:</b> <code>{top_pick}</code>",
            f"📝 <b>เปรียบเทียบภาพรวม:</b>\n{summary}\n" if summary else ""
        ]

        for idx, ev in enumerate(sorted_evals):
            sym = ev.get("symbol", "")
            score = ev.get("confidence_score", 0)
            dec = ev.get("decision", "PASS")
            dec_badge = "✅ <b>[BUY - ผ่านเกณฑ์]</b>" if dec == "BUY" else "❌ <b>[PASS - ไม่แนะนำ]</b>"
            m = medals[idx] if idx < len(medals) else f"#{idx+1}"

            trap_data = ev.get("momentum_trap_check") or {}
            vi_data = ev.get("vi_quality_check") or {}
            swing_data = ev.get("swing_risk_reward") or {}

            pros_str = "\n".join([f"    ➕ {p}" for p in ev.get("pros", [])])
            risks_str = "\n".join([f"    ➖ {r}" for r in ev.get("risks", [])])

            card = [
                f"━━━━━━━━━━━━━━━━━━━",
                f"{m} <b>{sym}</b> (ความมั่นใจ: <b>{score}/100</b>) | {dec_badge}",
                f"🔍 <b>มิติที่ 1 (ขึ้นจริง vs หลอก):</b> {trap_data.get('verdict', 'N/A')} - {trap_data.get('summary', '')}",
                f"💼 <b>มิติที่ 2 (งบ & VI):</b> {vi_data.get('grade', 'N/A')} - {vi_data.get('summary', '')}",
                f"🎯 <b>มิติที่ 3 (Risk/Reward):</b> {swing_data.get('verdict', 'N/A')} - {swing_data.get('summary', '')}",
                f"<b>จุดเด่น:</b>\n{pros_str}" if pros_str else "",
                f"<b>จุดเสี่ยง:</b>\n{risks_str}" if risks_str else "",
                f"📌 <b>บทสรุป:</b> <i>{ev.get('verdict', '')}</i>"
            ]
            lines.append("\n".join([c for c in card if c]) + "\n")

        lines.append("━━━━━━━━━━━━━━━━━━━")
        lines.append("💡 <i>(พิมพ์ /scan 1 เพื่อดูสรุปย่อแบบ 1 บรรทัด)</i>")
        return "\n".join(lines)
