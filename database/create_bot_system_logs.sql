-- DDL สำหรับตาราง bot_system_logs (Audit Trail & Event Logs)
CREATE TABLE IF NOT EXISTS public.bot_system_logs (
    log_id          BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(50) NOT NULL,
    level           VARCHAR(20) DEFAULT 'INFO',
    symbol          VARCHAR(20),
    message         TEXT NOT NULL,
    raw_payload     JSONB,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Bangkok', now())
);

CREATE INDEX IF NOT EXISTS idx_bot_system_logs_event_date ON public.bot_system_logs(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bot_system_logs_symbol ON public.bot_system_logs(symbol) WHERE symbol IS NOT NULL;

COMMENT ON TABLE public.bot_system_logs IS 'ตารางบันทึกประวัติการทำงาน (Audit Trail) คำสั่งซื้อขายและข้อผิดพลาดของระบบบอท';
