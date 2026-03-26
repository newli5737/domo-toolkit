"""Email Service — gửi email cảnh báo qua Gmail SMTP."""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

log = logging.getLogger(__name__)


def send_alert_email(
    subject: str,
    body: str,
    to_email: str,
    from_email: str,
    app_password: str,
) -> bool:
    """Gửi email cảnh báo qua Gmail SMTP.

    Args:
        subject: Tiêu đề email
        body: Nội dung email (plain text)
        to_email: Email nhận
        from_email: Gmail gửi
        app_password: App password của Gmail

    Returns:
        True nếu gửi thành công
    """
    try:
        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(from_email, app_password)
            server.sendmail(from_email, to_email, msg.as_string())

        log.info(f"[EMAIL] Đã gửi email tới {to_email}: {subject}")
        print(f"[EMAIL] Đã gửi email tới {to_email}: {subject}")
        return True
    except Exception as e:
        log.error(f"[EMAIL] Lỗi gửi email: {e}")
        print(f"[EMAIL] Lỗi gửi email: {e}")
        return False
