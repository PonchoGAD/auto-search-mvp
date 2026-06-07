from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.clients.yookassa import YooKassaClient
from src.config import settings
from src.db.enums import PaymentProvider, PaymentStatus, SubscriptionPlan, SubscriptionStatus
from src.db.models import Payment, Subscription, User
from src.schemas.payments import PaymentCreateRequest, PaymentWebhookRequest


PAID_PLANS = {
    SubscriptionPlan.PREMIUM.value,
    SubscriptionPlan.PRO.value,
}

VALID_PROVIDERS = {item.value for item in PaymentProvider}
VALID_PAYMENT_STATUSES = {item.value for item in PaymentStatus}


class PaymentsRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_id(self, payment_id: int) -> Optional[Payment]:
        stmt = select(Payment).where(Payment.id == payment_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_external_payment_id(self, external_payment_id: str) -> Optional[Payment]:
        clean_external_id = str(external_payment_id or "").strip()
        if not clean_external_id:
            return None

        stmt = select(Payment).where(Payment.external_payment_id == clean_external_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_idempotency_key(
        self,
        user_id: int,
        provider: str,
        idempotency_key: str,
    ) -> Optional[Payment]:
        clean_key = str(idempotency_key or "").strip()
        if not clean_key:
            return None

        stmt = select(Payment).where(
            Payment.user_id == user_id,
            Payment.provider == provider,
            Payment.idempotency_key == clean_key,
        )

        return self.db.execute(stmt).scalar_one_or_none()

    def list_by_user(self, user_id: int) -> list[Payment]:
        stmt = (
            select(Payment)
            .where(Payment.user_id == user_id)
            .order_by(Payment.created_at.desc(), Payment.id.desc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def latest_payments(self, limit: int = 50) -> list[dict]:
        stmt = (
            select(Payment)
            .order_by(Payment.created_at.desc(), Payment.id.desc())
            .limit(limit)
        )
        rows = list(self.db.execute(stmt).scalars().all())
        return [self._to_dict(row) for row in rows]

    def total_payments(self) -> int:
        stmt = select(func.count(Payment.id))
        return int(self.db.execute(stmt).scalar() or 0)

    def successful_payments(self) -> int:
        stmt = select(func.count(Payment.id)).where(
            Payment.status == PaymentStatus.SUCCEEDED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def failed_payments(self) -> int:
        stmt = select(func.count(Payment.id)).where(
            Payment.status == PaymentStatus.FAILED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def pending_payments(self) -> int:
        stmt = select(func.count(Payment.id)).where(
            Payment.status == PaymentStatus.PENDING.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def canceled_payments(self) -> int:
        stmt = select(func.count(Payment.id)).where(
            Payment.status == PaymentStatus.CANCELED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def refunded_payments(self) -> int:
        stmt = select(func.count(Payment.id)).where(
            Payment.status == PaymentStatus.REFUNDED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def total_revenue(self) -> Decimal:
        stmt = select(func.coalesce(func.sum(Payment.amount), 0)).where(
            Payment.status == PaymentStatus.SUCCEEDED.value,
        )
        return Decimal(str(self.db.execute(stmt).scalar() or 0))

    def revenue_last_30d(self) -> Decimal:
        since = datetime.now(timezone.utc) - timedelta(days=30)

        stmt = select(func.coalesce(func.sum(Payment.amount), 0)).where(
            Payment.status == PaymentStatus.SUCCEEDED.value,
            Payment.paid_at >= since,
        )
        return Decimal(str(self.db.execute(stmt).scalar() or 0))

    def revenue_today(self) -> Decimal:
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        stmt = select(func.coalesce(func.sum(Payment.amount), 0)).where(
            Payment.status == PaymentStatus.SUCCEEDED.value,
            Payment.paid_at >= start,
        )
        return Decimal(str(self.db.execute(stmt).scalar() or 0))

    def payment_stats(self) -> dict:
        return {
            "total_revenue": str(self.total_revenue()),
            "revenue_today": str(self.revenue_today()),
            "revenue_30d": str(self.revenue_last_30d()),
            "payments_total": self.total_payments(),
            "payments_successful": self.successful_payments(),
            "payments_failed": self.failed_payments(),
            "payments_pending": self.pending_payments(),
            "payments_canceled": self.canceled_payments(),
            "payments_refunded": self.refunded_payments(),
        }

    def create_for_telegram_user(
        self,
        payload: PaymentCreateRequest,
    ) -> tuple[Payment, Optional[str], Optional[str]]:
        user = self.db.execute(
            select(User).where(User.telegram_user_id == payload.telegram_user_id)
        ).scalar_one_or_none()

        if not user:
            raise LookupError("User not found")

        provider = (payload.provider or settings.PAYMENT_PROVIDER or PaymentProvider.STUB.value).strip().lower()
        plan = (payload.plan or SubscriptionPlan.PREMIUM.value).strip().lower()

        if provider not in VALID_PROVIDERS:
            raise ValueError(f"invalid payment provider: {provider}")

        if plan not in PAID_PLANS:
            raise ValueError(f"invalid paid subscription plan: {plan}")

        # Stars: override amount/currency from config, bot just passes a placeholder
        if provider == PaymentProvider.STARS.value:
            stars_map = {
                SubscriptionPlan.PREMIUM.value: settings.PAYMENT_PLAN_PREMIUM_STARS,
                SubscriptionPlan.PRO.value: settings.PAYMENT_PLAN_PRO_STARS,
            }
            amount = Decimal(str(stars_map.get(plan, settings.PAYMENT_PLAN_PREMIUM_STARS)))
            currency = "XTR"
        else:
            amount = Decimal(str(payload.amount))
            if amount <= 0:
                raise ValueError("payment amount must be positive")
            currency = str(payload.currency or settings.DEFAULT_CURRENCY).upper().strip()
            if not currency:
                raise ValueError("currency is required")

        idempotency_key = str(payload.idempotency_key or "").strip() or None

        if idempotency_key:
            existing = self.get_by_idempotency_key(
                user_id=user.id,
                provider=provider,
                idempotency_key=idempotency_key,
            )
            if existing:
                return existing, existing.payment_url, existing.invoice_url

        external_payment_id = self._generate_external_payment_id(provider)

        success_url = payload.success_url or settings.payment_success_url
        fail_url = payload.fail_url or settings.payment_fail_url
        return_url = payload.return_url or settings.PAYMENT_RETURN_URL

        payment_url, invoice_url, yookassa_id = self._build_provider_invoice(
            provider=provider,
            external_payment_id=external_payment_id,
            amount=amount,
            currency=currency,
            description=payload.description,
            plan=plan,
            return_url=return_url,
            success_url=success_url,
            fail_url=fail_url,
        )

        # YooKassa returns its own UUID — use it as external_payment_id for webhook lookup
        if yookassa_id:
            external_payment_id = yookassa_id

        now_iso = datetime.now(timezone.utc).isoformat()

        meta_payload = {
            "plan": plan,
            "return_url": return_url,
            "success_url": success_url,
            "fail_url": fail_url,
            "idempotency_key": idempotency_key,
            "payment_url": payment_url,
            "invoice_url": invoice_url,
            "created_by": "telegram_bot",
            "provider": provider,
            "events": [
                {
                    "type": "payment_created",
                    "status": PaymentStatus.PENDING.value,
                    "at": now_iso,
                    "external_payment_id": external_payment_id,
                }
            ],
            "meta": payload.payload or {},
        }

        entity = Payment(
            user_id=user.id,
            provider=provider,
            status=PaymentStatus.PENDING.value,
            amount=amount,
            currency=currency,
            external_payment_id=external_payment_id,
            idempotency_key=idempotency_key,
            payment_url=payment_url,
            invoice_url=invoice_url,
            return_url=return_url,
            success_url=success_url,
            fail_url=fail_url,
            description=payload.description,
            payload=meta_payload,
        )

        try:
            self.db.add(entity)
            self.db.commit()
            self.db.refresh(entity)
        except IntegrityError:
            self.db.rollback()

            if idempotency_key:
                existing = self.get_by_idempotency_key(
                    user_id=user.id,
                    provider=provider,
                    idempotency_key=idempotency_key,
                )
                if existing:
                    return existing, existing.payment_url, existing.invoice_url

            existing = self.get_by_external_payment_id(external_payment_id)
            if existing:
                return existing, existing.payment_url, existing.invoice_url

            raise

        if provider == PaymentProvider.STUB.value and getattr(settings, "PAYMENT_STUB_SUCCESS_ENABLED", False):
            entity.status = PaymentStatus.SUCCEEDED.value
            entity.paid_at = datetime.now(timezone.utc)
            entity.payload = self._append_event(
                entity.payload,
                {
                    "type": "stub_auto_success",
                    "status": PaymentStatus.SUCCEEDED.value,
                    "at": datetime.now(timezone.utc).isoformat(),
                },
            )
            self.db.add(entity)
            self.db.commit()
            self.db.refresh(entity)
            self._activate_subscription_from_payment(entity)

        return entity, entity.payment_url, entity.invoice_url

    def apply_webhook(self, payload: PaymentWebhookRequest) -> Optional[Payment]:
        payment = self.get_by_external_payment_id(payload.external_payment_id)
        if not payment:
            return None

        self._validate_webhook_payload(payment, payload)

        current_payload = dict(payment.payload or {})

        event_id = str(payload.event_id or "").strip()
        if event_id:
            processed_events = list(current_payload.get("processed_webhook_events") or [])
            if event_id in processed_events:
                current_payload["last_duplicate_webhook_at"] = datetime.now(timezone.utc).isoformat()
                payment.payload = current_payload
                self.db.add(payment)
                self.db.commit()
                self.db.refresh(payment)
                return payment

            processed_events.append(event_id)
            current_payload["processed_webhook_events"] = processed_events[-50:]

        if payload.status not in VALID_PAYMENT_STATUSES:
            raise ValueError(f"invalid payment status: {payload.status}")

        if payment.status == PaymentStatus.SUCCEEDED.value and payload.status == PaymentStatus.SUCCEEDED.value:
            current_payload["last_duplicate_webhook_at"] = datetime.now(timezone.utc).isoformat()
            current_payload["last_duplicate_webhook"] = payload.payload or payload.raw_payload or {}
            payment.payload = current_payload

            self.db.add(payment)
            self.db.commit()
            self.db.refresh(payment)

            return payment

        old_status = payment.status
        payment.status = payload.status

        if payload.amount is not None:
            amount = Decimal(str(payload.amount))
            if amount <= 0:
                raise ValueError("webhook amount must be positive")
            payment.amount = amount

        if payload.currency is not None:
            payment.currency = payload.currency.upper().strip()

        webhook_received_at = datetime.now(timezone.utc)

        event_payload = {
            "type": payload.event_type or payload.webhook_type or "webhook",
            "status": payload.status,
            "at": webhook_received_at.isoformat(),
            "event_id": event_id or None,
            "status_from": old_status,
            "status_to": payload.status,
            "provider": payload.provider,
        }

        current_payload["webhook"] = payload.payload or {}
        current_payload["raw_payload"] = payload.raw_payload or payload.payload or {}
        current_payload["metadata"] = payload.metadata or {}
        current_payload["webhook_received_at"] = webhook_received_at.isoformat()
        current_payload["webhook_status_from"] = old_status
        current_payload["webhook_status_to"] = payload.status
        current_payload["webhook_event_id"] = event_id or None
        current_payload = self._append_event(current_payload, event_payload)
        payment.payload = current_payload

        if payload.status == PaymentStatus.SUCCEEDED.value:
            payment.paid_at = payload.paid_at or webhook_received_at

        self.db.add(payment)
        self.db.commit()
        self.db.refresh(payment)

        if payment.status == PaymentStatus.SUCCEEDED.value:
            self._activate_subscription_from_payment(payment)

        if payment.status in {
            PaymentStatus.FAILED.value,
            PaymentStatus.CANCELED.value,
            PaymentStatus.REFUNDED.value,
        }:
            self._rollback_payment_if_needed(payment)

        return payment

    def mark_failed(self, payment: Payment, reason: str | None = None) -> Payment:
        if payment.status == PaymentStatus.SUCCEEDED.value:
            return payment

        payment.status = PaymentStatus.FAILED.value

        current_payload = dict(payment.payload or {})
        if reason:
            current_payload["failure_reason"] = reason
        current_payload["failed_at"] = datetime.now(timezone.utc).isoformat()
        current_payload = self._append_event(
            current_payload,
            {
                "type": "payment_failed",
                "status": PaymentStatus.FAILED.value,
                "at": datetime.now(timezone.utc).isoformat(),
                "reason": reason,
            },
        )
        payment.payload = current_payload

        self.db.add(payment)
        self.db.commit()
        self.db.refresh(payment)

        return payment

    def mark_canceled(self, payment: Payment, reason: str | None = None) -> Payment:
        if payment.status == PaymentStatus.SUCCEEDED.value:
            return payment

        payment.status = PaymentStatus.CANCELED.value

        current_payload = dict(payment.payload or {})
        if reason:
            current_payload["cancel_reason"] = reason
        current_payload["canceled_at"] = datetime.now(timezone.utc).isoformat()
        current_payload = self._append_event(
            current_payload,
            {
                "type": "payment_canceled",
                "status": PaymentStatus.CANCELED.value,
                "at": datetime.now(timezone.utc).isoformat(),
                "reason": reason,
            },
        )
        payment.payload = current_payload

        self.db.add(payment)
        self.db.commit()
        self.db.refresh(payment)

        return payment

    def _activate_subscription_from_payment(self, payment: Payment) -> None:
        payload = dict(payment.payload or {})
        plan = payload.get("plan") or SubscriptionPlan.PREMIUM.value

        if plan not in PAID_PLANS:
            plan = SubscriptionPlan.PREMIUM.value

        now = datetime.now(timezone.utc).replace(microsecond=0)
        duration_days = _plan_duration_days(plan)
        expires_at = now + timedelta(days=duration_days)

        active_subscriptions = self.db.execute(
            select(Subscription).where(
                Subscription.user_id == payment.user_id,
                Subscription.status == SubscriptionStatus.ACTIVE.value,
            )
        ).scalars().all()

        for subscription in active_subscriptions:
            meta = subscription.meta or {}
            if meta.get("payment_id") == payment.id:
                return

        for item in active_subscriptions:
            item.status = SubscriptionStatus.CANCELED.value
            item.canceled_at = now
            self.db.add(item)

        new_subscription = Subscription(
            user_id=payment.user_id,
            plan=plan,
            status=SubscriptionStatus.ACTIVE.value,
            starts_at=now,
            expires_at=expires_at,
            meta={
                "payment_id": payment.id,
                "external_payment_id": payment.external_payment_id,
                "provider": payment.provider,
                "duration_days": duration_days,
                "activated_at": now.isoformat(),
            },
        )
        self.db.add(new_subscription)

        user = self.db.execute(select(User).where(User.id == payment.user_id)).scalar_one_or_none()
        if user:
            user.is_premium = True
            self.db.add(user)

        payment.payload = self._append_event(
            dict(payment.payload or {}),
            {
                "type": "subscription_activated",
                "status": payment.status,
                "at": now.isoformat(),
                "plan": plan,
                "duration_days": duration_days,
            },
        )
        self.db.add(payment)

        self.db.commit()

    def _rollback_payment_if_needed(self, payment: Payment) -> None:
        payload = dict(payment.payload or {})
        payload["rollback_checked_at"] = datetime.now(timezone.utc).isoformat()
        payload = self._append_event(
            payload,
            {
                "type": "payment_recovery_checked",
                "status": payment.status,
                "at": datetime.now(timezone.utc).isoformat(),
            },
        )
        payment.payload = payload

        self.db.add(payment)
        self.db.commit()

    def _validate_webhook_payload(
        self,
        payment: Payment,
        payload: PaymentWebhookRequest,
    ) -> None:
        if payload.provider != payment.provider:
            raise ValueError("webhook provider does not match payment provider")

        if payload.amount is not None:
            incoming_amount = Decimal(str(payload.amount))
            if incoming_amount != payment.amount:
                raise ValueError("webhook amount does not match payment amount")

        if payload.currency is not None:
            incoming_currency = payload.currency.upper().strip()
            if incoming_currency != payment.currency.upper().strip():
                raise ValueError("webhook currency does not match payment currency")

        self._verify_provider_signature(payment, payload)

    def _verify_provider_signature(
        self,
        payment: Payment,
        payload: PaymentWebhookRequest,
    ) -> None:
        # YooKassa webhook is authenticated via Basic Auth at the HTTP layer
        if payment.provider == PaymentProvider.YOOKASSA.value:
            return

        secret = self._provider_secret(payment.provider)

        if not secret:
            if payment.provider != PaymentProvider.STUB.value:
                raise PermissionError("payment provider secret is not configured")
            return

        if not payload.signature:
            raise PermissionError("missing provider webhook signature")

        message = self._signature_message(payload)
        expected = hmac.new(
            secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        provided = str(payload.signature)

        if hmac.compare_digest(expected, provided):
            return

        if hmac.compare_digest(provided, secret):
            return

        raise PermissionError("invalid provider webhook signature")

    def _signature_message(self, payload: PaymentWebhookRequest) -> str:
        return (
            f"{payload.provider}:"
            f"{payload.external_payment_id}:"
            f"{payload.status}:"
            f"{payload.amount or ''}:"
            f"{payload.currency or ''}:"
            f"{payload.event_id or ''}"
        )

    def _provider_secret(self, provider: str) -> Optional[str]:
        if provider == PaymentProvider.YOOKASSA.value:
            # YooKassa uses Basic Auth verified at HTTP layer — no HMAC secret needed here
            return None

        if provider == PaymentProvider.STARS.value:
            return settings.PAYMENT_WEBHOOK_SECRET

        if provider == PaymentProvider.STRIPE.value:
            return settings.PAYMENT_STRIPE_WEBHOOK_SECRET or settings.PAYMENT_WEBHOOK_SECRET

        if provider == PaymentProvider.TELEGRAM.value:
            return settings.PAYMENT_TELEGRAM_PROVIDER_TOKEN or settings.PAYMENT_WEBHOOK_SECRET

        return settings.PAYMENT_WEBHOOK_SECRET

    def _generate_external_payment_id(self, provider: str) -> str:
        return f"{provider}-{uuid4().hex}"

    def _build_provider_invoice(
        self,
        provider: str,
        external_payment_id: str,
        amount: Decimal,
        currency: str,
        description: str | None,
        plan: str,
        return_url: str | None,
        success_url: str | None,
        fail_url: str | None,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Returns (payment_url, invoice_url, yookassa_payment_id).
        yookassa_payment_id is set only for YooKassa — caller uses it to override external_payment_id.
        """
        if provider == PaymentProvider.STUB.value:
            return None, None, None

        if provider == PaymentProvider.STARS.value:
            # Stars have no external URL — payment happens inline in Telegram
            return None, None, None

        if provider == PaymentProvider.YOOKASSA.value:
            return self._build_yookassa_invoice(
                internal_idempotency_key=external_payment_id,
                amount=amount,
                currency=currency,
                description=description,
                return_url=return_url or success_url,
                plan=plan,
            )

        if provider == PaymentProvider.TELEGRAM.value:
            return None, None, None

        if provider == PaymentProvider.STRIPE.value:
            return None, None, None

        return None, None, None

    def _build_yookassa_invoice(
        self,
        internal_idempotency_key: str,
        amount: Decimal,
        currency: str,
        description: str | None,
        return_url: str | None,
        plan: str,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        shop_id = settings.PAYMENT_YOOKASSA_SHOP_ID
        secret_key = settings.PAYMENT_YOOKASSA_SECRET_KEY

        if not shop_id or not secret_key:
            return None, None, None

        redirect_url = return_url or settings.PAYMENT_RETURN_URL or "https://t.me/"

        client = YooKassaClient(shop_id=shop_id, secret_key=secret_key)

        try:
            result = client.create_payment(
                amount=amount,
                currency=currency,
                description=description or f"Auto-search {plan} subscription",
                idempotency_key=internal_idempotency_key,
                return_url=redirect_url,
                metadata={"plan": plan, "internal_id": internal_idempotency_key},
            )
        except Exception:
            return None, None, None

        yookassa_payment_id: Optional[str] = result.get("id")
        confirmation_url: Optional[str] = (result.get("confirmation") or {}).get("confirmation_url")

        return confirmation_url, confirmation_url, yookassa_payment_id

    def _append_event(self, payload: dict | None, event: dict) -> dict:
        current_payload = dict(payload or {})
        events = list(current_payload.get("events") or [])
        events.append(event)
        current_payload["events"] = events[-100:]
        return current_payload

    def _to_dict(self, row: Payment) -> dict:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "provider": row.provider,
            "status": row.status,
            "amount": str(row.amount),
            "currency": row.currency,
            "external_payment_id": row.external_payment_id,
            "idempotency_key": getattr(row, "idempotency_key", None),
            "payment_url": getattr(row, "payment_url", None),
            "invoice_url": getattr(row, "invoice_url", None),
            "return_url": getattr(row, "return_url", None),
            "success_url": getattr(row, "success_url", None),
            "fail_url": getattr(row, "fail_url", None),
            "description": row.description,
            "payload": row.payload,
            "paid_at": row.paid_at,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }


def _plan_duration_days(plan: str) -> int:
    if plan == SubscriptionPlan.PRO.value:
        return int(getattr(settings, "PAYMENT_PLAN_DURATION_DAYS", 30))

    if plan == SubscriptionPlan.PREMIUM.value:
        return int(getattr(settings, "PAYMENT_PLAN_DURATION_DAYS", 30))

    return int(getattr(settings, "PAYMENT_PLAN_DURATION_DAYS", 30))