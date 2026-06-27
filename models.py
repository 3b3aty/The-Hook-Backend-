from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Float, Text, JSON


# =========================
# CATEGORY
# =========================
class Category(Base):
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)

    emails = relationship("Email", back_populates="category")


# =========================
# USER
# =========================
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)
    google_user_id = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    name = Column(String, nullable=True)
    profile_picture = Column(String, nullable=True)

    provider = Column(String, default="google")

    access_token = Column(Text, nullable=True)
    refresh_token = Column(Text, nullable=True)
    token_expiry = Column(DateTime, nullable=True)

    refresh_token_jwt = Column(Text, nullable=True)
    refresh_token_jwt_expiry = Column(DateTime, nullable=True)

    is_active = Column(Boolean, default=True)

    last_login = Column(DateTime, nullable=True)
    last_email_sync = Column(DateTime, nullable=True)
    last_history_id = Column(String, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    interface_sender = relationship(
        "Interface", back_populates="sender", foreign_keys="Interface.sender_id")
    interface_receivers = relationship(
        "Interface", back_populates="receiver", foreign_keys="Interface.receiver_id")
    labels = relationship("Label", back_populates="user")
    label_rules = relationship(
        "LabelRule", back_populates="user", foreign_keys="LabelRule.user_id")
    sent_label_rules = relationship(
        "LabelRule", back_populates="from_user", foreign_keys="LabelRule.from_user_id")

# =========================
# EMAIL
# =========================


class Email(Base):
    __tablename__ = 'emails'

    id = Column(Integer, primary_key=True, index=True)
    gmail_message_id = Column(String, unique=True, index=True)
    thread_id = Column(String, index=True)

    subject = Column(String)
    body_full = Column(String)
    body_snippet = Column(String, nullable=True)
    labels = Column(Text, nullable=True)
    date = Column(DateTime)

    delivery_status = Column(String, default="draft")

    category_id = Column(Integer, ForeignKey('categories.id'), nullable=True)

    # GLOBAL STATUS
    # PENDING, PROCESSING, ANALYZED, FAILED
    status = Column(String, default="PENDING")
    is_read = Column(Boolean, default=False)
    is_hooked = Column(Boolean, default=False)
    is_trash = Column(Boolean, default=False)
    is_starred = Column(Boolean, default=False)

    # PARTIAL STATUS
    urls_status = Column(String, default="PENDING")
    attachments_status = Column(String, default="PENDING")
    body_status = Column(String, default="PENDING")
    headers_status = Column(String, default="PENDING")

    # QUEUE FLAGS (avoid duplicate enqueue)
    is_urls_queued = Column(Boolean, default=False)
    is_attachments_queued = Column(Boolean, default=False)
    is_body_queued = Column(Boolean, default=False)
    is_headers_queued = Column(Boolean, default=False)

    # FINAL RESULT
    risk_score = Column(Float, default=0.0)
    final_verdict = Column(String, nullable=True)

    # TIMESTAMPS
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    analyzed_at = Column(DateTime, nullable=True)

    # RELATIONSHIPS
    category = relationship("Category", back_populates="emails")
    interfaces = relationship("Interface", back_populates="email")
    deadlines = relationship("EmailDeadline", back_populates="email")

    headers = relationship(
        "EmailHeaders", back_populates="email", uselist=False)
    urls = relationship("UrlsExtracted", back_populates="email")
    attachments = relationship("Attachments", back_populates="email")
    email_labels = relationship("EmailLabel", back_populates="email")

    user_actions = relationship("UserAction", back_populates="email")
    body_classification = relationship(
        "BodyClassification", back_populates="email", uselist=False)


# =========================
# LABELS
# =========================
class Label(Base):
    __tablename__ = 'labels'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'),
                     nullable=False, index=True)
    name = Column(String, nullable=False)
    color = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="labels")
    rules = relationship("LabelRule", back_populates="label")
    emails = relationship("EmailLabel", back_populates="label")


# =========================
# LABEL RULES
# =========================
class LabelRule(Base):
    __tablename__ = 'label_rules'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'),
                     nullable=False, index=True)
    label_id = Column(Integer, ForeignKey('labels.id'),
                      nullable=False, index=True)
    from_user_id = Column(Integer, ForeignKey(
        'users.id'), nullable=False, index=True)

    user = relationship("User", back_populates="label_rules",
                        foreign_keys=[user_id])
    label = relationship("Label", back_populates="rules")
    from_user = relationship(
        "User", back_populates="sent_label_rules", foreign_keys=[from_user_id])


# =========================
# EMAIL LABELS
# =========================
class EmailLabel(Base):
    __tablename__ = 'email_labels'

    email_id = Column(Integer, ForeignKey('emails.id'),
                      primary_key=True, nullable=False)
    label_id = Column(Integer, ForeignKey('labels.id'),
                      primary_key=True, nullable=False)

    email = relationship("Email", back_populates="email_labels")
    label = relationship("Label", back_populates="emails")


# =========================
# INTERFACE (Sender/Receiver Mapping)
# =========================
class Interface(Base):
    __tablename__ = 'interfaces'

    id = Column(Integer, primary_key=True, index=True)

    sender_id = Column(Integer, ForeignKey('users.id'))
    receiver_id = Column(Integer, ForeignKey('users.id'))
    email_id = Column(Integer, ForeignKey('emails.id'))

    email = relationship("Email", back_populates="interfaces")
    sender = relationship(
        "User", back_populates="interface_sender", foreign_keys=[sender_id])
    receiver = relationship(
        "User", back_populates="interface_receivers", foreign_keys=[receiver_id])


# =========================
# EMAIL HEADERS
# =========================
class EmailHeaders(Base):
    __tablename__ = 'email_headers'

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey('emails.id'), index=True)

    return_path = Column(String, nullable=True)
    reasons = Column(JSON, nullable=True)
    verdict = Column(String, nullable=True)
    score = Column(Float, nullable=True)

    raw_headers = Column(Text, nullable=True)

    received_chain = Column(String, nullable=True)
    spf_result = Column(String, nullable=True)
    dkim_result = Column(String, nullable=True)
    dmarc_result = Column(String, nullable=True)

    status = Column(String, default="PENDING")
    analyzed_at = Column(DateTime, nullable=True)

    email = relationship("Email", back_populates="headers")


# =========================
# URLS
# =========================
class UrlsExtracted(Base):
    __tablename__ = 'urls_extracted'

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey('emails.id'),
                      nullable=False, index=True)

    url = Column(String, nullable=False)

    reasons = Column(JSON, nullable=True)
    verdict = Column(String, nullable=True)

    status = Column(String, default="PENDING")
    analyzed_at = Column(DateTime, nullable=True)

    email = relationship("Email", back_populates="urls")


# =========================
# ATTACHMENTS
# =========================
class Attachments(Base):
    __tablename__ = 'attachments'

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey('emails.id'),
                      nullable=False, index=True)

    file_name = Column(String, nullable=False)
    file_url = Column(String, nullable=True)
    file_size = Column(Integer, nullable=True)
    file_type = Column(String, nullable=True)
    hash_sha256 = Column(String, nullable=True)

    status = Column(String, default="PENDING")
    analyzed_at = Column(DateTime, nullable=True)

    email = relationship("Email", back_populates="attachments")

    static_analysis = relationship(
        "StaticAnalysis", back_populates="attachment", uselist=False)
    dynamic_analysis = relationship(
        "DynamicAnalysis", back_populates="attachment", uselist=False)


# =========================
# STATIC ANALYSIS
# =========================
class StaticAnalysis(Base):
    __tablename__ = 'static_analysis'

    id = Column(Integer, primary_key=True, index=True)
    attach_id = Column(Integer, ForeignKey(
        'attachments.id'), unique=True, nullable=False)

    score = Column(Float, nullable=True)
    reasons = Column(JSON, nullable=True)
    verdict = Column(String, nullable=True)

    attachment = relationship("Attachments", back_populates="static_analysis")


# =========================
# DYNAMIC ANALYSIS
# =========================
class DynamicAnalysis(Base):
    __tablename__ = 'dynamic_analysis'

    id = Column(Integer, primary_key=True, index=True)
    attach_id = Column(Integer, ForeignKey(
        'attachments.id'), unique=True, nullable=False)

    score = Column(Float, nullable=True)
    reasons = Column(JSON, nullable=True)
    verdict = Column(String, nullable=True)

    attachment = relationship("Attachments", back_populates="dynamic_analysis")


# =========================
# BODY CLASSIFICATION
# =========================
class BodyClassification(Base):
    __tablename__ = 'body_classification'

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey('emails.id'),
                      unique=True, nullable=False, index=True)

    confidence = Column(Float, nullable=True)
    verdict = Column(String, nullable=True)

    status = Column(String, default="PENDING")
    analyzed_at = Column(DateTime(timezone=True), server_default=func.now())

    email = relationship("Email", back_populates="body_classification")


# =========================
# EMAIL DEADLINES
# =========================
class EmailDeadline(Base):
    __tablename__ = 'email_deadlines'

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey('emails.id'))

    deadline = Column(DateTime, nullable=True)
    alert_sent = Column(Boolean, default=False)

    email = relationship("Email", back_populates="deadlines")


# =========================
# USER ACTIONS
# =========================
class UserAction(Base):
    __tablename__ = 'user_actions'

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey('emails.id'), nullable=False)

    action = Column(String, nullable=False)
    time_stamp = Column(DateTime(timezone=True), server_default=func.now())

    email = relationship("Email", back_populates="user_actions")


# =========================
# TASK TRACKING (FOR CELERY)
# =========================
class AnalysisTask(Base):
    __tablename__ = "analysis_tasks"

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey("emails.id"), index=True)

    task_type = Column(String)  # url, attachment, body, headers

    status = Column(String, default="PENDING")
    # PENDING, STARTED, SUCCESS, FAILED, RETRY

    retries = Column(Integer, default=0)
    error_message = Column(String, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
